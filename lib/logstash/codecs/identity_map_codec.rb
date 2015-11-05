# encoding: utf-8
require "logstash/namespace"
require "thread_safe"

# This class is a Codec duck type
# Using Composition, it maps from a stream identity to
# a cloned codec instance via the same API as a Codec
# it implements the codec public API

module LogStash module Codecs class IdentityMapCodec
  # subclass of Exception, LS has more than limit (20000) active streams
  class IdentityMapUpperLimitException < Exception; end

  module EightyPercentWarning
    extend self
    def visit(imc)
      current_size, limit = imc.current_size_and_limit
      return if current_size < (limit * 0.8)
      imc.logger.warn("IdentityMapCodec has reached 80% capacity",
        :current_size => current_size, :upper_limit => limit)
    end
  end

  module UpperLimitReached
    extend self
    def visit(imc)
      current_size, limit = imc.current_size_and_limit
      return if current_size < limit
      # we hit the limit
      # try to clean out stale streams
      current_size, limit = imc.map_cleanup
      return if current_size < limit
      # we are still at the limit and all streams are in use
      imc.logger.error("IdentityMapCodec has reached 100% capacity",
          :current_size => current_size, :upper_limit => limit)
      raise IdentityMapUpperLimitException.new
    end
  end

  class MapCleaner
    def initialize(imc, interval)
      @imc, @interval = imc, interval
      @running = false
    end

    def start
      return self if running?
      @running = true
      @thread = Thread.new(@imc) do |imc|
        loop do
          sleep @interval
          break if !@running
          imc.map_cleanup
        end
      end
      self
    end

    def running?
      @running
    end

    def stop
      return if !running?
      @running = false
      @thread.wakeup
    end
  end

  # A composite class to hold both the codec and the eviction_timeout
  # instances of this Value Object are stored in the mapping hash
  class CodecValue
    attr_reader :codec
    attr_accessor :timeout

    def initialize(codec)
      @codec = codec
    end
  end

  #maximum size of the mapping hash
  MAX_IDENTITIES = 20_000

  # time after which a stream is
  # considered stale
  # each time a stream is accessed
  # it is given a new timeout
  EVICT_TIMEOUT = 60 * 60 * 1 # 1 hour

  # time that the cleaner thread sleeps for
  # before it tries to clean out stale mappings
  CLEANER_INTERVAL = 60 * 5 # 5 minutes

  attr_reader :identity_map
  attr_accessor :base_codec, :cleaner

  def initialize(codec)
    @base_codec = codec
    @base_codecs = [codec]
    @identity_map = ThreadSafe::Hash.new &method(:codec_builder)
    @max_identities = MAX_IDENTITIES
    @evict_timeout = EVICT_TIMEOUT
    @cleaner = MapCleaner.new(self, CLEANER_INTERVAL)
    @decode_block = lambda {|*| }
  end

  # ==============================================
  # Constructional/builder methods
  # chain this method off of new
  #
  # used to add a non-default maximum identities
  def max_identities(max)
    @max_identities = max.to_i
    self
  end

  # used to add a non-default evict timeout
  def evict_timeout(timeout)
    @evict_timeout = timeout.to_i
    self
  end

  # used to add  a non-default cleaner interval
  def cleaner_interval(interval)
    @cleaner.stop
    @cleaner = MapCleaner.new(self, interval.to_i)
    self
  end
  # end Constructional/builder methods
  # ==============================================

  # ==============================================
  # Codec API
  def decode(data, identity = nil, &block)
    @decode_block = block if @decode_block != block
    stream_codec(identity).decode(data, &block)
  end

  alias_method :<<, :decode

  def encode(event, identity = nil)
    stream_codec(identity).encode(event)
  end

  # this method will not be called from
  # the input or the pipeline unless
  # we implement codec flush on shutdown
  # problematic, because we may not have
  # received all the multiline parts yet.
  # but if we don't flush we will lose data
  def flush(&block)
    all_codecs.each do |codec|
      #let ruby do its default args thing
      block.nil? ? codec.flush : codec.flush(&block)
    end
  end

  def close()
    cleaner.stop
    all_codecs.each(&:close)
  end
  # end Codec API
  # ==============================================

  def all_codecs
    no_streams? ? @base_codecs : identity_map.values.map(&:codec)
  end

  def max_limit
    @max_identities
  end

  def identity_count
    identity_map.size
  end

  # support cleaning of stale stream/codecs
  # a stream is considered stale if it has not
  # been accessed in the last @evict_timeout
  # period (default 1 hour)
  def map_cleanup
    cut_off = Time.now.to_i
    # delete_if is atomic
    # contents should not mutate during this call
    identity_map.delete_if do |identity, compo|
      if (flag = compo.timeout <= cut_off)
        compo.codec.flush(&@decode_block)
      end
      flag
    end
    current_size_and_limit
  end

  def current_size_and_limit
    [identity_count, max_limit]
  end

  def logger
    # we 'borrow' the codec's logger as we don't have our own
    @base_codec.logger
  end

  def codec_without_usage_update(identity)
    find_codec_value(identity).codec
  end

  def eviction_timestamp_for(identity)
    find_codec_value(identity).timeout
  end

  private

  def stream_codec(identity)
    return base_codec if identity.nil?
    record_codec_usage(identity) # returns codec
  end

  def find_codec_value(identity)
    identity_map[identity]
  end

  # for nil stream this method is not called
  def record_codec_usage(identity)
    check_map_limits
    # only start the cleaner if streams are in use
    # continuous calls to start are OK
    cleaner.start
    compo = find_codec_value(identity)
    compo.timeout = eviction_timestamp
    compo.codec
  end

  def eviction_timestamp
    Time.now.to_i + @evict_timeout
  end

  def check_map_limits
    UpperLimitReached.visit(self)
    EightyPercentWarning.visit(self)
  end

  def codec_builder(hash, k)
    codec = hash.empty? ? @base_codec : @base_codec.clone
    compo = CodecValue.new(codec)
    hash.store(k, compo)
  end

  def no_streams?
    identity_map.empty?
  end
end end end
