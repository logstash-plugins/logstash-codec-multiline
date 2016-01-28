# encoding: utf-8
require "logstash/codecs/base"
require "logstash/util/charset"
require "logstash/timestamp"
require "logstash/codecs/auto_flush"

require "grok-pure"
require 'logstash/patterns/core'
require "logstash/util/buftok"

# The multiline codec will collapse multiline messages and merge them into a
# single event.
#
# The original goal of this codec was to allow joining of multiline messages
# from files into a single event. For example, joining Java exception and
# stacktrace messages into a single event.
#
# The config looks like this:
# [source,ruby]
#     input {
#       stdin {
#         codec => multiline {
#           pattern => "pattern, a regexp"
#           negate => "true" or "false"
#           what => "previous" or "next"
#         }
#       }
#     }
#
# The `pattern` should match what you believe to be an indicator that the field
# is part of a multi-line event.
#
# The `what` must be `previous` or `next` and indicates the relation
# to the multi-line event.
#
# The `negate` can be `true` or `false` (defaults to `false`). If `true`, a
# message not matching the pattern will constitute a match of the multiline
# filter and the `what` will be applied. (vice-versa is also true)
#
# For example, Java stack traces are multiline and usually have the message
# starting at the far-left, with each subsequent line indented. Do this:
# [source,ruby]
#     input {
#       stdin {
#         codec => multiline {
#           pattern => "^\s"
#           what => "previous"
#         }
#       }
#     }
#
# This says that any line starting with whitespace belongs to the previous line.
#
# Another example is to merge lines not starting with a date up to the previous
# line..
# [source,ruby]
#     input {
#       file {
#         path => "/var/log/someapp.log"
#         codec => multiline {
#           # Grok pattern names are valid! :)
#           pattern => "^%{TIMESTAMP_ISO8601} "
#           negate => true
#           what => previous
#         }
#       }
#     }
#
# This says that any line not starting with a timestamp should be merged with the previous line.
#
# One more common example is C line continuations (backslash). Here's how to do that:
# [source,ruby]
#     filter {
#       multiline {
#         type => "somefiletype"
#         pattern => "\\$"
#         what => "next"
#       }
#     }
#
# This says that any line ending with a backslash should be combined with the
# following line.
#
module LogStash module Codecs class Multiline < LogStash::Codecs::Base
  config_name "multiline"

  # The regular expression to match.
  config :pattern, :validate => :string, :required => true

  # If the pattern matched, does event belong to the next or previous event?
  config :what, :validate => ["previous", "next"], :required => true

  # Negate the regexp pattern ('if not matched').
  config :negate, :validate => :boolean, :default => false

  # Logstash ships by default with a bunch of patterns, so you don't
  # necessarily need to define this yourself unless you are adding additional
  # patterns.
  #
  # Pattern files are plain text with format:
  # [source,ruby]
  #     NAME PATTERN
  #
  # For example:
  # [source,ruby]
  #     NUMBER \d+
  config :patterns_dir, :validate => :array, :default => []

  # The character encoding used in this input. Examples include `UTF-8`
  # and `cp1252`
  #
  # This setting is useful if your log files are in `Latin-1` (aka `cp1252`)
  # or in another character set other than `UTF-8`.
  #
  # This only affects "plain" format logs since JSON is `UTF-8` already.
  config :charset, :validate => ::Encoding.name_list, :default => "UTF-8"

  # Change the delimiter that separates lines
  config :delimiter, :validate => :string, :default => "\n"

  # Tag multiline events with a given tag. This tag will only be added
  # to events that actually have multiple lines in them.
  config :multiline_tag, :validate => :string, :default => "multiline"

  # The accumulation of events can make logstash exit with an out of memory error
  # if event boundaries are not correctly defined. This settings make sure to flush
  # multiline events after reaching a number of lines, it is used in combination
  # max_bytes.
  config :max_lines, :validate => :number, :default => 500

  # The accumulation of events can make logstash exit with an out of memory error
  # if event boundaries are not correctly defined. This settings make sure to flush
  # multiline events after reaching a number of bytes, it is used in combination
  # max_lines.
  config :max_bytes, :validate => :bytes, :default => "10 MiB"

  # The accumulation of multiple lines will be converted to an event when either a
  # matching new line is seen or there has been no new data appended for this time
  # auto_flush_interval. No default.  If unset, no auto_flush. Units: seconds
  config :auto_flush_interval, :validate => :number

  def register
    @tokenizer = FileWatch::BufferedTokenizer.new(@delimiter)
    @grok = Grok.new
    @patterns_dir = [LogStash::Patterns::Core.path] + @patterns_dir
    @patterns_dir.each do |path|
      if ::File.directory?(path)
        path = ::File.join(path, "*")
      end

      Dir.glob(path).each do |file|
        @logger.info("Grok loading patterns from file", :path => file)
        @grok.add_patterns_from_file(file)
      end
    end

    @grok.compile(@pattern)
    @logger.debug("Registered multiline plugin", :type => @type, :config => @config)

    reset_buffer

    @handler = method("do_#{@what}".to_sym)

    @converter = LogStash::Util::Charset.new(@charset)
    @converter.logger = @logger

    # TODO: (colin) I don't really understand this @last_seen_listener poutine. I needed to create
    # this lamda to DRY across the close & auto_flush methods to pass the closure to the new
    # @tokenizer which I figured needs to be flushed upon close. there does not seem to be
    # explicit tests for this closing logic.
    # what is not clear here is the initialization of @last_seen_listener which gets initialized
    # in the accept method but the close method systematically call auto_flush which assumes the
    # existence of @last_seen_listener. this whole logic is confusing and should be either made
    # more explicit and self documenting OR documentation should be prodided.
    @auto_flush_block = lambda do |event|
      @last_seen_listener.process_event(event)
    end

    if @auto_flush_interval
      # will start on first decode
      @auto_flush_runner = AutoFlush.new(self, @auto_flush_interval)
    end
  end

  def decode(data, &block)
    @tokenizer.extract(data.force_encoding("ASCII-8BIT")).each do |line|
      match_line(@converter.convert(line), &block)
    end
  end

  def encode(event)
    # Nothing to do.
    @on_event.call(event, event)
  end

  # this is the termination or final flush API
  #
  # @param block [Proc] the closure that will be called for all events that need to be flushed.
  def flush(&block)
    remainder = @tokenizer.flush
    match_line(@converter.convert(remainder), &block) unless remainder.empty?
    flush_buffered_events(&block)
  end

  # TODO: (colin) I believe there is a problem here in calling auto_flush. auto_flush depends on
  # @auto_flush_block which references @last_seen_listener which is only initialized in the context of
  # IdentityMapCodec which in turn I believe cannot by assumed. the multiline codec could run without
  # IdentityMapCodec.
  def close
    if auto_flush_runner.pending?
      #will cancel task if necessary
      auto_flush_runner.stop
    end

    remainder = @tokenizer.flush
    match_line(@converter.convert(remainder), &@auto_flush_block) unless remainder.empty?

    auto_flush
  end

  # TODO: (colin) what is the pupose of this accept method? AFAICT it is only used if this codec is used
  # within the IdentityMapCodec. it is not clear when & in which context this method is used.
  # I believe the codec should still be able to live outside the context of an IdentityMapCodec but there
  # are usage of ivars like @last_seen_listener only inititalized int the context of IdentityMapCodec.
  def accept(listener)
    # memoize references to listener that holds upstream state
    @previous_listener = @last_seen_listener || listener
    @last_seen_listener = listener
    decode(listener.data) do |event|
      what_based_listener.process_event(event)
    end
  end

  def buffer(line)
    @buffer_bytes += line.bytesize
    @buffer.push(line)
  end

  def reset_buffer
    @buffer = []
    @buffer_bytes = 0
  end

  # TODO: (colin) this method is not clearly documented as being required by the AutoFlush class & tasks.
  # I belive there is a problem here with the usage of @auto_flush_block which assumes to be in the
  # context of an IdentityMapCodec but the multiline codec could run without IdentityMapCodec
  def auto_flush
    flush_buffered_events(&@auto_flush_block)
  end

  # TODO: (colin) auto_flush_active? doesn't seem to be used anywhere. any reason to keep this api?
  def auto_flush_active?
    !@auto_flush_interval.nil?
  end

  # private

  # merge all currently bufferred events and call the passed block for the resulting merged event
  #
  # @param block [Proc] the closure that will be called for the resulting merged event
  def flush_buffered_events(&block)
    if block_given? && @buffer.any?
      no_error = true
      event = merge_events
      begin
        yield event
      rescue ::Exception => e
        # need to rescue everything
        # likliest cause: backpressure or timeout by exception
        # can't really do anything but leave the data in the buffer for next time if there is one
        @logger.error("Multiline: buffered events flush downstream error", :exception => e)
        no_error = false
      end
      reset_buffer if no_error
    end
  end

  # evalutate if a given line matches the configured pattern and call the appropriate do_next or do_previous
  # handler given the match state.
  #
  # @param line [String] the string to match against the pattern
  # @param block [Proc] the closure that will be called for each event that might result after processing this line
  def match_line(line, &block)
    match = @grok.match(line)
    @logger.debug? && @logger.debug("Multiline", :pattern => @pattern, :line => line, :match => !match.nil?, :negate => @negate)

    # Add negate option
    match = (match and !@negate) || (!match and @negate)
    @handler.call(line, match, &block)
  end

  def merge_events
    event = LogStash::Event.new(LogStash::Event::TIMESTAMP => @time, "message" => @buffer.join(NL))
    event.tag @multiline_tag if @multiline_tag && @buffer.size > 1
    event.tag "multiline_codec_max_bytes_reached" if over_maximum_bytes?
    event.tag "multiline_codec_max_lines_reached" if over_maximum_lines?
    event
  end

  def doing_previous?
    @what == "previous"
  end

  def what_based_listener
    doing_previous? ? @previous_listener : @last_seen_listener
  end

  def do_next(line, matched, &block)
    buffer(line)
    auto_flush_runner.start
    flush_buffered_events(&block) if !matched || buffer_over_limits?
  end

  def do_previous(line, matched, &block)
    flush_buffered_events(&block) if !matched || buffer_over_limits?
    auto_flush_runner.start
    buffer(line)
  end

  def over_maximum_lines?
    @buffer.size > @max_lines
  end

  def over_maximum_bytes?
    @buffer_bytes >= @max_bytes
  end

  def buffer_over_limits?
    over_maximum_lines? || over_maximum_bytes?
  end

  def auto_flush_runner
    @auto_flush_runner || AutoFlushUnset.new(nil, nil)
  end
end end end
