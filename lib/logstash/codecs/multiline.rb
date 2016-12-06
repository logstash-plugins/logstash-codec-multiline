# encoding: utf-8
require "logstash/codecs/base"
require "logstash/util/charset"
require "logstash/timestamp"
require "logstash/codecs/auto_flush"

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
#           what => "previous"
#         }
#       }
#     }
#
# This says that any line not starting with a timestamp should be merged with the previous line.
#
# One more common example is C line continuations (backslash). Here's how to do that:
# [source,ruby]
#     input {
#       stdin {
#         codec => multiline {
#           pattern => "\\$"
#           what => "next"
#         }
#       }
#     }
#
# This says that any line ending with a backslash should be combined with the
# following line.
#
# Another use case for the multiline codec is to add sequence numbers to every event.
# For example, when you need to retain the original order of the events, but your 
# applications logged multiple events on the exact same timestamp, then there is
# currently no way to achieve it without using some identifier to sort the events into
# their original order again.
#
# For example:
# [source,ruby]
#     input {
#       stdin {
#         codec => multiline {
#           pattern => "^\s"
#           what => "previous"
#           sequencer_enabled => true
#           sequencer_field => "seqnr"
#           sequencer_start => 0
#           sequencer_rollover => 10000
#         }
#       }
#     }
#
# The `sequencer_enabled` can be `true` or `false (defaults to `false`). If
# `true`, every event will get a sequence number saved to the `sequencer_field`
# field.
#
# The `sequencer_field` (defaults to `"seq"`) may be used to let the multiline
# codec save the sequence number to a custom field name.
#
# The `sequencer_start` (defaults to `1`) defines the number at which the
# sequence number would start at and also the number at which it would rollover
# to, once the `sequencer_rollover` value has been reached.
#
# The `sequencer_rollover` (defaults to `100000`) defines the upper boundary
# at which the number sequence would reset back to `sequencer_start`. Take note
# that the sequence number can never be equal to `sequencer_rollover` and
# `sequencer_start` must always be smaller than `sequencer_rollover`.
#
# Please take note that the sequencer has only been tested when used in conjunction with
# the `file` input plugin of Logstash, which has a single worker per file handle.
# The sequence cannot be guaranteed when Logstash is configured to use multiple pipeline
# workers and the multiline codec isn't used for an input plugin.
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
  # matching new line is seen or there has been no new data appended for this many
  # seconds. No default.  If unset, no auto_flush. Units: seconds
  config :auto_flush_interval, :validate => :number

  # Whether the event should get an incremental sequence number.
  # If unset, no sequence number field would be set for the events.
  config :sequencer_enabled, :validate => :boolean, :default => false

  # Defines the field name where the sequence number would be set.
  config :sequencer_field, :validate => :string, :default => "seq"

  # The number at which the sequencer would start at and also rollover to
  # once the rollover value has been reached.
  config :sequencer_start, :validate => :number, :default => 1

  # The number at which the sequencer would rollover to sequence_start again.
  # The max sequence number would always be smaller than sequencer_rollover,
  # and never equal to sequencer_rollover.
  config :sequencer_rollover, :validate => :number, :default => 100000

  public

  def register
    require "grok-pure" # rubygem 'jls-grok'
    require 'logstash/patterns/core'

    # Detect if we are running from a jarfile, pick the right path.
    patterns_path = []
    patterns_path += [LogStash::Patterns::Core.path]

    @sequence = @sequencer_start
    @grok = Grok.new

    @patterns_dir = patterns_path.to_a + @patterns_dir
    @patterns_dir.each do |path|
      if ::File.directory?(path)
        path = ::File.join(path, "*")
      end

      Dir.glob(path).each do |file|
        @logger.debug("Grok loading patterns from file", :path => file)
        @grok.add_patterns_from_file(file)
      end
    end

    @grok.compile(@pattern)
    @logger.trace("Registered multiline plugin", :type => @type, :config => @config)

    reset_buffer

    @handler = method("do_#{@what}".to_sym)

    @converter = LogStash::Util::Charset.new(@charset)
    @converter.logger = @logger
    if @auto_flush_interval
      # will start on first decode
      @auto_flush_runner = AutoFlush.new(self, @auto_flush_interval)
    end
  end # def register

  def use_mapper_auto_flush
    return unless auto_flush_active?
    @auto_flush_runner = AutoFlushUnset.new(nil, nil)
    @auto_flush_interval = @auto_flush_interval.to_f
  end

  def accept(listener)
    # memoize references to listener that holds upstream state
    @previous_listener = @last_seen_listener || listener
    @last_seen_listener = listener
    decode(listener.data) do |event|
      what_based_listener.process_event(event)
    end
  end

  def decode(text, &block)
    text = @converter.convert(text)
    text.split("\n").each do |line|
      match = @grok.match(line)
      @logger.debug("Multiline", :pattern => @pattern, :text => line,
                    :match => !match.nil?, :negate => @negate)

      # Add negate option
      match = (match and !@negate) || (!match and @negate)
      @handler.call(line, match, &block)
    end
  end # def decode

  def buffer(text)
    @buffer_bytes += text.bytesize
    @buffer.push(text)
  end

  def flush(&block)
    if block_given? && @buffer.any?
      no_error = true
      events = merge_events
      begin
        yield events
      rescue ::Exception => e
        # need to rescue everything
        # likliest cause: backpressure or timeout by exception
        # can't really do anything but leave the data in the buffer for next time if there is one
        @logger.error("Multiline: flush downstream error", :exception => e)
        no_error = false
      end
      reset_buffer if no_error
    end
  end

  def auto_flush(listener = @last_seen_listener)
    return if listener.nil?

    flush do |event|
      listener.process_event(event)
    end
  end

  def merge_events
    event = LogStash::Event.new(LogStash::Event::TIMESTAMP => @time, "message" => @buffer.join(NL))
    event.tag @multiline_tag if @multiline_tag && @buffer.size > 1
    event.tag "multiline_codec_max_bytes_reached" if over_maximum_bytes?
    event.tag "multiline_codec_max_lines_reached" if over_maximum_lines?
    apply_sequence(event) if @sequencer_enabled
    event
  end

  def reset_buffer
    @buffer = []
    @buffer_bytes = 0
  end

  def doing_previous?
    @what == "previous"
  end

  def what_based_listener
    doing_previous? ? @previous_listener : @last_seen_listener
  end

  def do_next(text, matched, &block)
    buffer(text)
    auto_flush_runner.start
    flush(&block) if !matched || buffer_over_limits?
  end

  def do_previous(text, matched, &block)
    flush(&block) if !matched || buffer_over_limits?
    auto_flush_runner.start
    buffer(text)
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

  def encode(event)
    # Nothing to do.
    @on_event.call(event, event)
  end # def encode

  def close
    auto_flush_runner.stop
  end

  def auto_flush_active?
    !@auto_flush_interval.nil?
  end

  def auto_flush_runner
    @auto_flush_runner || AutoFlushUnset.new(nil, nil)
  end

  def apply_sequence(event)
    event.set(@sequencer_field, @sequence)

    @sequence += 1
    @sequence = @sequencer_start if @sequence >= @sequencer_rollover
  end
end end end # class LogStash::Codecs::Multiline
