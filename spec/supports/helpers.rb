

def decode_events
  multiline =  LogStash::Codecs::Multiline.new(options)

  events = []
  random_number_of_events.times do |n|
    multiline.decode(sample_event) { |event| events << event }
  end

  # Grab the in-memory-event
  multiline.flush { |event| events << event }
  events
end

module Mlc
  class LineListener
    attr_reader :data, :path, :queue, :codec
    # use attr_reader to define noop methods of Listener API
    attr_reader :deleted, :created, :error, :eof #, :line

    def initialize(queue, codec, path = '')
      # store state from upstream
      @queue = queue
      @codec = codec
      @path = path
    end

    # receives a line from some upstream source
    # and sends it downstream
    def accept(data)
      @codec.accept dup_adding_state(data)
    end

    def process_event(event)
      event["path"] = path
      @queue << event
    end

    def add_state(data)
      @data = data
      self
    end

    private

    # dup and add state for downstream
    def dup_adding_state(line)
      self.class.new(queue, codec, path).add_state(line)
    end
  end

  class LineErrorListener < LineListener
    def process_event(event)
      raise StandardError.new("OMG, Daleks!")
    end
  end

  class MultilineRspec < LogStash::Codecs::Multiline
    def internal_buffer
      @buffer
    end
    def buffer_size
      @buffer.size
    end
  end

  class TracerBase
    def initialize() @tracer = []; end

    def trace_for(symbol)
      params = @tracer.assoc(symbol)
      params.nil? ? false : params.last
    end

    def clear()
      @tracer.clear()
    end
  end

  class MultilineLogTracer < TracerBase
    def warn(*args) @tracer.push [:warn, args]; end
    def error(*args) @tracer.push [:error, args]; end
    def debug(*args) @tracer.push [:debug, args]; end
    def info(*args) @tracer.push [:info, args]; end

    def info?() true; end
    def debug?() true; end
    def warn?() true; end
    def error?() true; end
  end

  class AutoFlushTracer < TracerBase
    def auto_flush() @tracer.push [:auto_flush, true]; end
  end

  class IdentityMapCodecTracer < TracerBase
    def clone() self.class.new; end
    def decode(data) @tracer.push [:decode, data]; end
    def encode(event) @tracer.push [:encode, event]; end
    def flush(&block) @tracer.push [:flush, block.call]; end
    def close() @tracer.push [:close, true]; end
    def logger() @logger ||= MultilineLogTracer.new; end
  end
end

RSpec::Matchers.define(:have_an_empty_buffer) do
  match do |actual|
    actual.buffer_size.zero?
  end

  failure_message do
    "Expecting #{actual.buffer_size} to be 0"
  end
end

RSpec::Matchers.define(:match_path_and_line) do |path, line|
  match do |actual|
    actual["path"] == path && actual["message"] == line.join($/)
  end

  failure_message do
    "Expecting #{actual['path']} to equal `#{path}` and #{actual["message"]} to equal #{line.join($/)}"
  end
end
