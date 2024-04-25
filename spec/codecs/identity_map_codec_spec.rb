# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/codecs/identity_map_codec"
require "logstash/codecs/multiline"
require_relative "../supports/helpers.rb"

describe LogStash::Codecs::IdentityMapCodec do
  let(:codec)   { Mlc::IdentityMapCodecTracer.new }
  let(:logger)  { codec.logger }
  let(:demuxer) { described_class.new(codec) }
  let(:stream1) { "stream-a" }
  let(:codec1)  { demuxer.codec_without_usage_update(stream1) }
  let(:arg1)    { "data-a" }

  before(:all) do
    @thread_abort = Thread.abort_on_exception
    Thread.abort_on_exception = true
  end

  after(:all) do
    Thread.abort_on_exception = @thread_abort
  end

  after do
    codec.close
  end

  describe "operating without stream identity" do
    let(:stream1) { nil }

    it "transparently refers to the original codec" do
      resolved_codec = demuxer.codec_without_usage_update(stream1)
      expect(resolved_codec).to eq(codec)
    end
  end

  describe "operating with stream identity" do

    before { demuxer.decode(arg1, stream1) }

    it "the first identity refers to a copy of original codec" do
      expect(codec1).to_not eql(codec)
    end
  end

  describe "#decode" do
    context "when no identity is used" do
      let(:stream1) { nil }

      it "calls the method on the original codec" do
        demuxer.decode(arg1, stream1)

        expect(codec.trace_for(:decode)).to eq(arg1)
      end
    end

    context "when multiple identities are used" do
      let(:stream2) { "stream-b" }
      let(:codec2) { demuxer.codec_without_usage_update(stream2) }
      let(:arg2)   { "data-b" }

      it "calls the method on the appropriate codec" do
        demuxer.decode(arg1, stream1)
        demuxer.decode(arg2, stream2)

        expect(codec1.trace_for(:decode)).to eq(arg1)
        expect(codec2.trace_for(:decode)).to eq(arg2)
      end
    end
  end

  describe "#encode" do
    context "when no identity is used" do
      let(:stream1) { nil }
      let(:arg1) { LogStash::Event.new({"type" => "file"}) }

      it "calls the method on the original codec" do
        demuxer.encode(arg1, stream1)

        expect(codec.trace_for(:encode)).to eq(arg1)
      end
    end

    context "when multiple identities are used" do
      let(:stream2) { "stream-b" }
      let(:codec2) { demuxer.codec_without_usage_update(stream2) }
      let(:arg2)   { LogStash::Event.new({"type" => "file"}) }

      it "calls the method on the appropriate codec" do
        demuxer.encode(arg1, stream1)
        demuxer.encode(arg2, stream2)

        expect(codec1.trace_for(:encode)).to eq(arg1)
        expect(codec2.trace_for(:encode)).to eq(arg2)
      end
    end
  end

  describe "#close" do
    context "when no identity is used" do
      before do
        demuxer.decode(arg1)
      end

      it "calls the method on the original codec" do
        demuxer.close
        expect(codec.trace_for(:close)).to be_truthy
      end
    end

    context "when multiple identities are used" do
      let(:stream2) { "stream-b" }
      let(:codec2) { demuxer.codec_without_usage_update(stream2) }
      let(:arg2)   { LogStash::Event.new({"type" => "file"}) }

      before do
        demuxer.decode(arg1, stream1)
        demuxer.decode(arg2, stream2)
      end

      it "calls the method on all codecs" do
        demuxer.close

        expect(codec1.trace_for(:close)).to be_truthy
        expect(codec2.trace_for(:close)).to be_truthy
      end
    end

    context '(multiple threads)' do
      let(:thread_count) { (ENV['THREAD_COUNT'] || 100).to_i }

      let(:running_threads) do
        thread_count.times.map do
          Thread.start(described_class.new(codec)) do |codec|
            codec.decode(arg1, stream1)
            Thread.pass
            codec.close
          end
        end
      end

      let(:stop_timeout) { 5 }

      it 'does not linger around' do
        running_threads.each do |thread|
          next unless thread.alive?
          sleep(0.5)
          if thread.alive?
            sleep(stop_timeout)
            fail "codec did not stop in #{stop_timeout}s" if thread.alive?
          end
        end
      end

    end
  end

  describe "over capacity protection" do
    let(:demuxer) { described_class.new(codec).max_identities(limit) }

    context "when capacity at 80% or higher" do
      let(:limit) { 10 }

      it "a warning is logged" do
        limit.pred.times do |i|
          demuxer.decode(Object.new, "stream#{i}")
        end

        expect(logger.trace_for(:warn).first).to match %r|has reached 80% capacity|
      end
    end

    context "when capacity is exceeded" do
      let(:limit) { 2 }
      let(:error_class) { LogStash::Codecs::IdentityMapCodec::IdentityMapUpperLimitException }

      it "an exception is raised" do
        limit.times do |i|
          demuxer.decode(Object.new, "stream#{i}")
        end
        expect { demuxer.decode(Object.new, "stream4") }.to raise_error(error_class)
      end

      context "initially but some streams are idle and can be evicted" do
        let(:demuxer) { described_class.new(codec).max_identities(limit).evict_timeout(1) }

        it "an exception is NOT raised" do
          demuxer.decode(Object.new, "stream1")
          sleep(1.2)
          demuxer.decode(Object.new, "stream2")
          expect(demuxer.identity_count).to eq(limit)
          expect { demuxer.decode(Object.new, "stream4"){|*| 42 } }.not_to raise_error
        end
      end
    end
  end

  describe "usage tracking" do
    let(:demuxer) { described_class.new(codec).evict_timeout(10) }
    context "when an operation is performed by identity" do
      it "the new eviction time for that identity is recorded" do
        demuxer.decode(Object.new, "stream1")
        current_eviction = demuxer.eviction_timestamp_for("stream1")
        sleep(2)
        demuxer.decode(Object.new, "stream1")
        expect(demuxer.eviction_timestamp_for("stream1")).to be >= current_eviction + 2
      end
    end
  end

  describe "codec eviction" do
    context "when an identity has become stale" do
      let(:demuxer) { described_class.new(codec).evict_timeout(1).cleaner_interval(1) }
      it "the cleaner evicts the codec and flushes it first" do
        demuxer.decode(Object.new, "stream1"){|*| 42}
        mapped_codec = demuxer.codec_without_usage_update("stream1") # get before eviction

        sleep(2.1)

        expect(mapped_codec.trace_for(:flush)).to eq(42)
        expect(demuxer.identity_map.keys).not_to include("stream1")
      end
    end

    context "when an identity has become stale and an evition block is set" do
      let(:demuxer) do
        described_class.new(codec)
        .evict_timeout(1)
        .cleaner_interval(1)
        .eviction_block(lambda {|*| 24} )
      end
      it "the cleaner evicts the codec and flushes it first using the eviction_block" do
        demuxer.decode(Object.new, "stream1"){|*| 42}
        mapped_codec = demuxer.codec_without_usage_update("stream1") # get before eviction

        sleep(2.1)

        expect(mapped_codec.trace_for(:flush)).to eq(24)
        expect(demuxer.identity_map.keys).to_not include("stream1")
      end
    end
  end

  describe "observer/listener based processing" do
    let(:listener) { Mlc::LineListener }
    let(:queue)    { [] }
    let(:identity) { "stream1" }
    let(:config)   { {"pattern" => "^\\s", "what" => "previous"} }
    let(:mlc) { Mlc::MultilineRspec.new(config).tap {|c| c.register } }
    let(:imc) { described_class.new(mlc) }

    before do
      l_0 = Mlc::LineListener.new(queue, imc, identity).tap {|o| o.accept("foo")}
    end

    describe "normal processing" do
      context "when wrapped codec has auto-flush deactivated" do
        it "no events are generated (the line is buffered)" do
          expect(imc.identity_count).to eq(1)
          expect(queue.size).to eq(0)

          mapped_codec = imc.codec_without_usage_update(identity)
          expect(mapped_codec.internal_buffer[0]).to eq("foo")
        end
      end

      context "when wrapped codec has auto-flush activated" do
        let(:config) { {"pattern" => "^\\s", "what" => "previous", "auto_flush_interval" => 0.2} }
        before do
          l_1 = Mlc::LineListener.new(queue, imc, "stream2").tap {|o| o.accept("bar")}
          l_2 = Mlc::LineListener.new(queue, imc, "stream3").tap {|o| o.accept("baz")}
        end
        it "three events are auto flushed from three different identities/codecs" do
          sleep 0.6 # wait for auto_flush - in multiples of 0.5 plus 0.1
          expect(queue.size).to eq(3)
          e1, e2, e3 = queue
          expect(e1.get("path")).to eq("stream1")
          expect(e1.get("message")).to eq("foo")

          expect(e2.get("path")).to eq("stream2")
          expect(e2.get("message")).to eq("bar")

          expect(e3.get("path")).to eq("stream3")
          expect(e3.get("message")).to eq("baz")
          expect(imc.identity_count).to eq(3)
        end
      end
    end

    describe "evict method" do
      context "when evicting and wrapped codec implements auto-flush" do
        it "flushes and removes the identity" do
          expect(imc.identity_count).to eq(1)
          imc.evict(identity)
          expect(queue[0].get("message")).to eq("foo")
          expect(imc.identity_count).to eq(0)
        end
      end
    end
  end
end

describe LogStash::Codecs::IdentityMapCodec::PeriodicRunner do
  let(:listener) do
    double('Listener', logger: double('Logger').as_null_object,
           some_method: double('method_symbol').as_null_object)
  end
  let(:interval) { 1 }
  let(:method_symbol) { :some_method }
  subject(:runner) { described_class.new(listener, interval, method_symbol) }

  before(:all) do
    Thread.abort_on_exception = true
  end

  describe "normal shutdown" do
    it "starts first and stops successfully" do
      start_thread = Thread.start do
        runner.start
      end

      stop_thread = Thread.start do
        sleep(1)
        runner.stop
      end

      start_thread.join
      stop_thread.join

      expect(runner.instance_variable_get('@listener')).to be_nil
    end
  end

  describe "race condition" do
    let(:atomic_bool) { Concurrent::AtomicBoolean.new(true) }
    let(:running) { double('AtomicBooleanStub') }

    before do
      allow(running).to receive(:make_true) do
        atomic_bool.make_true
      end

      allow(running).to receive(:make_false) do
        atomic_bool.make_false
        sleep(2)
      end

      allow(running).to receive(:true?) do
        atomic_bool.true?
      end
    end

    before :each do
      runner.instance_variable_set('@running', running)
    end

    it "starts several times and stops successfully" do
      start_thread = Thread.start do
        5.times do
          runner.start
          sleep(1)
        end
      end

      stop_thread = Thread.start do
        sleep(1) # give time to runner start
        runner.stop
      end

      start_thread.join
      stop_thread.join

      expect(runner.instance_variable_get('@listener')).to be_nil
      expect(runner.instance_variable_get('@thread').alive?).to be_falsey
    end
  end
end