# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/codecs/auto_flush"
require "logstash/codecs/multiline"
require_relative "../supports/helpers.rb"

describe "AutoFlush and AutoFlushUnset" do
  before(:all) do
    @thread_abort = Thread.abort_on_exception
    Thread.abort_on_exception = true
  end

  after(:all) do
    Thread.abort_on_exception = @thread_abort
  end

  let(:flushable)  { Mlc::AutoFlushTracer.new }
  let(:flush_wait) { 0.25 }

  describe LogStash::Codecs::AutoFlush do
    subject { described_class.new(flushable, flush_wait) }

    context "when initialized" do
      it "#stopped? is false" do
        expect(subject.stopped?).to be_falsey
      end
    end

    context "when starting once" do
      before { subject.start }
      after  { subject.stop }

      it "calls auto_flush on flushable" do
        sleep flush_wait + 0.5
        expect(flushable.trace_for(:auto_flush)).to be_truthy
      end
    end

    context "when starting multiple times" do
      before { subject.start }
      after  { subject.stop }

      it "calls auto_flush on flushable" do
        expect(flushable.trace_for(:auto_flush)).to be_falsey
        sleep 0.1
        subject.start
        expect(flushable.trace_for(:auto_flush)).to be_falsey
        sleep 0.1
        subject.start
        expect(flushable.trace_for(:auto_flush)).to be_falsey

        sleep flush_wait + 0.5
        expect(flushable.trace_for(:auto_flush)).to be_truthy
      end
    end

    context "when retriggering during execution" do
      let(:flush_wait) { 1 }
      let(:delay)      { 1 }
      before do
        flushable.set_delay(delay)
        subject.start
      end
      after  { subject.stop }

      it "calls auto_flush twice on flushable" do
        now = Time.now.to_f
        combined_time = flush_wait + delay
        expect(flushable.trace_for(:auto_flush)).to be_falsey
        sleep flush_wait + 0.25 # <---- wait for execution to start
        subject.start          # <---- because of the semaphore, this waits for delay = 2 seconds, then starts a new thread
        sleep combined_time + 0.5   # <---- wait for more than 4 seconds = flush_wait + delay
        elapsed1, elapsed2 = flushable.full_trace_for(:delay).map{ |el| (el - now).floor }
        expect(elapsed1).to eq(combined_time)
        expect(elapsed2).to eq(2 * combined_time)
        expect(flushable.full_trace_for(:auto_flush)).to eq([true, true])
      end
    end

    context "when stopping before timeout" do
      before do
        subject.start
        sleep 0.1
        subject.stop
      end

      it "does not call auto_flush on flushable" do
        expect(flushable.trace_for(:auto_flush)).to be_falsey
      end

      it "#stopped? is true" do
        expect(subject.stopped?).to be_truthy
      end
    end
  end

  describe LogStash::Codecs::AutoFlushUnset do
    subject { described_class.new(flushable, 2) }

    it "#stopped? is true" do
      expect(subject.stopped?).to be_truthy
    end

    it "#start returns self" do
      expect(subject.start).to eq(subject)
    end

    it "#stop returns self" do
      expect(subject.start).to eq(subject)
    end
  end
end
