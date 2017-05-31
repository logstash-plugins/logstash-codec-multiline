# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/codecs/auto_flush"
require "logstash/codecs/multiline"
require_relative "../supports/helpers.rb"

describe "AutoFlush and AutoFlushUnset" do
  let(:flushable)  { Mlc::AutoFlushTracer.new }
  let(:flush_wait) { 0.1 }

  describe LogStash::Codecs::AutoFlush do
    subject { described_class.new(flushable, flush_wait) }

    context "when initialized" do
      it "#pending? is false" do
        expect(subject.pending?).to be_falsy
      end

      it "#stopped? is false" do
        expect(subject.stopped?).to be_falsy
      end

      it "#finished? is true" do
        expect(subject.finished?).to be_truthy
      end
    end

    context "when started" do
      let(:flush_wait) { 20 }

      before { subject.start }
      after  { subject.stop }

      it "#pending? is true" do
        expect(subject.pending?).to be_truthy
      end

      it "#stopped? is false" do
        expect(subject.stopped?).to be_falsy
      end

      it "#finished? is false" do
        expect(subject.finished?).to be_falsy
      end
    end

    context "when finished" do
      before do
        subject.start
        sleep flush_wait + 0.1
      end

      after  { subject.stop }

      it "calls auto_flush on flushable" do
        expect(flushable.trace_for(:auto_flush)).to be_truthy
      end

      it "#pending? is false" do
        expect(subject.pending?).to be_falsy
      end

      it "#stopped? is false" do
        expect(subject.stopped?).to be_falsy
      end

      it "#finished? is true" do
        expect(subject.finished?).to be_truthy
      end
    end

    context "when stopped" do
      before do
        subject.start
        subject.stop
      end

      it "does not call auto_flush on flushable" do
        expect(flushable.trace_for(:auto_flush)).to be_falsy
      end

      it "#pending? is false" do
        expect(subject.pending?).to be_falsy
      end

      it "#stopped? is true" do
        expect(subject.stopped?).to be_truthy
      end

      it "#finished? is false" do
        expect(subject.finished?).to be_falsy
      end
    end
  end

  describe LogStash::Codecs::AutoFlushUnset do
    subject { described_class.new(flushable, 2) }

    it "#pending? is false" do
      expect(subject.pending?).to be_falsy
    end

    it "#stopped? is true" do
      expect(subject.stopped?).to be_truthy
    end

    it "#finished? is true" do
      expect(subject.finished?).to be_truthy
    end

    it "#start returns self" do
      expect(subject.start).to eq(subject)
    end

    it "#stop returns self" do
      expect(subject.start).to eq(subject)
    end
  end
end
