# encoding: utf-8
require "logstash/codecs/multiline"
require "logstash/event"
require "insist"
require_relative "../supports/helpers.rb"
# above helper also defines a subclass of Multiline
# called MultilineRspec that exposes the internal buffer
# and a Logger Mock

describe LogStash::Codecs::Multiline do
  context "#decode" do
    let(:config) { {} }
    let(:codec) { LogStash::Codecs::Multiline.new(config).tap {|c| c.register } }
    let(:events) { [] }
    let(:line_producer) do
      lambda do |lines|
        lines.each do |line|
          codec.decode(line) do |event|
            events << event
          end
        end
      end
    end

    it "should be able to handle multiline events with additional lines space-indented without sequencing by default" do
      config.update("pattern" => "^\\s", "what" => "previous")
      lines = [ "hello world", "   second line", "another first line" ]
      line_producer.call(lines)
      codec.flush { |e| events << e }

      expect(events.size).to eq(2)
      expect(events[0].get("message")).to eq "hello world\n   second line"
      expect(events[0].get("tags")).to include("multiline")
      expect(events[0].get("seq")).to be_nil
      expect(events[1].get("message")).to eq "another first line"
      expect(events[1].get("tags")).to be_nil
      expect(events[1].get("seq")).to be_nil
    end

    it "should be able to save the events' sequence number to a custom field name" do
      config.update("sequencer_enabled" => true, "sequencer_field" => "seqnr", "pattern" => "^\\s", "what" => "previous")
      lines = [ "1", "2", "3" ]
      line_producer.call(lines)
      codec.flush { |e| events << e }

      expect(events.size).to eq(3)
      events.each do |event|
        expect(event.get("seqnr")).to eq event.get("message").to_i
      end
    end

    it "should be able to sequence events with a custom start and rollover value" do
      config.update("sequencer_enabled" => true, "sequencer_start" => 10, "sequencer_rollover" => 13, "pattern" => "^\\s", "what" => "previous")
      lines = [ "10", "11", "12", "10" ]
      line_producer.call(lines)
      codec.flush { |e| events << e }

      expect(events.size).to eq(4)
      events.each do |event|
        expect(event.get("seq")).to eq event.get("message").to_i
      end
    end

    it "should be able to sequence events without counting each line and handle multiline events with additional lines space-indented" do
      config.update("sequencer_enabled" => true, "pattern" => "^\\s", "what" => "previous")
      lines = [ "hello world", "   second line", "another first line" ]
      line_producer.call(lines)
      codec.flush { |e| events << e }

      expect(events.size).to eq(2)
      expect(events[0].get("message")).to eq "hello world\n   second line"
      expect(events[0].get("tags")).to include("multiline")
      expect(events[0].get("seq")).to eq 1
      expect(events[1].get("message")).to eq "another first line"
      expect(events[1].get("tags")).to be_nil
      expect(events[1].get("seq")).to eq 2
    end

    it "should allow custom tag added to multiline events" do
      config.update("pattern" => "^\\s", "what" => "previous", "multiline_tag" => "hurray")
      lines = [ "hello world", "   second line", "another first line" ]
      line_producer.call(lines)
      codec.flush { |e| events << e }

      expect(events.size).to eq 2
      expect(events[0].get("tags")).to include("hurray")
      expect(events[1].get("tags")).to be_nil
    end

    it "should handle new lines in messages" do
      config.update("pattern" => '\D', "what" => "previous")
      lineio = StringIO.new("1234567890\nA234567890\nB234567890\n0987654321\n")
      until lineio.eof
        line = lineio.read(256) #when this is set to 36 the tests fail
        codec.decode(line) {|evt| events.push(evt)}
      end
      codec.flush { |e| events << e }
      expect(events[0].get("message")).to eq "1234567890\nA234567890\nB234567890"
      expect(events[1].get("message")).to eq "0987654321"
    end

    it "should allow grok patterns to be used" do
      config.update(
        "pattern" => "^%{NUMBER} %{TIME}",
        "negate" => true,
        "what" => "previous"
      )

      lines = [ "120913 12:04:33 first line", "second line", "third line" ]

      line_producer.call(lines)
      codec.flush { |e| events << e }

      insist { events.size } == 1
      insist { events.first.get("message") } == lines.join("\n")
    end

    context "using default UTF-8 charset" do

      it "should decode valid UTF-8 input" do
        config.update("pattern" => "^\\s", "what" => "previous")
        lines = [ "foobar", "κόσμε" ]
        lines.each do |line|
          expect(line.encoding.name).to eq "UTF-8"
          expect(line.valid_encoding?).to be_truthy
          codec.decode(line) { |event| events << event }
        end
        codec.flush { |e| events << e }
        expect(events.size).to eq 2

        events.zip(lines).each do |tuple|
          expect(tuple[0].get("message")).to eq tuple[1]
          expect(tuple[0].get("message").encoding.name).to eq "UTF-8"
        end
      end

      it "should escape invalid sequences" do
        config.update("pattern" => "^\\s", "what" => "previous")
        lines = [ "foo \xED\xB9\x81\xC3", "bar \xAD" ]
        lines.each do |line|
          expect(line.encoding.name).to eq "UTF-8"
          expect(line.valid_encoding?).to eq false

          codec.decode(line) { |event| events << event }
        end
        codec.flush { |e| events << e }
        expect(events.size).to eq 2

        events.zip(lines).each do |tuple|
          expect(tuple[0].get("message")).to eq tuple[1].inspect[1..-2]
          expect(tuple[0].get("message").encoding.name).to eq "UTF-8"
        end
      end
    end


    context "with valid non UTF-8 source encoding" do

      it "should encode to UTF-8" do
        config.update("charset" => "ISO-8859-1", "pattern" => "^\\s", "what" => "previous")
        samples = [
          ["foobar", "foobar"],
          ["\xE0 Montr\xE9al", "à Montréal"],
        ]

        # lines = [ "foo \xED\xB9\x81\xC3", "bar \xAD" ]
        samples.map{|(a, b)| a.force_encoding("ISO-8859-1")}.each do |line|
          expect(line.encoding.name).to eq "ISO-8859-1"
          expect(line.valid_encoding?).to eq true

          codec.decode(line) { |event| events << event }
        end
        codec.flush { |e| events << e }
        expect(events.size).to eq 2

        events.zip(samples.map{|(a, b)| b}).each do |tuple|
          expect(tuple[1].encoding.name).to eq "UTF-8"
          expect(tuple[0].get("message")).to eq tuple[1]
          expect(tuple[0].get("message").encoding.name).to eq "UTF-8"
        end
      end
    end

    context "with invalid non UTF-8 source encoding" do

     it "should encode to UTF-8" do
        config.update("charset" => "ASCII-8BIT", "pattern" => "^\\s", "what" => "previous")
        samples = [
          ["\xE0 Montr\xE9al", "� Montr�al"],
          ["\xCE\xBA\xCF\x8C\xCF\x83\xCE\xBC\xCE\xB5", "����������"],
        ]
        events = []
        samples.map{|(a, b)| a.force_encoding("ASCII-8BIT")}.each do |line|
          expect(line.encoding.name).to eq "ASCII-8BIT"
          expect(line.valid_encoding?).to eq true

          codec.decode(line) { |event| events << event }
        end
        codec.flush { |e| events << e }
        expect(events.size).to eq 2

        events.zip(samples.map{|(a, b)| b}).each do |tuple|
          expect(tuple[1].encoding.name).to eq "UTF-8"
          expect(tuple[0].get("message")).to eq tuple[1]
          expect(tuple[0].get("message").encoding.name).to eq "UTF-8"
        end
      end

    end
  end

  context "with non closed multiline events" do
    let(:random_number_of_events) { rand(300..1000) }
    let(:sample_event) { "- Sample event" }
    let(:events) { decode_events }
    let(:unmerged_events_count) { events.collect { |event| event.get("message").split(LogStash::Codecs::Multiline::NL).size }.inject(&:+) }

    context "break on maximum_lines" do
      let(:max_lines) { rand(10..100) }
      let(:options) {
        {
          "pattern" => "^-",
          "what" => "previous",
          "max_lines" => max_lines,
          "max_bytes" => "2 mb"
        }
      }

      it "flushes on a maximum lines" do
        expect(unmerged_events_count).to eq(random_number_of_events)
      end

      it "tags the event" do
        expect(events.first.get("tags")).to include("multiline_codec_max_lines_reached")
      end
    end

    context "break on maximum bytes" do
      let(:max_bytes) { rand(30..100) }
      let(:options) {
        {
          "pattern" => "^-",
          "what" => "previous",
          "max_lines" => 20000,
          "max_bytes" => max_bytes
        }
      }

      it "flushes on a maximum bytes size" do
        expect(unmerged_events_count).to eq(random_number_of_events)
      end

      it "tags the event" do
        expect(events.first.get("tags")).to include("multiline_codec_max_bytes_reached")
      end
    end
  end

  describe "auto flushing" do
    let(:config) { {} }
    let(:events) { [] }
    let(:lines) do
      { "en.log" => ["hello world", " second line", " third line"],
        "fr.log" => ["Salut le Monde", " deuxième ligne", " troisième ligne"],
        "de.log" => ["Hallo Welt"] }
    end
    let(:listener_class) { Mlc::LineListener }
    let(:auto_flush_interval) { 2 }

    let(:line_producer) do
      lambda do |path|
        #create a listener that holds upstream state
        listener = listener_class.new(events, codec, path)
        lines[path].each do |data|
          listener.accept(data)
        end
      end
    end

    let(:codec) do
      Mlc::MultilineRspec.new(config).tap {|c| c.register}
    end

    before :each do
      expect(LogStash::Codecs::Multiline).to receive(:logger).and_return(Mlc::MultilineLogTracer.new).at_least(:once)
    end

    context "when auto_flush_interval is not set" do
      it "does not build any events" do
        config.update("pattern" => "^\\s", "what" => "previous")
        line_producer.call("en.log")
        sleep auto_flush_interval + 0.1
        expect(events.size).to eq(0)
        expect(codec.buffer_size).to eq(3)
      end
    end

    context "when the auto_flush raises an exception" do
      let(:errmsg) { "OMG, Daleks!" }
      let(:listener_class) { Mlc::LineErrorListener }

      it "does not build any events, logs an error and the buffer data remains" do
        config.update("pattern" => "^\\s", "what" => "previous",
          "auto_flush_interval" => auto_flush_interval)
        line_producer.call("en.log")
        sleep(auto_flush_interval + 0.2)
        msg, args = codec.logger.trace_for(:error)
        expect(msg).to eq("Multiline: flush downstream error")
        expect(args[:exception].message).to eq(errmsg)
        expect(events.size).to eq(0)
        expect(codec.buffer_size).to eq(3)
      end
    end

    def assert_produced_events(key, sleeping)
      line_producer.call(key)
      sleep(sleeping)
      yield
      expect(codec).to have_an_empty_buffer
    end

    context "mode: previous, when there are pauses between multiline file writes" do
      it "auto-flushes events from the accumulated lines to the queue" do
        config.update("pattern" => "^\\s", "what" => "previous",
          "auto_flush_interval" => auto_flush_interval)

        assert_produced_events("en.log", auto_flush_interval + 0.1) do
          expect(events[0]).to match_path_and_line("en.log", lines["en.log"])
        end

        line_producer.call("fr.log")
        #next line(s) come before auto-flush i.e. assert its buffered
        sleep(auto_flush_interval - 0.3)
        expect(codec.buffer_size).to eq(3)
        expect(events.size).to eq(1)

        assert_produced_events("de.log", auto_flush_interval + 0.1) do
          # now the events are generated
          expect(events[1]).to match_path_and_line("fr.log", lines["fr.log"])
          expect(events[2]).to match_path_and_line("de.log", lines["de.log"])
        end
      end
    end

    context "mode: next, when there are pauses between multiline file writes" do

      let(:lines) do
        { "en.log" => ["hello world++", "second line++", "third line"],
          "fr.log" => ["Salut le Monde++", "deuxième ligne++", "troisième ligne"],
          "de.log" => ["Hallo Welt"] }
      end

      it "auto-flushes events from the accumulated lines to the queue" do
        config.update("pattern" => "\\+\\+$", "what" => "next",
          "auto_flush_interval" => auto_flush_interval)

        assert_produced_events("en.log", auto_flush_interval + 0.1) do
          # wait for auto_flush
          expect(events[0]).to match_path_and_line("en.log", lines["en.log"])
        end

        assert_produced_events("de.log", auto_flush_interval - 0.3) do
          #this file is read before auto-flush
          expect(events[1]).to match_path_and_line("de.log", lines["de.log"])
        end

        assert_produced_events("fr.log", auto_flush_interval + 0.1) do
          # wait for auto_flush
          expect(events[2]).to match_path_and_line("fr.log", lines["fr.log"])
        end
      end
    end
  end
end
