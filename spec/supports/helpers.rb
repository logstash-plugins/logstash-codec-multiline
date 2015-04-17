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
