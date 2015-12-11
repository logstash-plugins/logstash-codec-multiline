# encoding: utf-8
require "concurrent"

module LogStash module Codecs class AutoFlush
  def initialize(mc, interval)
    @mc, @interval = mc, interval
    @stopped = Concurrent::AtomicBoolean.new # false by default
  end

  def start
    # can't start if pipeline is stopping
    return self if stopped?
    if pending?
      @task.reset
    elsif finished?
      @task = Concurrent::ScheduledTask.execute(@interval) do
        @mc.auto_flush()
      end
    # else the task is executing
    end
    self
  end

  def finished?
    return true if @task.nil?
    @task.fulfilled?
  end

  def pending?
    @task && @task.pending?
  end

  def stopped?
    @stopped.value
  end

  def stop
    @stopped.make_true
    @task.cancel if pending?
  end
end

class AutoFlushUnset
  def initialize(mc, interval)
  end

  def pending?
    false
  end

  def stopped?
    true
  end

  def start
    self
  end

  def finished?
    true
  end

  def stop
    self
  end
end end end
