# encoding: utf-8
require "concurrent"

module LogStash module Codecs class AutoFlush
  def initialize(mc, interval)
    @mc, @interval = mc, interval
    @stopped = Concurrent::AtomicBoolean.new # false by default
  end

  # def start
  #   # can't start if pipeline is stopping
  #   return self if stopped?
  #   if pending?
  #     @task.cancel
  #     create_task
  #   elsif finished?
  #     create_task
  #   # else the task is executing
  #   end
  #   self
  # end

  def start
    # can't start if pipeline is stopping
    return self if stopped?

    if pending? && @task.cancel
      create_task
      return self
    end
    # maybe we have a timing edge case
    # where pending? was true but cancel failed
    # because the task started running
    if finished?
      create_task
      return self
    end
    # else the task is executing
    # wait for task to complete
    # flush could feasibly block on queue access
    @task.value
    create_task
    self
  end

  def create_task
    @task = Concurrent::ScheduledTask.execute(@interval) do
      @mc.auto_flush()
    end
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
    cancel
  end

  def cancel
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

  def cancel
    self
  end
end end end
