# Copyright (c) 2009-2011 VMware, Inc.
require 'spec_helper'

module Do

  # the tests below do various things then wait for something to
  # happen -- so there's a potential for a race condition.  to
  # minimize the risk of the race condition, increase this value (0.1
  # seems to work about 95% of the time); but to make the tests run
  # faster, decrease it
  STEP_DELAY = 0.1

  def self.at(index, &blk)
    EM.add_timer(index*STEP_DELAY) { blk.call if blk }
  end
end

