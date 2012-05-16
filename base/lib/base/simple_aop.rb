# Copyright (c) 2009-2012 VMware, Inc.
# Simple before filter to instance methods. We can chain filters to multiple methods.
# Filter method can return nil or raise error to terminater the evoke chain.
#
# Example:
# Class MyClass
#   include Before
#
#   def f1(msg) puts msg end
#
#   def f2
#     puts "f2"
#   end
#
#   def before_filter
#     puts "before"
#
#     true  # return true to proceed
#   end
#
#   # use before method after methods are defined
#   before [:f1, :f2], :before_filter
# end
#
# c = MyClass.new
# c.f1("hello")
# c.f2
# =>
# before
# hello
# before
# f2
module Before

  def self.included(base)
    base.extend ClassMethods
  end

  module ClassMethods
    PREFIX = "___".freeze

    def before(methods, callbacks)
      Array(methods).each do | method|
        method = method.to_sym
        callbacks = Array(callbacks).map{|callback| callback.to_sym}

        enhance_method(method, callbacks)
      end
    end

    # enhance single method with callbacks
    def enhance_method(method, callbacks)
      _method = (PREFIX + method.to_s).to_sym
      alias_method _method, method

      self.send(:define_method, method) do |*args, &blk|
        [callbacks, _method].flatten.each do |callback|
          break unless self.send(callback, *args, &blk)
        end
      end
    end
  end
end
