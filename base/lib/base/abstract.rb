# Copyright (c) 2009-2011 VMware, Inc.
class Class
  def abstract(*args)
    args.each do |method_name|
      define_method(method_name) do |*args|
        raise NotImplementedError.new("Unimplemented abstract method #{self.class.name}##{method_name}")
      end
    end
  end
end

