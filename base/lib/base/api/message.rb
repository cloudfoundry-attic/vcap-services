# Copyright (c) 2009-2011 VMware, Inc.
#
$:.unshift(File.expand_path("../..", __FILE__))
require 'json_message'
require 'base'

class ServiceMessage < JsonMessage

  def set_field(field, value)
    field = field.to_sym
    raise ValidationError.new({field => "Unknown field #{field}"}) unless self.class.fields.has_key?(field)
    f = self.class.fields[field]
    # delete an optional field
    if value.nil? and f.required == false
      @msg.delete(field)
    else
      errs = f.schema.validate(value)
      raise ValidationError.new({field => errs}) if errs
      @msg[field] = value
    end
  end

  # Return a deep copy of @msg
  def dup
    @msg.deep_dup
  end

  def inspect
    @msg.inspect
  end
end
