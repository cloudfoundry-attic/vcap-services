module FunctionalExampleGroup
  def self.included(base)
    base.instance_eval do
      include IntegrationExampleGroup
    end
  end
end
