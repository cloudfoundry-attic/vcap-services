require 'spec_helper'

describe VCAP::Services::Mysql::Node do
  before do
    VCAP::Services::Mysql::Node.any_instance.stub(:initialize)
  end

  describe "#get_host" do
    context "when the mysql option provided host is localhost" do
      before do
        subject.instance_variable_set(:@local_ip, "base_ip")
        subject.instance_variable_set(:@mysql_configs, {"5.5" => {
            "host" => "localhost"
        }})
      end

      it "returns the IP of this machine" do
        subject.get_host.should == "base_ip"
      end
    end

    context "when the mysql option host is an external hostname" do
      before do
        subject.instance_variable_set(:@mysql_configs, {"5.5" => {
            "host" => "external.example.com"
        }})
      end

      it "returns the external hostname" do
        subject.get_host.should == "external.example.com"
      end
    end
  end
end
