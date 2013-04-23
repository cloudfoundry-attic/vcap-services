require_relative "../../../spec_helper"
require "marketplaces/appdirect/plan_factory"

describe VCAP::Services::Marketplace::Appdirect::PlanFactory do
  let(:basic_attrs) {
    {
      "id" => "free",
      "description" => "Free",
      "free" => true,
    }
  }
  describe ".build" do
    it "creates a correct addon plan" do
      plan = described_class.build(basic_attrs.merge('external_id'  => 'addonOffering_18'))
      plan.should be_a(VCAP::Services::Marketplace::Appdirect::AddonPlan)
    end

    it "creates a correct edition plan" do
      plan = described_class.build(basic_attrs.merge('external_id' => 'edition_85'))
      plan.should be_a(VCAP::Services::Marketplace::Appdirect::EditionPlan)
    end

    it "is defensive" do
      expect do
        described_class.build(basic_attrs.merge('external_id' => 'edition851'))
      end.to raise_error(ArgumentError)
    end
  end
end
