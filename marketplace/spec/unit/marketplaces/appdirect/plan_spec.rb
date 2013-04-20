require_relative "../../../spec_helper"
require "marketplaces/appdirect/plan"

module VCAP::Services::Marketplace::Appdirect
  describe Plan do
    subject(:plan){
      Plan.new('id' => 1, 'description' => 'an awesome plan', 'free' => false, 'external_id' => 'addonOffering_18')
    }

    describe '#to_hash' do
      it 'has the right keys' do
        subject.to_hash.keys.should =~ %w(id description free external_id extra)
      end

      context 'key "extra"' do
        let(:hash) { plan.to_hash.fetch('extra') }

        it "has empty extra be default" do
          hash.should == {}
        end
      end
    end

    describe "fetching extra information from public service description" do
      let(:addons_attr_list) do
        [
          {
            'id'  => 19,
          },
          {
            'id'  => 18,
            'code' => 'this is not the real code',
            'descriptionHtml'  => 'description HTML',
            'paymentPlans' => [
              {
                'id' => 190,
                'frequency' => 'MONTHLY',
                'contract' => {
                  'blockSwitchToShorterContract' => false,
                  'blockContractDowngrades' => false,
                  'blockContractUpgrades' => false,
                },
                'allowCustomUsage' => false,
                'keepBillDateOnUsageChange' => false,
                'separatePrepaid' => false,
                'costs' => [
                  {
                    'unit' => 'NOT_APPLICABLE',
                    'minUnits' => 0E-10,
                    'maxUnits' => nil,
                    'meteredUsage' => false,
                    'increment' => nil,
                    'pricePerIncrement' => false,
                    'amounts' => [
                      {
                        'currency' => 'USD',
                        'value' => 20
                      }
                    ]
                  }
                ]
              }
            ]
          }

        ]
      end

      it "correctly selects the one with matching id" do
        addon_attrs = subject.addon_attrs(addons_attr_list)
        addon_attrs.fetch('id').should == 18
        addon_attrs.fetch('code').should == 'this is not the real code'
      end

      context "assigning extra information" do
        before do
          subject.assign_extra_information(addons_attr_list)
        end

        it 'extra has the correct keys' do
          subject.to_hash.fetch('extra').keys.should =~ %w(cost bullets)
        end

        it "fetches the cost and bullets from addon attributes" do
          subject.extra.fetch('cost').should == 20.00
        end

        it "uses the descriptionHtml as the bullets" do
          subject.extra.fetch('bullets').should == ['description HTML']
        end
      end
    end
  end
end

