require_relative "../../../spec_helper"
require "marketplaces/appdirect/addon_plan"

module VCAP::Services::Marketplace::Appdirect
  describe AddonPlan do
    subject(:plan){
      described_class.new('id' => 1, 'description' => 'an awesome plan',
        'free' => false, 'external_id' => 'addonOffering_18')
    }

    describe '#to_hash' do
      it 'has the right keys' do
        subject.to_hash.keys.should =~ %w(id description free external_id extra)
      end

      context 'key "extra"' do
        let(:hash) { plan.to_hash.fetch('extra') }

        it "has nil extra be default" do
          hash.should be_nil
        end
      end
    end

    describe "fetching extra information from public service description" do
      let(:extra_information_without_usd_pricing) do
        {
          'addonOfferings' => [
            {
              'id'  => 18,
              'paymentPlans' => [
                {
                  'id' => 190,
                  'frequency' => 'MONTHLY',
                  'costs' => [
                    {
                      'amounts' => [
                        {
                          'currency' => 'AUD',
                          'value' => 30
                        },
                      ]
                    }
                  ]
                }
              ]
            }
          ]
        }
      end
      let(:extra_information) do
        {
          'addonOfferings' => [
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
                          'currency' => 'AUD',
                          'value' => 30
                        },
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
        }
      end

      it "correctly selects the one with matching id" do
        addon_attrs = subject.addon_attrs(extra_information["addonOfferings"])
        addon_attrs.fetch('id').should == 18
        addon_attrs.fetch('code').should == 'this is not the real code'
      end

      context "assigning extra information" do
        before do
          subject.assign_extra_information(extra_information)
        end

        it 'extra has the correct keys' do
          subject.to_hash.fetch('extra').keys.should =~ %w(cost bullets)
        end

        it "fetches the cost in USD from addon attributes" do
          subject.extra.fetch('cost').should == 20.00
        end

        it "raises error when no USD pricing info is available" do
          expect {
            subject.assign_extra_information(extra_information_without_usd_pricing)
          }.to raise_error /A USD pricing is required/
        end

        it "uses the descriptionHtml as the bullets" do
          subject.extra.fetch('bullets').should == ['description HTML']
        end
      end
    end
  end
end

