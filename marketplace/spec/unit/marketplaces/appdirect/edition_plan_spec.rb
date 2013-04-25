require_relative "../../../spec_helper"
require "marketplaces/appdirect/edition_plan"

module VCAP::Services::Marketplace::Appdirect
  describe EditionPlan do
    subject(:plan){
      described_class.new(
        'id' => 1,
        'description' => 'an awesome plan',
        'free' => false,
        'external_id' => 'edition_85'
      )
    }

    describe "#extra" do
      it "defaults to nil" do
        plan.extra.should be_nil
      end
    end

    describe '#to_hash' do
      it 'has the right keys' do
        hash = subject.to_hash
        hash.keys.should match_array %w(id description free external_id extra)
      end

      it 'has the right content' do
        plan.to_hash.should eq({
          'id' => 1,
          'description' => 'an awesome plan',
          'free' => false,
          'external_id' => 'edition_85',
          'extra' => nil
        })
      end

      context 'key "extra"' do
        let(:hash) { plan.to_hash.fetch('extra') }

        it "has nil extra be default" do
          hash.should be_nil
        end
      end
    end

    describe "fetching extra information from public service description" do
      let(:extra_information) do
        {
          'pricing' => {
            'editions' => [{
              'id' =>  85,
              'plans' =>  [
                {
                  'id' =>  179,
                  'frequency' =>  "MONTHLY",
                  'costs' =>  [
                    {
                      'amounts' =>  [
                        {
                          'currency' =>  "AUD",
                          'value' =>  30,
                        },
                        {
                          'currency' =>  "USD",
                          'value' =>  20,
                        }
                      ]
                    }
                  ],
                }
              ],
                'bullets' =>  ['two', 'bullets'],
            }]
          }
        }
      end

      let(:edition_attr_list_without_usd_pricing) do
        {
          'pricing' => {
            'editions' => [{
              'id' =>  85,
              'plans' =>  [
                {
                  'id' =>  179,
                  'frequency' =>  "MONTHLY",
                  'costs' =>  [
                    {
                      'amounts' =>  [
                        {
                          'currency' =>  "AUD",
                          'value' =>  30,
                        }
                      ]
                    }
                  ],
                }
              ],
                'bullets' =>  ['two', 'bullets'],
            }]
          }
        }
      end

      it "correctly selects the one with matching id" do
        edition_attrs = subject.edition_attrs(extra_information.fetch('pricing').fetch('editions'))
        edition_attrs.fetch('id').should == 85
      end

      context "assigning extra information" do
        let(:extra) { subject.to_hash.fetch('extra') }
        before do
          subject.assign_extra_information(extra_information)
        end

        it 'extra has the correct keys' do
          extra.keys.should =~ %w(cost bullets)
        end

        it "fetches the cost in USD from edition attributes" do
          extra.fetch('cost').should == 20.00
        end

        it "raises when no amount in USD exists" do
          expect {
            subject.assign_extra_information(edition_attr_list_without_usd_pricing)
          }.to raise_error /A USD pricing is required/
        end

        it "uses the the bullets" do
          extra.fetch('bullets').should == ['two', 'bullets']
        end
      end
    end
  end
end

