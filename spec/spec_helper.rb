require "rspec"
require "yaml"
require "yajl"
require "httpclient"
require "json"
require "active_support/core_ext"

SPEC_ROOT = File.expand_path(File.dirname(__FILE__))
ASSETS_DIR = File.join(SPEC_ROOT, "assets")
BOSH_ROOT_DIR = File.expand_path File.join(SPEC_ROOT, "..")
BOSH_TMP_DIR = File.expand_path File.join(BOSH_ROOT_DIR, "tmp")

def require_dir(dir_pattern)
  Dir.glob(File.expand_path(dir_pattern, File.dirname(__FILE__))) do |filename|
    require filename
  end
end

require_dir '../vendor/integration-test-support/support/**/*.rb'

tmp_dir = File.expand_path('tmp', File.dirname(__FILE__))
FileUtils.mkdir_p(tmp_dir)
IntegrationExampleGroup.tmp_dir = tmp_dir

RSpec.configure do |c|
  c.include IntegrationExampleGroup, :type => :integration, :example_group => {:file_path => /\/integration\//}
end
