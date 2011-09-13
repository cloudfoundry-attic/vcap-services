require 'digest/sha1'
require 'fileutils'
require 'tempfile'
require 'tmpdir'

# self contained
$:.unshift File.expand_path('../../vendor/gems/json/lib', __FILE__)
$:.unshift File.expand_path('../../vendor/gems/httpclient/lib', __FILE__)
$:.unshift File.expand_path('../../vendor/gems/rubyzip2/lib', __FILE__)

require 'json/pure'
require 'httpclient'
require 'zip/zipfilesystem'

# This class captures the common interactions with AppCloud that can be shared by different
# clients. Clients that use this class are the VMC CLI and the integration test automation.
# TBD - ABS: This is currently a minimal extraction of methods to tease out
# the interactive aspects of the VMC CLI from the AppCloud API calls.
# Update service related API to new version.

module VMC; end

class VMC::BaseClient

  def register_internal(base_uri, email, password, auth_hdr = {})
    response = HTTPClient.post("#{base_uri}/users", {:email => email, :password => password}.to_json, auth_hdr)
    raise(JSON.parse(response.content)['description'] || 'registration failed') if response.status != 200 && response.status != 204
  end

  def login_internal(base_uri, email, password)
    response = HTTPClient.post "#{base_uri}/users/#{email}/tokens", {:password => password}.to_json
    raise "login failed" if response.status != 200
    token = JSON.parse(response.content)['token']
  end

  def change_passwd_internal(base_uri, user_info, auth_hdr)
    email = user_info['email']
    response = HTTPClient.put("#{base_uri}/users/#{email}", user_info.to_json, auth_hdr)
    raise(JSON.parse(response.content)['description'] || 'password change failed') if response.status != 200 && response.status != 204
  end

  def get_user_internal(base_uri, email, auth_hdr)
    response = HTTPClient.get "#{base_uri}/users/#{email}", nil, auth_hdr
  end

  def delete_user_internal(base_uri, email, auth_hdr)
    response = HTTPClient.delete "#{base_uri}/users/#{email}", auth_hdr
  end

  def get_apps_internal(droplets_uri, auth_hdr)
    response = HTTPClient.get droplets_uri, nil, auth_hdr
    raise "(#{response.status}) can not contact server" if (response.status > 500 || response.status == 404)
    raise "Access Denied, please login or register" if response.status == 403
    droplets_full = JSON.parse(response.content)
  rescue => e
    error "Problem executing list command, #{e}"
  end

  def create_app_internal(droplets_uri, app_manifest, auth_hdr)
    response = HTTPClient.post droplets_uri, app_manifest.to_json, auth_hdr
    # Auto redirection
    if response.status == 302
      location = response.header["Location"][0]
      res = HTTPClient.get location, nil, auth_hdr
      return res
    else
      return nil
    end
  end

  def get_app_internal(droplets_uri, appname, auth_hdr)
    response = HTTPClient.get "#{droplets_uri}/#{appname}", nil, auth_hdr
  end

  def delete_app_internal(droplets_uri, appname, services_to_delete, auth_hdr)
    HTTPClient.delete "#{droplets_uri}/#{appname}", auth_hdr
    services_to_delete.each { |service_name|
      HTTPClient.delete "#{services_uri}/#{service_name}", auth_hdr
    }
  end

  def upload_app_bits(resources_uri, droplets_uri, appname, auth_hdr, opt_war_file, provisioned_db = false)
#    puts "[#{resources_uri}]:[#{droplets_uri}]:[#{appname}]:[#{auth_hdr}]:[#{opt_war_file}]:[#{provisioned_db}]"
    explode_dir = "#{Dir.tmpdir}/.vcap_#{appname}_files"
    FileUtils.rm_rf(explode_dir) # Make sure we didn't have anything left over..

    # Stage the app appropriately and do the appropriate fingerprinting, etc.
    if opt_war_file
      Zip::ZipFile.foreach(opt_war_file) { |zentry|
        epath = "#{explode_dir}/#{zentry}"
        FileUtils.mkdir_p(File.dirname(epath)) unless (File.exists?(File.dirname(epath)))
        zentry.extract("#{explode_dir}/#{zentry}")
      }
    else
      FileUtils.cp_r('.', explode_dir)
    end

    # Send the resource list to the cloud controller, the response will tell us what it already has..
    fingerprints = []
    resource_files = Dir.glob("#{explode_dir}/**/*", File::FNM_DOTMATCH)
    resource_files.each { |filename|
      fingerprints << { :size => File.size(filename),
                        :sha1 => Digest::SHA1.file(filename).hexdigest,
                        :fn => filename # TODO(dlc) probably should not send over the wire
      } unless (File.directory?(filename) || !File.exists?(filename))
    }

    # Send resource fingerprints to the cloud controller
    response = HTTPClient.post resources_uri, fingerprints.to_json, auth_hdr
    appcloud_resources = nil
    if response.status == 200
      appcloud_resources = JSON.parse(response.content)
      # we will use the exploded version of the files here to whip through and delete what we
      # will have appcloud fill in for us.
      appcloud_resources.each { |resource| FileUtils.rm_f resource['fn'] }
    end

    # Perform Packing of the upload bits here.
    upload_file = "#{Dir.tmpdir}/#{appname}.zip"
    FileUtils.rm_f(upload_file)
    exclude = ['..', '*~', '#*#', '*.log']
    exclude << '*.sqlite3' if provisioned_db
    Zip::ZipFile::open(upload_file, true) { |zf|
      Dir.glob("#{explode_dir}/**/*", File::FNM_DOTMATCH).each { |f|
        process = true
        exclude.each { |e| process = false if File.fnmatch(e, File.basename(f)) }
        zf.add(f.sub("#{explode_dir}/",''), f) if (process && File.exists?(f))
      }
    }

    upload_size = File.size(upload_file);

    upload_data = {:application => File.new(upload_file, 'rb'), :_method => 'put'}
    if appcloud_resources
      # Need to adjust filenames sans the explode_dir prefix
      appcloud_resources.each { |ar| ar['fn'].sub!("#{explode_dir}/", '') }
      upload_data[:resources] = appcloud_resources.to_json
    end

    response = HTTPClient.post "#{droplets_uri}/#{appname}/application", upload_data, auth_hdr
    raise "Problem uploading application bits" if response.status != 200
    upload_size

  ensure
    # Cleanup if we created an exploded directory.
    FileUtils.rm_f(upload_file)
    FileUtils.rm_rf(explode_dir)
  end

  def update_app_state_internal droplets_uri, appname, appinfo, auth_hdr
     hdrs = auth_hdr.merge({'content-type' => 'application/json'})
     response = HTTPClient.put "#{droplets_uri}/#{appname}", appinfo.to_json, hdrs
  end

  def get_app_instances_internal(droplets_uri, appname, auth_hdr)
    response = HTTPClient.get "#{droplets_uri}/#{appname}/instances", nil, auth_hdr
    instances_info = JSON.parse(response.content)
  end

  def get_app_files_internal(droplets_uri, appname, instance, path, auth_hdr)
    cc_url = "#{droplets_uri}/#{appname}/instances/#{instance}/files/#{path}"
    cc_url.gsub!('files//', 'files/')
    response = HTTPClient.get cc_url, nil, auth_hdr
  end

  def get_app_crashes_internal(droplets_uri, appname, auth_hdr)
    response = HTTPClient.get "#{droplets_uri}/#{appname}/crashes", nil, auth_hdr
  end

  def get_app_stats_internal(droplets_uri, appname, auth_hdr)
    response = HTTPClient.get "#{droplets_uri}/#{appname}/stats", nil, auth_hdr
  end

  def update_app_internal(droplets_uri, appname, auth_hdr)
    response = HTTPClient.put "#{droplets_uri}/#{appname}/update", '', auth_hdr
  end

  def get_update_app_status(droplets_uri, appname, auth_hdr)
    response = HTTPClient.get "#{droplets_uri}/#{appname}/update", nil, auth_hdr
    raise "Problem updating application" if response.status != 200
    response
  end

  def provision_service_internal(config_uri, request, auth_hdr)
    response = HTTPClient.post config_uri, request.to_json, auth_hdr
  end

  def bind_service_internal(binding_uri, request, auth_hdr)
    response = HTTPClient.post binding_uri, request.to_json, auth_hdr
  end

  def unprovision_service_internal(config_uri, service_id, auth_hdr)
    uri = config_uri + '/' + service_id
    response = HTTPClient.delete uri, auth_hdr
  end

  def add_service_internal(services_uri, service_manifest, auth_hdr)
    response = HTTPClient.post services_uri, service_manifest.to_json, auth_hdr
  end

  def remove_service_internal(services_uri, service_id, auth_hdr)
    HTTPClient.delete "#{services_uri}/#{service_id}", auth_hdr
  end

  def delete_services_internal(services_uri, services, auth_hdr)
    services.each do |service_name|
      HTTPClient.delete "#{services_uri}/#{service_name}", auth_hdr
    end
  end

  def error(msg)
    STDERR.puts(msg)
  end
end


