# Copyright (c) 2009-2011 VMware, Inc.
require 'fileutils'
require 'uri'
require 'httpclient'

module MysqlServiceHelper

  def load_mysql_config
    mysql_node_file = File.expand_path("../../../../mysql/config/mysql_node.yml", __FILE__)
    mysql_gateway_file = File.expand_path("../../../../mysql/config/mysql_gateway.yml", __FILE__)
    mysql_backup_file = File.expand_path("../../../../mysql/config/mysql_backup.yml", __FILE__)
    begin
      @config[:mysql_config] = File.open(mysql_node_file) do |f|
        YAML.load(f)
      end
      @config[:mysql_config][:backup_config] = File.open(mysql_backup_file) do |f1|
        YAML.load(f1)
      end
      @config[:mysql_config][:gateway_config] = File.open(mysql_gateway_file) do |f2|
        YAML.load(f2)
      end
    rescue => e
      raise "Could not read mysql node configuration file:  #{e}"
    end
  end

  def mysql_backup(service)
    name = service["service_id"]
    name.should_not == nil
    folder = mysql_backup_folder(name)
    FileUtils.rm_rf(folder)
    File.exist?(folder).should == false
    mysql_backup_bin = File.expand_path("../../../../mysql/bin/mysql_backup", __FILE__)
    gem_file = File.expand_path("../../../../mysql/Gemfile", __FILE__)
    # FIXME Bunlder.with_clean_env seems to have bug: https://github.com/carlhuda/bundler/issues/1133 Use explicit BUNDKE_GEMFILE as workaround
    cmd = "export BUNDLE_GEMFILE=#{gem_file};#{mysql_backup_bin}"
    Bundler.with_clean_env do
      result = %x[#{cmd}]
      debug "backup result: #{result}"
    end
    File.exist?(folder).should == true
  end

  def mysql_backup_folder(name)
    nfs_path = @config[:mysql_config][:backup_config]["backup_path"]
    path_prefix = 'backups'
    full_path = File.join(nfs_path, path_prefix, "mysql", name[0,2], name[2,2], name[4,2], name)
  end

  def provision_mysql_service(token)
    name = "mysql-service-test"
    @service_alias = name
    prov_request = {
      :name => name,
      :label=> 'mysql-5.1',
      :plan=> 'free',
    }
    res = @client.provision_service_internal(@configuration_uri, prov_request, auth_json_hdr(token))
    debug "Provision response: #{res.content}"
    res.status.should == 200
    service = parse_json(res.content)
    service
  end

  def mysql_drop_service
    name = @service_detail['service_id']
    mysql_bin = @config[:mysql_config]["mysql_bin"]
    host, port, user, pass = %w(host port user pass).map{|k| @config[:mysql_config]["mysql"][k]}
    cmd ="echo 'drop database #{name}'| #{mysql_bin} -u#{user} -p#{pass} -h#{host} -P#{port}"
    result = %x[#{cmd}]
  end

  def mysql_drop_service_from_db
    name = @service_detail['service_id']
    sqlite_bin = 'sqlite3'
    file_uri = @config[:mysql_config]["local_db"]
    uri = URI.parse(file_uri)
    path = uri.path
    cmd = "echo 'delete from vcap_services_mysql_node_provisioned_services where name = \"#{name}\";'|#{sqlite_bin} #{path}"
    debug "drop local db cmd:#{cmd}"
    result = %x[#{cmd}]
    debug "drop local db cmd result:#{result}"
  end

  def mysql_recover
    name = @service_detail['service_id']
    ip_port = service_gateway_port("mysql")
    debug "ip_port of gw: #{ip_port}"
    recover_url = "http://#{ip_port}/service/internal/v1/recover"
    token = @config[:mysql_config][:gateway_config]["token"]
    hdr = {
      'X-VCAP-Service-Token' => token,
      'content-type' => "application/json",
    }
    # Find a sub directory in backup_folder
    path = mysql_backup_folder(name)
    subdir = nil
    Dir.foreach(path) do |sub|
      subdir = sub if not sub =~ /\./
    end
    subdir.should_not == nil
    req = {
      :instance_id => name,
      :backup_path => File.join(path, subdir)
    }
    debug ("url: #{recover_url}. Header: #{hdr.inspect}. Body: #{req.inspect}")
    res = HTTPClient.post(recover_url, req.to_json, hdr)
    debug "Return of recover: #{res.content}"
    res.status.should == 200
  end

end
