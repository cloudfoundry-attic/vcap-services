# Copyright (c) 2009-2011 VMware, Inc.
require 'xmlsimple'
require 'net/http'
require 'atmos_error'

class VCAP::Services::Atmos::Helper

  include VCAP::Services::Atmos

  def initialize(atmos_config, logger)
    @logger = logger

    @host = atmos_config[:host]
    @tenant = atmos_config[:tenant]
    @tenantadmin = atmos_config[:tenantadmin]
    @tenantpasswd = atmos_config[:tenantpasswd]
    @port = atmos_config[:port]
  end


  def create_subtenant(name)
    uri = URI.parse("http://#{@host}/sysmgmt/tenants/#{@tenant}/subtenants")

    headers = { 'x-atmos-authsource'          =>  'local',
                'x-atmos-tenantadmin'         =>  @tenantadmin,
                'x-atmos-tenantadminpassword' =>  @tenantpasswd,
                'x-atmos-authtype'            =>  'password',
                'x-atmos-subtenantname'       =>  name
              }
    req = Net::HTTP::Post.new(uri.request_uri)

    req["accept"] = '*/*'
    headers.keys.each {|f| req.add_field(f, headers[f])}

    @logger.debug "create subtenant #{name} uri: #{uri.inspect} req: #{req.inspect} hdrs: #{headers.inspect}"

    http = Net::HTTP.new(uri.host, @port.to_i)
    http.use_ssl = true
    http.verify_mode = OpenSSL::SSL::VERIFY_NONE

    res = http.request(req)

    # Response Body is like:
    # <?xml version="1.0" encoding="UTF-8"?>
    # <subtenantId>2e705c1a08e5412fb7231055d56733e0</subtenantId>

    if res.code == "201"
      id = XmlSimple.xml_in(res.body, { 'KeyAttr' => 'subtenantId' })
      return id
    end
    raise AtmosError.new(AtmosError::ATMOS_BACKEND_ERROR_CREATE_SUBTENAT, res.code)
  end

  def delete_subtenant(name)
    uri = URI.parse("http://#{@host}/sysmgmt/tenants/#{@tenant}/subtenants/#{name}")

    headers = { 'x-atmos-tenantadmin'         =>  @tenantadmin,
                'x-atmos-tenantadminpassword' =>  @tenantpasswd,
                'x-atmos-authtype'            =>  'password'
              }
    req = Net::HTTP::Delete.new(uri.request_uri)

    req["accept"] = '*/*'
    headers.keys.each {|f| req.add_field(f, headers[f])}

    @logger.debug "delete subtenant #{name}"

    http = Net::HTTP.new(uri.host, @port.to_i)
    http.use_ssl = true
    http.verify_mode = OpenSSL::SSL::VERIFY_NONE

    res = http.request(req)

    # Response Body is like:
    # <?xml version="1.0" encoding="UTF-8"?>
    # <deleted>true</deleted>

    raise AtmosError.new(AtmosError::ATMOS_BACKEND_ERROR_DELETE_SUBTENAT, res.code) unless res.code == "200"
    true
  end

  def create_user(username, subtenant_name)
    uri = URI.parse("http://#{@host}/sysmgmt/tenants/#{@tenant}/subtenants/#{subtenant_name}/uids")

    headers = { 'x-atmos-tenantadmin'         =>  @tenantadmin,
                'x-atmos-tenantadminpassword' =>  @tenantpasswd,
                'x-atmos-authtype'            =>  'password',
                'x-atmos-uid'                 =>  username
              }

    req = Net::HTTP::Post.new(uri.request_uri)
    req["accept"] = '*/*'
    headers.keys.each {|f| req.add_field(f, headers[f])}

    @logger.debug "create user #{username} under subtenant #{subtenant_name} in tenant #{@tenant}"
    @logger.debug uri

    http = Net::HTTP.new(uri.host, @port.to_i)
    http.use_ssl = true
    http.verify_mode = OpenSSL::SSL::VERIFY_NONE

    res = http.request(req)

    # Response Body is like:
    # <?xml version="1.0" encoding="UTF-8"?>
    # <sharedSecret>jzpKHy5ee28jMpTiiqkmTa5vdu8=</sharedSecret>

    if res.code == "200"
      shared_secret = XmlSimple.xml_in(res.body, { 'KeyAttr' => 'sharedSecret' })
      return shared_secret
    end
    raise AtmosError.new(AtmosError::ATMOS_BACKEND_ERROR_CREATE_USER, res.code)
  end

  def delete_user(username, subtenant_name)
    uri = URI.parse("http://#{@host}/sysmgmt/tenants/#{@tenant}/subtenants/#{subtenant_name}/uids/#{username}")

    headers = { 'x-atmos-tenantadmin'         =>  @tenantadmin,
                'x-atmos-tenantadminpassword' =>  @tenantpasswd,
                'x-atmos-authtype'            =>  'password'
              }

    req = Net::HTTP::Delete.new(uri.request_uri)
    req["accept"] = '*/*'
    headers.keys.each {|f| req.add_field(f, headers[f])}

    @logger.debug "delete user #{username} under subtenant #{subtenant_name} in tenant #{@tenant}"
    @logger.debug uri

    http = Net::HTTP.new(uri.host, @port.to_i)
    http.use_ssl = true
    http.verify_mode = OpenSSL::SSL::VERIFY_NONE

    res = http.request(req)

    # Response Body:
    # <?xml version="1.0" encoding="UTF-8"?>
    # <deleted>true</deleted>

    raise AtmosError.new(AtmosError::ATMOS_BACKEND_ERROR_DELETE_USER, res.code) unless res.code == "200"
    true
  end
end
