require 'xmlsimple'
require 'net/http'

class VCAP::Services::Atmos::Helper 

  def initialize(aux, logger)
    @logger = logger

    @host = aux[:atmos_host]
    @tenant = aux[:atmos_tenant]
    @tenantadmin = aux[:atmos_tenantadmin]
    @tenantpasswd = aux[:atmos_tenantpasswd]
    @port = aux[:atmos_port]
  end


  def createSubtenant( name )
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

    @logger.debug "create subtenant #{name}"
    @logger.debug uri
    @logger.debug req

    http = Net::HTTP.new(uri.host, @port.to_i)
    http.use_ssl = true
    http.verify_mode = OpenSSL::SSL::VERIFY_NONE

    res = http.request(req)

# Response Body:
#   <?xml version="1.0" encoding="UTF-8"?>
#   <subtenantId>2e705c1a08e5412fb7231055d56733e0</subtenantId>

    if res.code == "201"
      id = XmlSimple.xml_in(res.body, { 'KeyAttr' => 'subtenantId' })
      return id
    end
    return nil
  end

  def deleteSubtenant( name )
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

# Response Body:
#   <?xml version="1.0" encoding="UTF-8"?>
#   <deleted>true</deleted>

    return false unless res.code == "200"
    return true
  end

  def createUser(username, subtenantname)
    uri = URI.parse("http://#{@host}/sysmgmt/tenants/#{@tenant}/subtenants/#{subtenantname}/uids")

    headers = { 'x-atmos-tenantadmin'         =>  @tenantadmin,
                'x-atmos-tenantadminpassword' =>  @tenantpasswd,
                'x-atmos-authtype'            =>  'password',
                'x-atmos-uid'                 =>  username
              }

    req = Net::HTTP::Post.new(uri.request_uri)
    req["accept"] = '*/*'
    headers.keys.each {|f| req.add_field(f, headers[f])}

    @logger.debug "create user #{username} under subtenant #{subtenantname} in tenant #{@tenant}"
    @logger.debug uri

    http = Net::HTTP.new(uri.host, @port.to_i)
    http.use_ssl = true
    http.verify_mode = OpenSSL::SSL::VERIFY_NONE

    res = http.request(req)

# Response Body:
#   <?xml version="1.0" encoding="UTF-8"?>
#   <sharedSecret>jzpKHy5ee28jMpTiiqkmTa5vdu8=</sharedSecret>

    if res.code == "200"
      sharedSecret = XmlSimple.xml_in(res.body, { 'KeyAttr' => 'sharedSecret' })
      return sharedSecret
    end
    return nil

  end

  def deleteUser(username, subtenantname)
    uri = URI.parse("http://#{@host}/sysmgmt/tenants/#{@tenant}/subtenants/#{subtenantname}/uids/#{username}")

    headers = { 'x-atmos-tenantadmin'         =>  @tenantadmin,
                'x-atmos-tenantadminpassword' =>  @tenantpasswd,
                'x-atmos-authtype'            =>  'password'
              }

    req = Net::HTTP::Delete.new(uri.request_uri)
    req["accept"] = '*/*'
    headers.keys.each {|f| req.add_field(f, headers[f])}

    @logger.debug "delete user #{username} under subtenant #{subtenantname} in tenant #{@tenant}"
    @logger.debug uri

    http = Net::HTTP.new(uri.host, @port.to_i)
    http.use_ssl = true
    http.verify_mode = OpenSSL::SSL::VERIFY_NONE

    res = http.request(req)

# Response Body:
#   <?xml version="1.0" encoding="UTF-8"?>
#   <deleted>true</deleted>

    return false unless res.code == "200"
    return true
  end

end
