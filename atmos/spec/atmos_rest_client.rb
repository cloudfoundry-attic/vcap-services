require 'net/http'
require 'net/https'
require 'uri'
require 'time'
require 'openssl'
require 'base64'

# A very simple Atmos client which focus on basic file operations
# using namespace method.
class AtmosClient
  include Digest
  include OpenSSL

  REQUIRED_OPTS = %w(url sid uid key)
  HEADERS = {
    :date => 'date',
    :emc_date => 'x-emc-date',
    :sign => 'x-emc-signature',
    :uid => 'x-emc-uid',
    :type => 'content-type',
    :extent => 'Extent',
  }
  def initialize(opts)
    @opts = opts
    # http server
    url = URI.parse(@opts[:url])
    @http = Net::HTTP.new(url.host, url.port)
    @http.use_ssl = true if (url.port == 443 || url.port == 10080)
    @http.verify_mode = OpenSSL::SSL::VERIFY_NONE
  end

  def getObjNS(path='')
    path = '/rest/namespace/'+ path
    req = Net::HTTP::Get.new(path)
    sendRequest(req)
  end

  def getObj(id)
    return unless id
    req = Net::HTTP::Get.new(id)
    sendRequest(req)
  end

  def createObjNS(path='', content=nil)
    path = '/rest/namespace/'+ path
    req = Net::HTTP::Post.new(path)
    req.body = content
    sendRequest(req)
  end

  def createObj(content=nil)
    req = Net::HTTP::Post.new('/rest/objects')
    req.body = content
    sendRequest(req)
  end

  def deleteObjNS(path='')
    path = '/rest/namespace/'+ path
    req = Net::HTTP::Delete.new(path)
    sendRequest(req)
  end

  def deleteObj(id)
    return unless id
    req = Net::HTTP::Delete.new(id)
    sendRequest(req)
  end

protected

  def sendRequest(req)
    prepareRequest(req)
    res = @http.start do |http|
      http.request(req)
    end
    return res
  end

  def prepareRequest(request)
    t = Time.now.httpdate
    request[HEADERS[:emc_date]] = t
    request[HEADERS[:date]] = t
    request[HEADERS[:type]]= 'application/octet-stream'
    request[HEADERS[:uid]]= "#{@opts[:sid]}/#{@opts[:uid]}"
    request[HEADERS[:sign]] = genAuthHeader(request)
  end

  def genAuthHeader(request)
    hashString = "#{request.method}\n"+
      "#{request[HEADERS[:type]]}\n" +
      "#{request[HEADERS[:extent]]}\n"+
      "#{request[HEADERS[:date]]}\n"+
      "#{request.path.downcase}\n"

    customArgs = {}
    request.each_header{ |key, value|
      if key =~ /^x-emc-/
        customArgs[key] = value
      end
    }
    customArgs = customArgs.sort()
    customHeaders = ""
    customArgs.each{ |key, value|
      customHeaders += key + ":" + value.lstrip.rstrip + "\n"
    }

    customHeaders = customHeaders.chomp()
    hashString += customHeaders
    digest = HMAC.digest(OpenSSL::Digest.new('sha1'), Base64.decode64(@opts[:key]), hashString)
    Base64.encode64(digest.to_s).chomp
  end
end

