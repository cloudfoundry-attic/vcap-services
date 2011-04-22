#!/usr/bin/ruby
$:.unshift File.join(File.dirname(__FILE__), '.')

require 'net/https'
require 'uri'
require "req_parser"

begin

def getPem(uri, certpath, saved_host)
        soc = TCPSocket.new(saved_host, uri.port)
        ssl = SSL::SSLSocket.new(soc)
        ssl.connect
        ssl.write("GET /rest HTTP/1.0\n")
        ssl.write("User-Agent: Mozilla/5.0 (Windows; U; Windows NT 5.1; ja; rv:1.9.0.4) Gecko/2008102920 Firefox/3.0.4\n")
        ssl.write("Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8\n")
        ssl.write("Accept-Language: ja,en-us;q=0.7,en;q=0.3\n")
        ssl.write("Accept-Charset: Shift_JIS,utf-8;q=0.7,*;q=0.7\n")
        ssl.write("\r\n\r\n")
        pem = ssl.peer_cert_chain.to_s
        ssl.close
        soc.close
        fp = File.open(certpath, 'w+')
        fp.puts pem
        fp.close
        return fp.path
end

def prepareRequest(request, url, arguments)
	request[CommonConstants::DATE_HEADER] = nil
	request[CommonConstants::SIG_HEADER] = nil
	request[CommonConstants::DATE_HEADER] = \
		CommonParser.createTimestamp()
	signature = CommonParser.createSignature(\
		request, url, \
			arguments[\
			CommonConstants::HMAC_KEY], \
			arguments[\
			CommonConstants::GEN_SIG])
	if signature != nil
		request[CommonConstants::SIG_HEADER] \
			= signature
	end
end

	arguments = ArgsParser.parseCli()
	if arguments == nil
		exit 1
	end

        ip_parts = arguments[CommonConstants::IP_ADDRESS].split(":")
        saved_host = ip_parts[0]
        saved_port = ip_parts[1]
        dummy_ip = CommonConstants::URL_REPLACE_VAL
        dummy_ip += ":#{saved_port}" if !saved_port.nil?
        arguments[CommonConstants::IP_ADDRESS] = dummy_ip

	url = CommonParser.createServiceUrl(arguments)
	if url == nil
		exit 1
	end

	request = ArgsParser.createRestMessage(arguments, url)
	if request == nil
		exit 1
	end


	if arguments[CommonConstants::GEN_SIG] != nil
		puts "Generate Signature:\n"
		prepareRequest(request, url, arguments)
		puts "\n#{CommonConstants::SIG_HEADER}: #{request[CommonConstants::SIG_HEADER]}"

		exit 0
	end
	if arguments[CommonConstants::OPERATION].eql?(CommonConstants::SHAREABLE_URL_STR)
		url.sub!(CommonConstants::URL_REPLACE_VAL, saved_host)
		parsedUri = URI.parse(request.path)
		puts "#{CommonConstants::SHAREABLE_URL_STR}: #{url}?#{parsedUri.query}"

		exit 0
	end

	
	parsedUri = URI.parse(url)

	if arguments[CommonConstants::TIMES] != nil
		tries = arguments[CommonConstants::TIMES].to_i()
		timing = true
	else
		tries = 1
		timing = false
	end

	timesArray = []
	$errorCase = false

	http = Net::HTTP.new(saved_host, parsedUri.port)
 	if parsedUri.port == CommonConstants::CMSSL_PORT or \
           parsedUri.port == CommonConstants::WSSSL_PORT
		http.use_ssl = true

		http.verify_mode = OpenSSL::SSL::VERIFY_NONE

		certfile = saved_host.to_s() + "." + parsedUri.port.to_s() + ".crt"
		certpath = CommonConstants::HTTPDCERT_PATH + certfile
             
		http.ca_file = File.join(File.dirname(certpath), certfile)
	end
	result = http.start() \
		{ |http|
			tries.times {
				prepareRequest(request, url, arguments)
				if timing == true
					beginTime = Time.now()
				end
				response = http.request(request)
				if timing == true
					endTime = Time.now()
				end
				success = ResponseParser.parseResponse(\
						response, \
						arguments[\
						CommonConstants::OPERATION], \
						arguments[\
						CommonConstants::READ_FILE])
				if success == 1
					$errorCase = true
					next
				end
				if timing == true
					diffTime = endTime - beginTime
					timesArray.push(diffTime)
				end
			}
		}

	if timing == true
		timeFile = nil
		if arguments[CommonConstants::OPERATION] == \
		   CommonConstants::CREATE_OBJ_STR
			timeFile = File.new(\
					arguments[CommonConstants::OBJECT_URI]\
					+ CommonConstants::CREATE_LOG, \
					CommonConstants::APPEND_LET)
		elsif arguments[CommonConstants::OPERATION] \
		      == CommonConstants::READ_OBJ_STR
			if arguments[CommonConstants::READ_FILE] == nil
				timeFile = File.new(arguments[\
						CommonConstants::OBJECT_ID] \
						+ CommonConstants::READ_LOG, \
						CommonConstants::APPEND_LET)
			else
				timeFile = File.new(arguments[\
						CommonConstants::READ_FILE] \
						+ CommonConstants::READ_LOG, \
						CommonConstants::APPEND_LET)
			end
		end
		if timeFile != nil
			timesArray.each { |timed|
				timeFile.puts "#{timed}"
			}
			timeFile.close()
		end
	end

	if $errorCase == true
		exit 1
	end
end

exit 0
