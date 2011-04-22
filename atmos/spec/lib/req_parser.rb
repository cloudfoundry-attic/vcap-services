$:.unshift File.join(File.dirname(__FILE__), '.')

require "parser"
require "res_parser"
require "base64"

module Net
	module HTTPHeader
		def capitalize(name)
			name
		end
	end
end

class GetoptLong
      def set_error(type, message)
          puts message
          exit 1
      end
end

module ArgsParser
	def ArgsParser.parseCli()
		arguments = {}

		CommonConstants::OPTIONS.each { |opt, arg|
			option = CommonParser.determineOptionString(opt)
			if option == nil
				return nil
			end

			arguments[option] = processArgument(arguments, \
							    option, arg)
			if arguments[option] == nil
				return nil
			end
		}

		if arguments[CommonConstants::OPERATION] == nil
			return nil
		elsif arguments[CommonConstants::VERSION] == 1
			CommonParser.printVersion()
			return nil
		end

		if arguments[CommonConstants::IP_ADDRESS] == nil
			arguments[CommonConstants::IP_ADDRESS] = "127.0.0.1"
		end
		
		return arguments
	end

	def ArgsParser.createRestMessage(arguments, url)
		operation = arguments[CommonConstants::OPERATION]

		case operation
			when CommonConstants::CREATE_OBJ_STR
				makeCreateObjRest(arguments, operation, url)
			when CommonConstants::READ_OBJ_STR
				makeReadObjRest(arguments, operation, url)
			when CommonConstants::UPDATE_OBJ_STR
				makeUpdateObjRest(arguments, operation, url)
			when CommonConstants::DELETE_OBJ_STR
				makeDeleteObjRest(arguments, operation, url)
			else
				return nil
		end
	end

	def ArgsParser.populateInitRequest(arguments, operation, url)
		parsedUri = URI.parse(url)

		case operation
			when CommonConstants::CREATE_OBJ_STR, \
			     CommonConstants::VERSION_OBJ_STR, \
			     CommonConstants::SET_USER_MD_STR, \
			     CommonConstants::SET_ACL_STR, \
			     CommonConstants::CR_HRDLNK_STR, \
                             CommonConstants::RENAME_OBJ_STR
				if parsedUri.query != nil
					request = Net::HTTP::Post.new(parsedUri.path+"?"+parsedUri.query)
				else request = Net::HTTP::Post.new(parsedUri.path)
				end
			when CommonConstants::READ_OBJ_STR, \
			     CommonConstants::LIST_VER_STR, \
			     CommonConstants::GET_USER_MD_STR, \
			     CommonConstants::GET_ACL_STR, \
			     CommonConstants::GET_SYS_MD_STR, \
			     CommonConstants::LIST_USER_MD_TAG_STR, \
			     CommonConstants::LIST_OBJ_STR, \
			     CommonConstants::QUERY_OBJ_STR, \
			     CommonConstants::GET_LIST_TAG_STR, \
			     CommonConstants::GET_SERVICEINFO_STR, \
			     CommonConstants::GET_OBJECT_INFO_STR
				if parsedUri.query != nil
					request = Net::HTTP::Get.new(parsedUri.path+"?"+parsedUri.query)
				else request = Net::HTTP::Get.new(parsedUri.path)
				end
			when CommonConstants::UPDATE_OBJ_STR, \
			     CommonConstants::RESTORE_VER_STR
				if parsedUri.query != nil
					request = Net::HTTP::Put.new(parsedUri.path+"?"+parsedUri.query)
				else request = Net::HTTP::Put.new(parsedUri.path)
				end
			when CommonConstants::DELETE_OBJ_STR, \
			     CommonConstants::DELETE_VER_STR, \
			     CommonConstants::DELETE_USER_MD_STR
				if parsedUri.query != nil
					request = Net::HTTP::Delete.new(parsedUri.path+"?"+parsedUri.query)
				else request = Net::HTTP::Delete.new(parsedUri.path)
				end
			when CommonConstants::TRUNCATE_OBJ_STR
				request = Net::HTTP::Put.new(parsedUri.path)
		        when CommonConstants::SHAREABLE_URL_STR
				if parsedUri.query != nil
					request = Net::HTTP::Get.new(parsedUri.path+"?"+parsedUri.query)
				else request = Net::HTTP::Get.new(parsedUri.path)
				end
				return request
			else
				puts "Incorrect operation was specified. " + \
					"Please try again."
				return nil
		end

		request = fillCredentials(request, arguments)
		return request
	end

	def ArgsParser.fillCredentials(request, arguments)
		if arguments[CommonConstants::UID] != nil
			request[CommonConstants::UID_HEADER] = \
				arguments[CommonConstants::UID]
		end
		currTime = CommonParser.createTimestamp()
		request[CommonConstants::DATE_HEADER] = currTime
		request[CommonConstants::VMW_DATE_HEADER] = currTime
		request[CommonConstants::CONT_TYPE_HEADER] = \
			CommonConstants::OBJECT_TYPE

		return request
	end

	def ArgsParser.makeCreateObjRest(arguments, operation, url)
		req = populateInitRequest(arguments, operation, url)

		if req == nil
			return nil
		end

		if arguments[CommonConstants::USER_ACL] != nil
			req[CommonConstants::USER_ACL_HEADER] = \
				arguments[CommonConstants::USER_ACL]
		end

		if arguments[CommonConstants::GROUP_ACL] != nil
			req[CommonConstants::GROUP_ACL_HEADER] = \
				arguments[CommonConstants::GROUP_ACL]
		end

		if arguments[CommonConstants::METADATA] != nil
                   	if arguments[CommonConstants::WSVERSION] != nil
				req[CommonConstants::USER_META_HEADER] = arguments[\
					CommonConstants::METADATA]
			else
				req[CommonConstants::META_HEADER] = arguments[\
					CommonConstants::METADATA]
			end
		end

		if arguments[CommonConstants::LISTABLE_METADATA] != nil
			req[CommonConstants::LIST_META_HEADER] = arguments[\
				CommonConstants::LISTABLE_METADATA]
		end

		if arguments[CommonConstants::OBJECT_URI] != nil
			req.body = CommonParser.readInEntireFile(\
				arguments[CommonConstants::OBJECT_URI])
			req[CommonConstants::CONT_LEN_HEADER] = \
				CommonParser.getFileSize(\
				arguments[CommonConstants::OBJECT_URI])
			req[CommonConstants::CONT_MD5_HEADER] = \
				Base64.encode64(\
				Digest::MD5.digest(req.body)).chomp()
		end

		if arguments[CommonConstants::CONTENT_TYPE] != nil
			req[CommonConstants::CONT_TYPE_HEADER] = \
				arguments[CommonConstants::CONTENT_TYPE]
		end
          
		if arguments[CommonConstants::WSVERSION] != nil
			req[CommonConstants::WSVERSION_HEADER] = \
				arguments[CommonConstants::WSVERSION]
		end

		if arguments[CommonConstants::WSCHKSUMALGO] != nil
			if arguments[CommonConstants::WSCHKSUMOFFSET] != nil
				if arguments[CommonConstants::WSCHKSUMVALUE] != nil
					req[CommonConstants::WSCHECKSUM_HEADER] = \
						CommonParser.composeChecksumHeader(\
						arguments[CommonConstants::WSCHKSUMALGO],\
						arguments[CommonConstants::WSCHKSUMOFFSET],\
						arguments[CommonConstants::WSCHKSUMVALUE])
				end
			end
		end

		return req
	end

	def ArgsParser.makeReadObjRest(arguments, operation, url)
		req = populateInitRequest(arguments, operation, url)

		if req == nil
			return nil
		end

		extentlist = arguments[CommonConstants::EXTENT]
		if !extentlist.nil?
			bytestring = CommonConstants::BYTES_VALUE
			extentlist.each {|extent|
			    extentstr = ""

		    	    offset = extent[CommonConstants::EXTENT_OFFSET]
		    	    length = extent[CommonConstants::EXTENT_LENGTH]

			    if !offset.nil? and length.nil?
			    	extentstr = offset.to_s() + "-"
			    elsif offset.nil? and !length.nil?
			    	extentstr = "-" + length.to_s()
			    else
			    	extentstr = offset.to_s() + "-" + (offset + length - 1).to_s()
			    end
			    bytestring = bytestring + extentstr + ","
			}

			bytestring.chop!
			req[CommonConstants::EXTENT_HEADER] = \
			    bytestring
		end

		if arguments[CommonConstants::LIMIT] != nil
			req[CommonConstants::LIMIT_HEADER] = \
				arguments[CommonConstants::LIMIT]
		end

		if arguments[CommonConstants::TOKEN] != nil
			req[CommonConstants::TOKEN_HEADER] = \
				arguments[CommonConstants::TOKEN]
		end

		if arguments[CommonConstants::WSVERSION] != nil
			req[CommonConstants::WSVERSION_HEADER] = \
				arguments[CommonConstants::WSVERSION]
		end

		if arguments[CommonConstants::INCL_MD] != nil
                        req[CommonConstants::INCL_MD_HEADER] = "true"
		end

		if arguments[CommonConstants::SMD_TAG] != nil
			req[CommonConstants::SYSTEM_TAG_HEADER] = \
				arguments[CommonConstants::SMD_TAG].join(",")
		end

		if arguments[CommonConstants::UMD_TAG] != nil
			req[CommonConstants::USER_TAG_HEADER] = \
				arguments[CommonConstants::UMD_TAG].join(",")
		end

		return req
	end

	def ArgsParser.makeUpdateObjRest(arguments, operation, url)
		req = populateInitRequest(arguments, operation, url)

		if req == nil
			return nil
		end

		if arguments[CommonConstants::USER_ACL] != nil
			req[CommonConstants::USER_ACL_HEADER] = \
				arguments[CommonConstants::USER_ACL]
		end

		if arguments[CommonConstants::GROUP_ACL] != nil
			req[CommonConstants::GROUP_ACL_HEADER] = \
				arguments[CommonConstants::GROUP_ACL]
		end

		if arguments[CommonConstants::METADATA] != nil
                   	if arguments[CommonConstants::WSVERSION] != nil
				req[CommonConstants::USER_META_HEADER] = arguments[\
					CommonConstants::METADATA]
			else
				req[CommonConstants::META_HEADER] = arguments[\
					CommonConstants::METADATA]
			end
		end

		if arguments[CommonConstants::LISTABLE_METADATA] != nil
			req[CommonConstants::LIST_META_HEADER] = arguments[\
				CommonConstants::LISTABLE_METADATA]
		end

		if arguments[CommonConstants::EXTENT] != nil
			extent = arguments[CommonConstants::EXTENT][0]

			offset = extent[CommonConstants::EXTENT_OFFSET]
			length = extent[CommonConstants::EXTENT_LENGTH]

			length = 0 if length.nil?

			lastBitTemp = (offset + length).to_s()
			if lastBitTemp.to_i().to_s() == lastBitTemp
				lastBit = (lastBitTemp.to_i() - 1).to_s()
			else
				lastBit = lastBitTemp
			end
			req[CommonConstants::EXTENT_HEADER] = \
				CommonConstants::BYTES_VALUE + \
				offset.to_s() + \
				"-" + lastBit
		end

		if arguments[CommonConstants::OBJECT_URI] != nil
			if arguments[CommonConstants::USER_EXTENT] == nil
				extent = {CommonConstants::EXTENT_OFFSET => 0,
					CommonConstants::EXTENT_LENGTH => \
					CommonParser.getFileSize(arguments\
					[CommonConstants::OBJECT_URI])}
				arguments[CommonConstants::USER_EXTENT] = Array.new
				arguments[CommonConstants::USER_EXTENT].push(extent)
			end

			extent = arguments[CommonConstants::USER_EXTENT][0]

			req.body = CommonParser.readInFile(\
				arguments[CommonConstants::OBJECT_URI], \
				extent[CommonConstants::EXTENT_OFFSET], \
				extent[CommonConstants::EXTENT_LENGTH])
			req[CommonConstants::CONT_LEN_HEADER] = \
				CommonParser.getPartialFileSize(\
				arguments[CommonConstants::OBJECT_URI], \
				extent[CommonConstants::EXTENT_OFFSET], \
				extent[CommonConstants::EXTENT_LENGTH])
			req[CommonConstants::CONT_MD5_HEADER] = \
				Base64.encode64(\
				Digest::MD5.digest(req.body)).chomp()
		end

		if arguments[CommonConstants::CONTENT_TYPE] != nil
			req[CommonConstants::CONT_TYPE_HEADER] = \
				arguments[CommonConstants::CONTENT_TYPE]
		end

		if arguments[CommonConstants::WSVERSION] != nil
			req[CommonConstants::WSVERSION_HEADER] = \
				arguments[CommonConstants::WSVERSION]
		end

		if arguments[CommonConstants::WSCHKSUMALGO] != nil
			if arguments[CommonConstants::WSCHKSUMOFFSET] != nil
				if arguments[CommonConstants::WSCHKSUMVALUE] != nil
					req[CommonConstants::WSCHECKSUM_HEADER] = \
						CommonParser.composeChecksumHeader(\
						arguments[CommonConstants::WSCHKSUMALGO],\
						arguments[CommonConstants::WSCHKSUMOFFSET],\
						arguments[CommonConstants::WSCHKSUMVALUE])
				end
			end
		end

		return req
	end

	def ArgsParser.makeDeleteObjRest(arguments, operation, url)
		req = populateInitRequest(arguments, operation, url)

		if req == nil
			return nil
		end

		return req
	end

	def ArgsParser.checkArgumentDefined(arguments, option)
		if arguments[option] == nil
			return false
		end

		if arguments[CommonConstants::OPERATION] == \
		   CommonConstants::READ_OBJ_STR \
		   and option == CommonConstants::EXTENT
			return false
		end

		puts "Passed-in argument list has the #{option} option " + \
			"listed more than once."
		return true
	end

	def ArgsParser.processArgument(arguments, option, arg)
		if CommonConstants::NO_ARG_ARGS.include?(option) == false
			if checkArgumentDefined(arguments, option) == true
				return nil
			elsif arg == nil
				puts "The #{option} option requires an " + \
					"argument."
				return nil
			end
		end

		case option
			when CommonConstants::HELP, CommonConstants::VERSION, \
			     CommonConstants::VERBOSE, CommonConstants::INCL_MD, \
                             CommonConstants::FORCE, CommonConstants::GEN_SIG, \
                             CommonConstants::INCLUDE_LAYOUT
				return 1
			when CommonConstants::OPERATION
				return CommonParser.processOperation(arg)
			when CommonConstants::USER_ACL, \
			     CommonConstants::GROUP_ACL
				return CommonParser.processAcl(arg)
			when CommonConstants::EXTENT, \
			     CommonConstants::USER_EXTENT
				extent = CommonParser.\
					processExtent(arg, option, arguments[option])
				return extent
			when CommonConstants::OBJECT_ID, \
			     CommonConstants::VER_OBJECT_ID, \
			     CommonConstants::UID, \
			     CommonConstants::FILENAME, \
			     CommonConstants::OBJECT_URI, \
			     CommonConstants::XQUERY, \
			     CommonConstants::IP_ADDRESS, \
			     CommonConstants::READ_FILE, \
			     CommonConstants::HMAC_KEY, \
			     CommonConstants::LOG_FILE, \
			     CommonConstants::TIMES, \
			     CommonConstants::METADATA, \
			     CommonConstants::LISTABLE_METADATA, \
			     CommonConstants::CONTENT_TYPE, \
			     CommonConstants::LIMIT, \
			     CommonConstants::TOKEN, \
			     CommonConstants::TRUNC_SIZE, \
			     CommonConstants::LINK_NAME, \
                             CommonConstants::WSVERSION, \
                             CommonConstants::NEW_NAME, \
			     CommonConstants::EXPIRES, \
			     CommonConstants::WSCHKSUMALGO, \
			     CommonConstants::WSCHKSUMOFFSET, \
			     CommonConstants::WSCHKSUMVALUE
				return arg
			when CommonConstants::TAG, \
			     CommonConstants::SMD_TAG, \
			     CommonConstants::UMD_TAG
				return CommonParser.processTag(arg)
			else
				puts "#{option} is an invalid option."
				return nil
		end
	end
end
