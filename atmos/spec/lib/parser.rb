$:.unshift File.join(File.dirname(__FILE__), '.')

require "consts"
require "time"
require "uri"
require "openssl"

include OpenSSL
include Digest

module CommonParser
	$debugging = 0

	def CommonParser.determineOptionString(option)
		case option
			when CommonConstants::LOG_OP_STR
				return CommonConstants::LOG_FILE
			when CommonConstants::IP_ADDR_OP_STR
				return CommonConstants::IP_ADDRESS
			when CommonConstants::UID_OP_STR
				return CommonConstants::UID
                        when CommonConstants::FILENAME_OP_STR
                                return CommonConstants::FILENAME
			when CommonConstants::OB_URI_OP_STR
				return CommonConstants::OBJECT_URI
			when CommonConstants::OB_ID_OP_STR
				return CommonConstants::OBJECT_ID
			when CommonConstants::RST_OB_ID_OP_STR
				return CommonConstants::VER_OBJECT_ID
			when CommonConstants::USR_ACL_OP_STR
				return CommonConstants::USER_ACL
			when CommonConstants::GRP_ACL_OP_STR
				return CommonConstants::GROUP_ACL
			when CommonConstants::OPER_OP_STR
				return CommonConstants::OPERATION
			when CommonConstants::META_OP_STR
				return CommonConstants::METADATA
			when CommonConstants::LIS_MD_OP_STR
				return CommonConstants::LISTABLE_METADATA
			when CommonConstants::TAG_OP_STR
				return CommonConstants::TAG
			when CommonConstants::QRY_OP_STR
				return CommonConstants::XQUERY
			when CommonConstants::EXT_OP_STR
				return CommonConstants::EXTENT
			when CommonConstants::US_EXT_OP_STR
				return CommonConstants::USER_EXTENT
			when CommonConstants::READ_OP_STR
				return CommonConstants::READ_FILE
			when CommonConstants::KEY_OP_STR
				return CommonConstants::HMAC_KEY
			when CommonConstants::HELP_OP_STR
				return CommonConstants::HELP
			when CommonConstants::VERS_OP_STR
				return CommonConstants::VERSION
			when CommonConstants::VERB_OP_STR
				return CommonConstants::VERBOSE
			when CommonConstants::TIME_OP_STR
				return CommonConstants::TIMES
			when CommonConstants::TYPE_OP_STR
				return CommonConstants::CONTENT_TYPE
			when CommonConstants::TRUNC_SIZE_OP_STR
				return CommonConstants::TRUNC_SIZE
			when CommonConstants::LIMIT_OP_STR
				return CommonConstants::LIMIT
			when CommonConstants::TOKEN_OP_STR
				return CommonConstants::TOKEN
			when CommonConstants::INCL_MD_OP_STR
			        return CommonConstants::INCL_MD
                        when CommonConstants::WSVERSION_OP_STR
				return CommonConstants::WSVERSION
			when CommonConstants::GET_SERVICEINFO_OP_STR
				return CommonConstants::GET_SERVICEINFO
			when CommonConstants::SMD_TAG_OP_STR
				return CommonConstants::SMD_TAG
			when CommonConstants::UMD_TAG_OP_STR
				return CommonConstants::UMD_TAG
			when CommonConstants::CR_HRDLNK_OP_STR
			return CommonConstants::LINK_NAME
			when CommonConstants::NAME_OP_STR
				return CommonConstants::NEW_NAME
			when CommonConstants::FORCE_OP_STR
				return CommonConstants::FORCE
		        when CommonConstants::GEN_SIG_OP_STR
				return CommonConstants::GEN_SIG
			when CommonConstants::EXPIRES_OP_STR
				return CommonConstants::EXPIRES
			when CommonConstants::WSCHKSUMALGO_OP_STR
				return CommonConstants::WSCHKSUMALGO
			when CommonConstants::WSCHKSUMOFFSET_OP_STR
				return CommonConstants::WSCHKSUMOFFSET
			when CommonConstants::WSCHKSUMVALUE_OP_STR
				return CommonConstants::WSCHKSUMVALUE		
                        when CommonConstants::INCLUDE_LAYOUT_OP_STR
                                return CommonConstants::INCLUDE_LAYOUT		
			else
				puts "#{opt} is not a valid option. " + \
					"Please try again."
				return nil
		end
	end

	def CommonParser.createServiceUrl(arguments)
		ipAddress = arguments[CommonConstants::IP_ADDRESS]
		operation = arguments[CommonConstants::OPERATION]
		objectId = arguments[CommonConstants::OBJECT_ID]
		fileName = arguments[CommonConstants::FILENAME]

		fileName = URI.escape(fileName) if !fileName.nil?


		pos = ipAddress.split(":")
		if pos[1].to_i == CommonConstants::CMSSL_PORT or \
           pos[1].to_i == CommonConstants::WSSSL_PORT
			proto_header = CommonConstants::HTTPS_PROTOCOL
		else
			proto_header = CommonConstants::HTTP_PROTOCOL
		end              
		beginStr = proto_header + ipAddress + CommonConstants::REST_STR 

		case operation
			when CommonConstants::CREATE_OBJ_STR, \
			     CommonConstants::LIST_OBJ_STR, \
			     CommonConstants::QUERY_OBJ_STR
				if fileName != nil
				        beginStr += CommonConstants::FILEPATH_STR
				        beginStr += "/" + fileName
                                else
                                        beginStr += CommonConstants::OBJECTS_STR
				end
				return beginStr
			when CommonConstants::READ_OBJ_STR

	                        if (objectId == nil and fileName == nil) or \
                	        	(objectId != nil and fileName != nil)
                        		     puts "Object ID or Filename is required for " + \
                                		  "the #{operation} " + \
	                        	           "operation. Please try again."
        	                	return nil
                		end
                                if objectId != nil
                                        beginStr += CommonConstants::OBJECTS_STR
                                        beginStr += "/" + objectId
                                else
                                        if fileName.length == 0
                                                beginStr += CommonConstants::FILEPATH_STR
                                        else
                                                beginStr += CommonConstants::FILEPATH_STR

                                                if !fileName[0, 1].eql?("/")
                                                        beginStr += "/"
                                                end

                                                beginStr += fileName
                                        end
                                end
                                return beginStr
                        when CommonConstants::UPDATE_OBJ_STR, \
			     CommonConstants::DELETE_OBJ_STR, \
			     CommonConstants::TRUNCATE_OBJ_STR, \
			     CommonConstants::CR_HRDLNK_STR, \
                             CommonConstants::RENAME_OBJ_STR
	                        if (objectId == nil and fileName == nil) or \
                	        	(objectId != nil and fileName != nil)
                        		     puts "Object ID or Filename is required for " + \
                                		  "the #{operation} " + \
	                        	           "operation. Please try again."
        	                	return nil
                		end
                                if objectId != nil
                                        beginStr += CommonConstants::OBJECTS_STR
                                        beginStr += "/" + objectId
                                else
                                        beginStr += CommonConstants::FILEPATH_STR
				        beginStr += "/" + fileName
                                end

                                if operation == CommonConstants::CR_HRDLNK_STR
                                        return beginStr + \
                                                CommonConstants::HARDLINK_STR
                                end

                                if operation == CommonConstants::RENAME_OBJ_STR
                                        return beginStr + \
                                                CommonConstants::RENAME_STR
                                else
                                        return beginStr
                                end
			when CommonConstants::VERSION_OBJ_STR
	                        if (objectId == nil and fileName == nil) or \
                	        	(objectId != nil and fileName != nil)
                        		     puts "Object ID or Filename is required for " + \
                                		  "the #{operation} " + \
	                        	           "operation. Please try again."
        	                	return nil
                		end
                                if objectId != nil
                                        beginStr += CommonConstants::OBJECTS_STR
                                        beginStr += "/" + objectId
                                else
                                        beginStr += CommonConstants::FILEPATH_STR
				        beginStr += "/" + fileName
                                end
                                return beginStr + CommonConstants::VERSIONS_STR
			when CommonConstants::SET_USER_MD_STR, \
			     CommonConstants::GET_USER_MD_STR, \
			     CommonConstants::DELETE_USER_MD_STR
                                if (objectId == nil and fileName == nil) or \
                                        (objectId != nil and fileName != nil)
                        		     puts "Object ID or Filename is required for " + \
						"the #{operation} " + \
						"operation. Please try again."
					return nil
				end
                                if objectId != nil
                                        beginStr += CommonConstants::OBJECTS_STR
                                        beginStr += "/" + objectId
                                else
                                        beginStr += CommonConstants::FILEPATH_STR
				        beginStr += "/" + fileName
                                end
				return beginStr + \
					CommonConstants::METADATA_STR + \
					CommonConstants::USER_STR
			when CommonConstants::SET_ACL_STR, \
			     CommonConstants::GET_ACL_STR
                                if (objectId == nil and fileName == nil) or \
                                        (objectId != nil and fileName != nil)
                                             puts "Object ID or Filename is required for " + \
						"the #{operation} " + \
						"operation. Please try again."
					return nil
				end
                                if objectId != nil
                                        beginStr += CommonConstants::OBJECTS_STR
                                        beginStr += "/" + objectId
                                else
                                        beginStr += CommonConstants::FILEPATH_STR
				        beginStr += "/" + fileName
                                end
				return beginStr + \
					CommonConstants::ACL_STR
			when CommonConstants::GET_OBJECT_INFO_STR
                                if (objectId == nil and fileName == nil) or \
                                        (objectId != nil and fileName != nil)
                                             puts "Object ID or Filename is required for " + \
						"the #{operation} " + \
						"operation. Please try again."
					return nil
				end
                                if objectId != nil
                                        beginStr += CommonConstants::OBJECTS_STR
                                        beginStr += "/" + objectId
                                else
                                        beginStr += CommonConstants::FILEPATH_STR
				        beginStr += "/" + fileName
                                end
				return beginStr + \
					CommonConstants::INFO_STR
			when CommonConstants::GET_SYS_MD_STR
                                if (objectId == nil and fileName == nil) or \
                                        (objectId != nil and fileName != nil)
                                             puts "Object ID or Filename is required for " + \
						"the #{operation} " + \
						"operation. Please try again."
					return nil
				end
                                if objectId != nil
                                        beginStr += CommonConstants::OBJECTS_STR
                                        beginStr += "/" + objectId
                                else
                                        beginStr += CommonConstants::FILEPATH_STR
				        beginStr += "/" + fileName
                                end
				return beginStr + \
					CommonConstants::METADATA_STR + \
					CommonConstants::SYSTEM_STR
			when CommonConstants::LIST_USER_MD_TAG_STR
                                if (objectId == nil and fileName == nil) or \
                                        (objectId != nil and fileName != nil)
                                             puts "Object ID or Filename is required for " + \
						"the #{operation} " + \
						"operation. Please try again."
					return nil
				end
                                if objectId != nil
                                        beginStr += CommonConstants::OBJECTS_STR
                                        beginStr += "/" + objectId
                                else
                                        beginStr += CommonConstants::FILEPATH_STR
				        beginStr += "/" + fileName
                                end
				return beginStr + \
					CommonConstants::METADATA_STR + \
					CommonConstants::TAGS_STR
			when CommonConstants::GET_LIST_TAG_STR
				beginStr += CommonConstants::OBJECTS_STR
				return beginStr + \
					CommonConstants::LIST_TAGS_STR
			when CommonConstants::GET_SERVICEINFO_STR
				return beginStr + CommonConstants::SERVICE_STR
			when CommonConstants::LIST_VER_STR
                                if (objectId == nil and fileName == nil) or \
                                        (objectId != nil and fileName != nil)
                                             puts "Object ID or Filename is required for " + \
						"the #{operation} " + \
						"operation. Please try again."
					return nil
				end
                                if objectId != nil
                                        beginStr += CommonConstants::OBJECTS_STR
                                        beginStr += "/" + objectId
                                else
                                        beginStr += CommonConstants::FILEPATH_STR
                                        beginStr += "/" + fileName
                                end
                                return beginStr + \
					CommonConstants::VERSIONS_STR

			when CommonConstants::DELETE_VER_STR
				if (objectId == nil and fileName == nil) or \
				   (objectId != nil and fileName != nil)
					puts "Object ID or Filename is required for " + \
					     "the #{operation} " + \
					     "operation. Please try again."
					return nil
				end
				if objectId != nil
					beginStr += CommonConstants::OBJECTS_STR
					beginStr += "/" + objectId
				else
					beginStr += CommonConstants::FILEPATH_STR
					beginStr += "/" + fileName
				end
				return beginStr + \
					CommonConstants::VERSIONS_STR

			when CommonConstants::RESTORE_VER_STR
				if (objectId == nil and fileName == nil) or \
				   (objectId != nil and fileName != nil)
					puts "Object ID or Filename is required for " + \
					     "the #{operation} " + \
					     "operation. Please try again."
					return nil
				end
				if objectId != nil
					beginStr += CommonConstants::OBJECTS_STR
					beginStr += "/" + objectId
				else
					beginStr += CommonConstants::FILEPATH_STR
					beginStr += "/" + fileName
				end
				return beginStr + \
					CommonConstants::VERSIONS_STR

			when CommonConstants::SHAREABLE_URL_STR
                                if (objectId == nil and fileName == nil) or \
                                        (objectId != nil and fileName != nil)
                                             puts "Object ID or Filename is required for " + \
						"the #{operation} " + \
						"operation. Please try again."
					return nil
				end
                                if objectId != nil
                                        beginStr += CommonConstants::OBJECTS_STR
                                        beginStr += "/" + objectId
                                else
                                        beginStr += CommonConstants::FILEPATH_STR
                                        beginStr += "/" + fileName
                                end
                                return beginStr
			else
				return nil
		end
	end

	def CommonParser.processOperation(arg)
		case arg
			when /^createobject$/i
				return CommonConstants::CREATE_OBJ_STR
			when /^readobject$/i
				return CommonConstants::READ_OBJ_STR
			when /^updateobject$/i
				return CommonConstants::UPDATE_OBJ_STR
			when /^deleteobject$/i
				return CommonConstants::DELETE_OBJ_STR
			when /^truncateobject$/i
				return CommonConstants::TRUNCATE_OBJ_STR
			when /^renameobject$/i
				return CommonConstants::RENAME_OBJ_STR
			when /^versionobject$/i
				return CommonConstants::VERSION_OBJ_STR
			when /^listversions$/i
				return CommonConstants::LIST_VER_STR
			when /^deleteversion$/i
				return CommonConstants::DELETE_VER_STR
			when /^restoreversion$/i
				return CommonConstants::RESTORE_VER_STR
			when /^setusermetadata$/i
				return CommonConstants::SET_USER_MD_STR
			when /^getusermetadata$/i
				return CommonConstants::GET_USER_MD_STR
			when /^deleteusermetadata$/i
				return CommonConstants::DELETE_USER_MD_STR
			when /^setacl$/i
				return CommonConstants::SET_ACL_STR
			when /^getacl$/i
				return CommonConstants::GET_ACL_STR
			when /^getsystemmetadata$/i
				return CommonConstants::GET_SYS_MD_STR
			when /^listusermetadatatags$/i
				return CommonConstants::LIST_USER_MD_TAG_STR
			when /^listobjects$/i
				return CommonConstants::LIST_OBJ_STR
			when /^queryobjects$/i
				return CommonConstants::QUERY_OBJ_STR
			when /^getlistabletags$/i
				return CommonConstants::GET_LIST_TAG_STR
			when /^getserviceinfo$/i
				return CommonConstants::GET_SERVICEINFO_STR
			when /^getobjectinfo$/i
				return CommonConstants::GET_OBJECT_INFO_STR
			when /^shareableurl$/i
				return CommonConstants::SHAREABLE_URL_STR
			when /^createhardlink$/i
				return CommonConstants::CR_HRDLNK_STR				
			else
				puts "Invalid operation was specified. " + \
					"Please try again."
				return nil
		end
	end

	def CommonParser.processAcl(arg)
		if arg == ""
			return {}
		end

		acl = ""
		beginAcl = true

		tempTokens = arg.split(/,/)
		tempTokens.each { |tempToken|
			tokens = tempToken.split(/=/)
			if tokens.length > CommonConstants::PAIR_SIZE
				puts "ACL parameters are in invalid format."
				return nil
			end

			if beginAcl == false
				acl += ","
			end

			acl += tokens[0] + "="
			if tokens[1] == nil
				acl += CommonConstants::NONE
			else
				case tokens[1]
					when /r/i
						acl += CommonConstants::READ
					when /w/i
						acl += CommonConstants::WRITE
					when /f/i
						acl += CommonConstants::FULL
					when /n/i
						acl += CommonConstants::NONE
					else
						puts "ACL parameters are " + \
							"in invalid format."
						return nil
				end
			end

			if beginAcl == true
				beginAcl = false
			end
		}

		return acl
	end

	def CommonParser.processExtent(arg, option, extentlist)

	    	extentlist = Array.new if extentlist.nil?

		if arg == ""
			return extentlist
		end

		pattern = /\A(\d+)?\s*,?\s*([-]?\d+)?\Z/
		match = pattern.match(arg)

		if match.nil?
			puts "#{option} parameters are in invalid format."
			return nil
		end
		
		offset = match[1].nil? ? nil : match[1].to_i()
		length = match[2].nil? ? nil : match[2].to_i()
		
		if (offset.nil? or length.nil?) and \
			option == CommonConstants::USER_EXTENT
			puts "#{option} parameters are in invalid format."
			return nil			
		end

		if offset.nil?
			length = length * -1 if length < 0
		elsif !length.nil? and length < 0
			puts "#{option} parameters are in invalid format."
			return nil
		end

		extent = {CommonConstants::EXTENT_OFFSET => offset,
			CommonConstants::EXTENT_LENGTH => length}
		extentlist.push(extent)

		return extentlist
	end

	def CommonParser.processTag(arg)
		if arg == ""
			return []
		end

		tag = []

		tempTokens = arg.split(/,/)
		tempTokens.each { |tempToken|
			tag.push(tempToken)
		}

		return tag
	end

	def CommonParser.printVersion()
		puts "\nVERSION: #{CommonConstants::VERSION_NUM}\n\n"
	end

	def CommonParser.getArgsArray(operation)
		case operation
			when CommonConstants::CREATE_OBJ_STR
				return CommonConstants::CREATE_OBJ_ARGS
			when CommonConstants::READ_OBJ_STR
				return CommonConstants::READ_OBJ_ARGS
			when CommonConstants::UPDATE_OBJ_STR
				return CommonConstants::UPDATE_OBJ_ARGS
			when CommonConstants::DELETE_OBJ_STR
				return CommonConstants::DELETE_OBJ_ARGS
			when CommonConstants::TRUNCATE_OBJ_STR
				return CommonConstants::TRUNCATE_OBJ_ARGS
			when CommonConstants::RENAME_OBJ_STR
				return CommonConstants::RENAME_OBJ_ARGS
			when CommonConstants::VERSION_OBJ_STR
				return CommonConstants::VERSION_OBJ_ARGS
			when CommonConstants::LIST_VER_STR
				return CommonConstants::LIST_VER_ARGS
			when CommonConstants::DELETE_VER_STR
				return CommonConstants::DELETE_VER_ARGS
			when CommonConstants::RESTORE_VER_STR
				return CommonConstants::RESTORE_VER_ARGS
			when CommonConstants::SET_USER_MD_STR
				return CommonConstants::SET_USER_MD_ARGS
			when CommonConstants::GET_USER_MD_STR
				return CommonConstants::GET_USER_MD_ARGS
			when CommonConstants::DELETE_USER_MD_STR
				return CommonConstants::DELETE_USER_MD_ARGS
			when CommonConstants::SET_ACL_STR
				return CommonConstants::SET_ACL_ARGS
			when CommonConstants::GET_ACL_STR
				return CommonConstants::GET_ACL_ARGS
			when CommonConstants::GET_SYS_MD_STR
				return CommonConstants::GET_SYS_MD_ARGS
			when CommonConstants::LIST_USER_MD_TAG_STR
				return CommonConstants::LIST_USER_MD_TAG_ARGS
			when CommonConstants::LIST_OBJ_STR
				return CommonConstants::LIST_OBJ_ARGS
			when CommonConstants::QUERY_OBJ_STR
				return CommonConstants::QUERY_OBJ_ARGS
			when CommonConstants::GET_LIST_TAG_STR
				return CommonConstants::GET_LIST_TAG_ARGS
			when CommonConstants::GET_SERVICEINFO_STR
				return CommonConstants::GET_SERVICEINFO_ARGS
			when CommonConstants::GET_OBJECT_INFO_STR
				return CommonConstants::GET_OBJECT_INFO_ARGS
			when CommonConstants::SHAREABLE_URL_STR
				return CommonConstants::SHAREABLE_URL_ARGS
			when CommonConstants::CR_HRDLNK_STR
				return CommonConstants::CR_HRDLNK_ARGS
			else
				puts "Incorrect operation was specified. " + \
					"Please try again."
				return nil
		end
	end

	def CommonParser.canonicalizeCustomHeaders(request)
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

		return customHeaders.chomp()
	end

	def CommonParser.createSignString(request, url)
		compositeString = ""

		if request.method != nil
			compositeString += request.method
		end
		compositeString += "\n"

		if request[CommonConstants::CONT_TYPE_HEADER] != nil
			compositeString += \
				request[CommonConstants::CONT_TYPE_HEADER]
		end
		compositeString += "\n"

		if request[CommonConstants::EXTENT_HEADER] != nil
			compositeString += \
				request[CommonConstants::EXTENT_HEADER]
		end
		compositeString += "\n"

		if request[CommonConstants::DATE_HEADER] != nil
			compositeString += \
				request[CommonConstants::DATE_HEADER]
		end
		compositeString += "\n"

		if url != nil
			compositeString += lowerCase(URI.unescape(URI.parse(url).path))

			if URI.parse(url).query != nil
		              	compositeString += "?" + \
					URI.parse(url).query.downcase()
			end
		end
		compositeString += "\n"

		customHeaders = canonicalizeCustomHeaders(request)
		compositeString += customHeaders

		return compositeString
	end

	def CommonParser.lowerCase(s)
                s.gsub!("'", "'\\\\''")
                s.gsub!('"', '\"')
                cmdstr = "perl -e 'use utf8; use POSIX qw(locale_h); binmode(STDOUT, \":utf8\"); print lc(\"#{s}\");'"
                cmd = open("|#{cmdstr}")
                cmdret = cmd.gets
                cmd.close
		return cmdret
	end

	def CommonParser.createSignature(request, url, key, gensigop)
		if key == nil
			return nil
		end

		compositeString = createSignString(request, url)
		if $debugging == 1 || gensigop != nil
			puts "\nString to sign:"
			puts "#{compositeString}"
		end

digest = HMAC.digest(OpenSSL::Digest.new(CommonConstants::SHA1_STR), \
                                     Base64.decode64(key), compositeString)

		return Base64.encode64(digest.to_s()).chomp()
	end

	def CommonParser.createTimestamp()
		return Time.now().httpdate()
	end

	def CommonParser.getFileSize(objectUri)
		return File.size(objectUri)
	end

	def CommonParser.getPartialFileSize(objectUri, offset, length)
		if length == 0
			return getFileSize(objectUri) - offset
		else
			return length
		end
	end

	def CommonParser.readInEntireFile(objectUri)
		return IO.read(objectUri)
	end

	def CommonParser.readInFile(objectUri, offset, length)
		return IO.read(objectUri, getPartialFileSize(objectUri, \
			offset, length), offset)
	end

	def CommonParser.composeChecksumHeader(algorithm, chksumOffset, checksumValue)
		case algorithm
			when CommonConstants::SHA0_STR
				return CommonConstants::SHA0_STR + "/" \
				       + chksumOffset + "/" + checksumValue

			when CommonConstants::SHA1_STR
				return CommonConstants::SHA1_STR + "/" \
				       + chksumOffset + "/" + checksumValue

			when CommonConstants::MD5_STR
				return CommonConstants::MD5_STR + "/" \
				       + chksumOffset + "/" + checksumValue

			else
				return nil
		end
	end

end
