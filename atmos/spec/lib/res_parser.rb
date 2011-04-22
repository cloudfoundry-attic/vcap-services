$:.unshift File.join(File.dirname(__FILE__), '.')

require "consts"
require "stringio"
require "time"
require "net/http"
require "rexml/document"

module ResponseParser
	def ResponseParser.parseResponse(response, operation, readFile)
		if response.code.to_i() >= 400 and response.code.to_i() <= 600
                        begin
        			xmlBody = REXML::Document.new(response.body)
        			error = xmlBody.elements[CommonConstants::ERROR]
        			error_code = \
        				error.elements[CommonConstants::CODE].text
        			error_message = \
        				error.elements[CommonConstants::MESSAGE].text

        			if $debugging == 1
        				puts "\nReturned HTTP error:"
        				puts "\tError code: #{error_code}"
        				print "\tError message: "
        			end
        			puts "#{error_message}"
                        rescue
                		if response.code.to_i() == 403
                			puts "You don't have permission to access /rest on this server."
                			puts "Please make sure compulsive SSL is disabled and try again."
                			return 1
                		else
                                        puts "HTTP Status: #{response.code}"
                                end     
                        end

			return 1
		end

		case operation
			when CommonConstants::CREATE_OBJ_STR, \
			     CommonConstants::VERSION_OBJ_STR
				printObjectId(response, operation)
			when CommonConstants::READ_OBJ_STR
				processReadObject(response, operation, 
						  readFile)
			when CommonConstants::UPDATE_OBJ_STR, \
			     CommonConstants::RESTORE_VER_STR, \
			     CommonConstants::DELETE_OBJ_STR, \
			     CommonConstants::DELETE_VER_STR, \
			     CommonConstants::TRUNCATE_OBJ_STR, \
			     CommonConstants::RENAME_OBJ_STR, \
			     CommonConstants::SET_USER_MD_STR, \
			     CommonConstants::DELETE_USER_MD_STR, \
			     CommonConstants::SET_ACL_STR, \
			     CommonConstants::CR_HRDLNK_STR
				printResultMessage(response, operation)
			else
				return 1
		end

		return 0
	end

	def ResponseParser.printResultMessage(response, operation)
		if $debugging == 1
			puts "\nReturned #{operation} response:\n\t" + \
				"#{CommonConstants::CODE}: " + \
				"#{response.code}\n\t" + \
				"#{CommonConstants::MESSAGE}: " + \
				"#{response.message}"
		end

		if (operation == CommonConstants::UPDATE_OBJ_STR or \
		   operation == CommonConstants::RESTORE_VER_STR or \
		   operation == CommonConstants::DELETE_OBJ_STR or \
		   operation == CommonConstants::DELETE_VER_STR or \
		   operation == CommonConstants::TRUNCATE_OBJ_STR) and \
		   $debugging == 1
			puts "\nDelta: " + \
				"#{response[CommonConstants::DELTA_HEADER]}"
		end
	end

	def ResponseParser.printObjectId(response, operation)
		tokens = response[CommonConstants::LOCATION].split(/\/\s*/)
		objectId = tokens[tokens.length-1]

		if $debugging == 1
			puts "\nReturned #{operation} response:\n\t" + \
				"#{CommonConstants::OBJECT_ID}: #{objectId}"
		else
			puts "#{objectId}"
		end

		if operation == CommonConstants::CREATE_OBJ_STR and \
		   $debugging == 1
			puts "\nDelta: " + \
				"#{response[CommonConstants::DELTA_HEADER]}"
		end
	end


        def ResponseParser.processReadObject(response, operation, readFile)
                directory = false
                if response[CommonConstants::META_HEADER] != nil
                        metadata = response[CommonConstants::META_HEADER].split(/,\s*/)
                        metadata.each { |meta|
                                values = meta.split(/=\s*/)
                                if values[0] == "type"
                                        if values[1] == "directory"
                                             directory = true   
                                        end     
                                        break
                                end       
                        }
                end

		if readFile == nil
			outputFile = File.new(CommonConstants::DEF_FILE, "w")
		else
			outputFile = File.new(readFile, "w")
		end

                
                if directory
                        processReadObjectDirectory(response, outputFile)
                else
                        processReadObjectRegular(response, outputFile)
                end


		if $debugging == 1
			puts "\nCreated object with file name " + \
				"#{outputFile.path} for #{operation} response."
		end

		if !response[CommonConstants::TOKEN_HEADER].nil?
			puts "\t#{CommonConstants::TOKEN}: #{response[CommonConstants::TOKEN_HEADER]}"
		end		
        end

        def ResponseParser.processReadObjectDirectory(response, outputFile)
                xmlBody = REXML::Document.new(response.body)
                rootNode = xmlBody.elements["ListDirectoryResponse"]
                listNode = rootNode.elements["DirectoryList"]

                puts "\nDirectory entries:" if $debugging == 1
                listNode.each_element("//DirectoryEntry") do |entry|
                        oid = entry.elements["ObjectID"].text
                        filetype = entry.elements["FileType"].text
                        filename = entry.elements["Filename"].text

                        puts "#{filename} (#{filetype}) (#{oid})" if $debugging == 1

			sysmd = entry.elements[CommonConstants::SYSTEM_METADATA_LIST]
			if !sysmd.nil?
			        sysmd = sysmd.elements
			        sysmd.each do |e|
			                tmp = "\t#{e.elements[CommonConstants::METADATA_NAME].text}"
                                        tmp += " = "
                                        if !e.elements[CommonConstants::METADATA_VALUE].text.nil?
        			                tmp += e.elements[CommonConstants::METADATA_VALUE].text
                                        end
                        		puts tmp if $debugging == 1
				end
                        end

			umd = entry.elements[CommonConstants::USER_METADATA_LIST]
			if !umd.nil?
			        umd = umd.elements
			        umd.each do |e|
			                tmp = "\t#{e.elements[CommonConstants::METADATA_NAME].text}"
                                        tmp += " = "
                                        if !e.elements[CommonConstants::METADATA_VALUE].text.nil?
        			                tmp += e.elements[CommonConstants::METADATA_VALUE].text
                                        end
                        		puts tmp if $debugging == 1
				end
                                puts "\n" if $debugging == 1
                        end        

                end

                outputFile.print "#{response.body}"
        end

        
	def ResponseParser.processReadObjectRegular(response, outputFile)
		
		boundary = ResponseParser.getBoundary(response[CommonConstants::CONT_TYPE_HEADER])
		if response.body.nil?
		elsif !boundary.nil?
			boundary = "--#{boundary}"
			idx = boundary.length

			currentrange = 0
			outputFile.print "-- Begin #{CommonConstants::MULTIPART_BYTERANGE} Response --\n"

			body = StringIO.new(response.body)
			while line = body.gets

				if line =~ /^#{boundary}/
					if line[idx, 2] == "--"
						outputFile.print "-- End #{CommonConstants::MULTIPART_BYTERANGE} Response --\n"
						break
					end

					currentrange += 1					

					line = body.gets
					if /^#{CommonConstants::CONT_TYPE_HEADER}:\s*(.+)$/.match(line)
						outputFile.print "-- Range: #{currentrange} "
						outputFile.print " Content-Type: #{$1.chomp}"
					else
						puts "Invalid #{CommonConstants::MULTIPART_BYTERANGE} header (#{CommonConstants::CONT_TYPE_HEADER})"
						break
					end

					line = body.gets
					if /#{CommonConstants::CONT_RANGE_HEADER}:\s*bytes\s*(\d+)-(\d+)\/(\d+)/.match(line)
						outputFile.print " Byte Offsets: #{$1}-#{$2}/#{$3} --\n"
					else
						puts "Invalid #{CommonConstants::MULTIPART_BYTERANGE} header (#{CommonConstants::CONT_RANGE_HEADER})"
						break
					end

					line = body.gets
					if line != CommonConstants::CRLF
						puts "Invalid #{CommonConstants::MULTIPART_BYTERANGE} header"					   
						break
					end
				elsif currentrange > 0
					outputFile.print "#{line}"
				end

			end

		else
			outputFile.print "#{response.body}"
		end
	end

	def ResponseParser.getBoundary(contentType)
	    if contentType =~ \
	    	/^#{CommonConstants::MULTIPART_BYTERANGE};\s*#{CommonConstants::BOUNDARY}=\s*(.+)$/
	    	return $1
	    end
	    return nil
	end

end
