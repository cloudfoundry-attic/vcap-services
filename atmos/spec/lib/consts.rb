require 'getoptlong'

module CommonConstants
	UID = "UID"
	FILENAME = "Filename"
	SIGNATURE = "Signature"
	TIMESTAMP = "Timestamp"
	USER_ACL = "UserACL"
	GROUP_ACL = "GroupACL"
	OBJECT_URI = "ObjectURI"
	METADATA = "Metadata"
	LISTABLE_METADATA = "ListableMetadata"
	METADATA_NAME = "Name"
	METADATA_VALUE = "Value"
	METADATA_LISTABLE = "Listable"
	OBJECT_ID = "ObjectID"
	VER_OBJECT_ID = "VersionObjectID"
	VER_ENTRY = "Ver"
	VER_NUMBER = "VerNum"
	VER_OID = "OID"
	VER_CRTIME = "itime"
	OBJECT = "Object"
	SYSTEM_METADATA_LIST = "SystemMetadataList"
	USER_METADATA_LIST = "UserMetadataList"
	EXTENT = "Extent"
	USER_EXTENT = "UserExtent"
	EXTENT_OFFSET = "Offset"
	EXTENT_LENGTH = "Size"
	EXTENT_LIST = "ExtentList"
	ATTACHMENT = "Object"
	TAG = "Tag"
	XQUERY = "XQuery"
	READ_FILE = "ReadFile"
	HMAC_KEY = "HMACKey"
	CONTENT_TYPE = "ObjectType"
	INCL_MD = "IncludeMetadata"
	TOKEN = "Token"
	LIMIT = "Limit"
	TRUNC_SIZE = "TruncSize"
        WSVERSION = "WSVersion"
	SMD_TAG = "SystemTags"
	UMD_TAG = "UserTags"
	LINK_NAME = "LinkName"
        NEW_NAME = "NewName"
        FORCE = "Force"
	EXPIRES = "Expires"
	WSCHKSUMALGO = "WsChecksumAlgo"
	WSCHKSUMOFFSET = "WsChecksumOffset"
	WSCHKSUMVALUE = "WsChecksumValue"
        INCLUDE_LAYOUT = "IncludeLayout"

	LOG_FILE = "LogFile"
	IP_ADDRESS = "IpAddress"
	HELP = "Help"
	VERSION = "Version"
	VERBOSE = "Verbose"
	OPERATION = "Operation"
	TIMES = "Times"
	GEN_SIG = "GenSig"

	HTTP_METHOD = "HTTPMethod"
	URI_PATH = "URIPath"

	UID_HEADER = "x-emc-uid"
	SIG_HEADER = "x-emc-signature"
	DATE_HEADER = "Date"
	VMW_DATE_HEADER = "x-emc-date"
	META_HEADER = "x-emc-meta"
	SYSTEM_META_HEADER = "x-emc-system-meta"
	USER_META_HEADER = "x-emc-user-meta"
	LIST_META_HEADER = "x-emc-listable-meta"
	UNENCODABLE_META_HEADER = "x-emc-unencodable-meta"
	TAG_HEADER = "x-emc-tags"
	SYSTEM_TAG_HEADER = "x-emc-system-tags"
	USER_TAG_HEADER = "x-emc-user-tags"
	XQUERY_HEADER = "x-emc-xquery"
	USER_ACL_HEADER = "x-emc-useracl"
	GROUP_ACL_HEADER = "x-emc-groupacl"
	DELTA_HEADER = "x-emc-delta"
	EXTENT_HEADER = "Range"
	BYTES_VALUE = "Bytes="
	CONT_LEN_HEADER = "Content-Length"
	CONT_TYPE_HEADER = "Content-Type"
	CONT_MD5_HEADER = "Content-MD5"
	CONT_RANGE_HEADER = "Content-Range"
	INCL_MD_HEADER = "x-emc-include-meta"
	TOKEN_HEADER = "x-emc-token"
	LIMIT_HEADER = "x-emc-limit"
	MODE_HEADER = "x-emc-mode"
	TRUNC_SIZE_HEADER = "x-emc-size"
        WSVERSION_HEADER = "x-emc-wsversion"
        PATH_HEADER = "x-emc-path"
        FORCE_HEADER = "x-emc-force"
	VERSION_OID_HEADER = "x-emc-version-oid"
	WSCHECKSUM_HEADER = "x-emc-wschecksum"
        INCLUDE_LAYOUT_HEADER = "x-emc-include-layout"

	NAMESPACE = "http://www.emc.com/maui"
	PREFIX = "maui"
        ATMOS = "Atmos"
	SERVICE = "Service"

	HTTP_PROTOCOL = "http://"
	HTTPS_PROTOCOL = "https://"
	HTTP_PORT = 80
	CMSSL_PORT = 443
	WSSSL_PORT = 10080
	HTTPDCERT_PATH = "/var/local/maui/httpdssl/"
	
	OBJECT_TYPE = "application/octet-stream"
	VERSION_NUM = 1.1
	SHA0_STR = "sha0"
	SHA1_STR = "sha1"
	MD5_STR = "md5"
	MULTIPART_BYTERANGE = "multipart/byteranges"
	BOUNDARY = "boundary"

	REST_STR = "/rest"
	OBJECTS_STR = "/objects"
	METADATA_STR = "?metadata"
	USER_STR = "/user"
	SYSTEM_STR = "/system"
	TAGS_STR = "/tags"
	ACL_STR = "?acl"
	LIST_TAGS_STR = "?listabletags"
	VERSIONS_STR = "?versions"
	FILEPATH_STR = "/namespace"
        SERVICE_STR = "/service"
	INFO_STR = "?info"
	HARDLINK_STR = "?hardlink"
        RENAME_STR = "?rename"

	READ = "READ"
	WRITE = "WRITE"
	FULL = "FULL_CONTROL"
	NONE = "NONE"

	USER = "USER"
	GROUP = "GROUP"

	PAIR_SIZE = 2
	EXTENT_SIZE = 2
	SINGLE_SIZE = 1
	EMPTY_SIZE = 0
	IP_PARTS = 4

        URL_REPLACE_VAL = "dummyurlvalue"

	CRLF = "\r\n"

	MODE_UPDATE = "update"
	MODE_APPEND = "append"
	MODE_TRUNCATE = "truncate"

	CREATE_OBJ_STR = "CreateObject"
	READ_OBJ_STR = "ReadObject"
	UPDATE_OBJ_STR = "UpdateObject"
	DELETE_OBJ_STR = "DeleteObject"
	TRUNCATE_OBJ_STR = "TruncateObject"
        RENAME_OBJ_STR = "RenameObject"
	VERSION_OBJ_STR = "VersionObject"
	LIST_VER_STR = "ListVersions"
	DELETE_VER_STR = "DeleteVersion"
	RESTORE_VER_STR = "RestoreVersion"

	SET_USER_MD_STR = "SetUserMetadata"
	GET_USER_MD_STR = "GetUserMetadata"
	DELETE_USER_MD_STR = "DeleteUserMetadata"

	SET_ACL_STR = "SetACL"
	GET_ACL_STR = "GetACL"

	GET_SYS_MD_STR = "GetSystemMetadata"
	LIST_USER_MD_TAG_STR = "ListUserMetadataTags"
	LIST_OBJ_STR = "ListObjects"
	QUERY_OBJ_STR = "QueryObjects"
	GET_LIST_TAG_STR = "GetListableTags"
        GET_SERVICEINFO_STR = "GetServiceInfo"
	GET_OBJECT_INFO_STR = "GetObjectInfo"
	CR_HRDLNK_STR = "CreateHardlink"

	SHAREABLE_URL_STR = "ShareableUrl"

	OPERATIONS = [ CREATE_OBJ_STR, READ_OBJ_STR, UPDATE_OBJ_STR, \
		       DELETE_OBJ_STR, TRUNCATE_OBJ_STR, RENAME_OBJ_STR, \
		       VERSION_OBJ_STR, LIST_VER_STR, DELETE_VER_STR, RESTORE_VER_STR, \
		       SET_USER_MD_STR, GET_USER_MD_STR, DELETE_USER_MD_STR, \
		       SET_ACL_STR, GET_ACL_STR, GET_SYS_MD_STR, \
		       LIST_USER_MD_TAG_STR, LIST_OBJ_STR, QUERY_OBJ_STR, \
		       GET_LIST_TAG_STR, GET_SERVICEINFO_STR, GET_OBJECT_INFO_STR, SHAREABLE_URL_STR, CR_HRDLNK_STR ]

	CREATE_OBJ_ARGS = [ UID, USER_ACL, GROUP_ACL, OBJECT_URI, METADATA, \
			    LISTABLE_METADATA, CONTENT_TYPE, FILENAME, WSVERSION, \
			    WSCHKSUMALGO, WSCHKSUMOFFSET, WSCHKSUMVALUE]
	DELETE_OBJ_ARGS = [ UID, OBJECT_ID, FILENAME ]
	UPDATE_OBJ_ARGS = [ UID, USER_ACL, GROUP_ACL, OBJECT_ID, EXTENT, \
			    OBJECT_URI, METADATA, LISTABLE_METADATA, \
			    USER_EXTENT, CONTENT_TYPE, FILENAME, WSVERSION, \
			    WSCHKSUMALGO, WSCHKSUMOFFSET, WSCHKSUMVALUE]
	READ_OBJ_ARGS   = [ UID, OBJECT_ID, EXTENT, READ_FILE, FILENAME, LIMIT, TOKEN, WSVERSION, INCL_MD, SMD_TAG, UMD_TAG ]
	TRUNCATE_OBJ_ARGS   = [ UID, OBJECT_ID, FILENAME, TRUNC_SIZE ]
	RENAME_OBJ_ARGS   = [ UID, OBJECT_ID, FILENAME, NEW_NAME, FORCE ]
	VERSION_OBJ_ARGS = [ UID, OBJECT_ID, FILENAME ]
	LIST_VER_ARGS = [ UID, OBJECT_ID, FILENAME ]
	DELETE_VER_ARGS = [ UID, OBJECT_ID, FILENAME ]
	RESTORE_VER_ARGS = [ UID, VER_OBJECT_ID, OBJECT_ID, FILENAME ]

	SET_USER_MD_ARGS = [ UID, OBJECT_ID, METADATA, LISTABLE_METADATA, \
			     FILENAME, WSVERSION ]
	GET_USER_MD_ARGS = [ UID, OBJECT_ID, TAG, FILENAME, WSVERSION ]
	DELETE_USER_MD_ARGS = [ UID, OBJECT_ID, TAG, FILENAME ]

	SET_ACL_ARGS = [ UID, OBJECT_ID, USER_ACL, GROUP_ACL, FILENAME ]
	GET_ACL_ARGS = [ UID, OBJECT_ID, FILENAME ]

	GET_SYS_MD_ARGS = [ UID, OBJECT_ID, TAG, FILENAME, WSVERSION ]
	LIST_USER_MD_TAG_ARGS = [ UID, OBJECT_ID, FILENAME ]
	LIST_OBJ_ARGS = [ UID, TAG, FILENAME, INCL_MD, LIMIT, TOKEN, SMD_TAG, UMD_TAG ]
	QUERY_OBJ_ARGS = [ UID, XQUERY ]
	GET_LIST_TAG_ARGS = [ UID, TAG, TOKEN ]
        GET_SERVICEINFO_ARGS = [ UID ]
        GET_OBJECT_INFO_ARGS = [ UID, OBJECT_ID, FILENAME, INCLUDE_LAYOUT ]
	SHAREABLE_URL_ARGS = [ UID, OBJECT_ID, FILENAME, EXPIRES ]
	CR_HRDLNK_ARGS   = [ UID, OBJECT_ID, FILENAME, LINK_NAME ]

	NO_ARG_ARGS = [ HELP, VERSION, VERBOSE ]
	COMMON_ARGS = [ HMAC_KEY, LOG_FILE, IP_ADDRESS, HELP, VERSION, \
			VERBOSE, OPERATION, TIMES, GEN_SIG ]

	LOCATION = "Location"
	DEF_FILE = "/tmp/tmp"
	ERROR = "Error"
	CODE = "Code"
	MESSAGE = "Message"
	ERROR_CODE = "error-code"
	ERROR_MESSAGE = "error-message"
	NAME = "Name"
	VALUE = "Value"
	META = /^x-emc-meta-/i
	LIST_META = /^x-emc-listable-meta-/i
	TAGS = "x-emc-tags"
	LIST_TAGS = "x-emc-listable-tags"
	LIST_OBJS_RESP = "ListObjectsResponse/"
	QUERY_OBJS_RESP = "QueryObjectsResponse/"
	LIST_VERS_RESP = "ListVersionsResponse/"
	OBJECT_INFO_RESP = "GetObjectInfoResponse"
	METHOD = "Method"
	URL = "URL"

	CREATE_LOG = "_create.log"
	READ_LOG = "_read.log"
	APPEND_LET = "a"

	UID_OP_STR = '--uid'
	FILENAME_OP_STR = '--filename'
	OB_URI_OP_STR = '--objecturi'
	USR_ACL_OP_STR = '--useracl'
	GRP_ACL_OP_STR = '--groupacl'
	META_OP_STR = '--metadata'
	LIS_MD_OP_STR = '--listmetadata'
	OB_ID_OP_STR = '--objectid'
	RST_OB_ID_OP_STR = '--restoreobjectid'
	TAG_OP_STR = '--tag'
	QRY_OP_STR = '--xquery'
	EXT_OP_STR = '--extent'
	US_EXT_OP_STR = '--userextent'
	READ_OP_STR = '--readfile'
	KEY_OP_STR = '--key'
	LOG_OP_STR = '--logfile'
	IP_ADDR_OP_STR = '--ipaddress'
	HELP_OP_STR = '--help'
	VERS_OP_STR = '--version'
	VERB_OP_STR = '--verbose'
	OPER_OP_STR = '--operation'
	TIME_OP_STR = '--times'
	TYPE_OP_STR = '--contenttype'
	INCL_MD_OP_STR = '--includemeta'
	TOKEN_OP_STR = '--token'
	LIMIT_OP_STR = '--limit'
	TRUNC_SIZE_OP_STR = '--size'
	GET_SERVICEINFO_OP_STR = '--getserviceinfo'
        WSVERSION_OP_STR = '--wsversion'
	SMD_TAG_OP_STR = '--systemtags'
	UMD_TAG_OP_STR = '--usertags'
	CR_HRDLNK_OP_STR = '--linkname'
        NAME_OP_STR = '--name'
        FORCE_OP_STR = '--force'
	GEN_SIG_OP_STR = '--gensig'
	EXPIRES_OP_STR = '--expires'
	WSCHKSUMALGO_OP_STR = '--checksumalgo'
	WSCHKSUMOFFSET_OP_STR = '--checksumoffset'
	WSCHKSUMVALUE_OP_STR = '--checksumvalue'
        INCLUDE_LAYOUT_OP_STR = '--layout'

	UID_OP = '-p'
	FILENAME_OP = '-c'
	OB_URI_OP = '-u'
	USR_ACL_OP = '-a'
	GRP_ACL_OP = '-g'
	META_OP = '-m'
	LIS_MD_OP = '-b'
	OB_ID_OP = '-i'
	RST_OB_ID_OP = '-d'
	TAG_OP = '-t'
	QRY_OP = '-q'
	EXT_OP = '-e'
	US_EXT_OP = '-x'
	READ_OP = '-r'
	KEY_OP = '-k'
	LOG_OP = '-l'
	IP_ADDR_OP = '-s'
	HELP_OP = '-h'
	VERS_OP = '-o'
	VERB_OP = '-v'
	OPER_OP = '-f'
	TIME_OP = '-n'
	TYPE_OP = '-y'
	TRUNC_SIZE_OP = '-z'
	WSCHKSUMALGO_OP = '-w'

	OPTIONS = GetoptLong.new(\
		[ UID_OP_STR, UID_OP, GetoptLong::REQUIRED_ARGUMENT ],\
		[ FILENAME_OP_STR, FILENAME_OP, GetoptLong::OPTIONAL_ARGUMENT ],\
		[ OB_URI_OP_STR, OB_URI_OP, GetoptLong::REQUIRED_ARGUMENT ],\
		[ USR_ACL_OP_STR, USR_ACL_OP, GetoptLong::REQUIRED_ARGUMENT ],\
		[ GRP_ACL_OP_STR, GRP_ACL_OP, GetoptLong::REQUIRED_ARGUMENT ],\
		[ META_OP_STR, META_OP, GetoptLong::REQUIRED_ARGUMENT ],\
		[ LIS_MD_OP_STR, LIS_MD_OP, GetoptLong::REQUIRED_ARGUMENT ], \
		[ OB_ID_OP_STR, OB_ID_OP, GetoptLong::REQUIRED_ARGUMENT ],\
		[ RST_OB_ID_OP_STR, RST_OB_ID_OP, GetoptLong::REQUIRED_ARGUMENT ],\
		[ TAG_OP_STR, TAG_OP, GetoptLong::REQUIRED_ARGUMENT ],\
		[ QRY_OP_STR, QRY_OP, GetoptLong::REQUIRED_ARGUMENT ],\
		[ EXT_OP_STR, EXT_OP, GetoptLong::REQUIRED_ARGUMENT ],\
		[ US_EXT_OP_STR, US_EXT_OP, GetoptLong::REQUIRED_ARGUMENT ],\
		[ READ_OP_STR, READ_OP, GetoptLong::REQUIRED_ARGUMENT ],\
		[ KEY_OP_STR, KEY_OP, GetoptLong::REQUIRED_ARGUMENT ],\
		[ LOG_OP_STR, LOG_OP, GetoptLong::REQUIRED_ARGUMENT ],\
		[ IP_ADDR_OP_STR, IP_ADDR_OP, GetoptLong::REQUIRED_ARGUMENT ],\
		[ HELP_OP_STR, HELP_OP, GetoptLong::NO_ARGUMENT ],\
		[ VERS_OP_STR, VERS_OP, GetoptLong::NO_ARGUMENT ],\
		[ VERB_OP_STR, VERB_OP, GetoptLong::NO_ARGUMENT ],\
		[ OPER_OP_STR, OPER_OP, GetoptLong::REQUIRED_ARGUMENT ],\
		[ TIME_OP_STR, TIME_OP, GetoptLong::REQUIRED_ARGUMENT ],\
		[ TYPE_OP_STR, TYPE_OP, GetoptLong::REQUIRED_ARGUMENT ], \
		[ TRUNC_SIZE_OP_STR, TRUNC_SIZE_OP, GetoptLong::REQUIRED_ARGUMENT ], \
		[ LIMIT_OP_STR, GetoptLong::REQUIRED_ARGUMENT ], \
		[ TOKEN_OP_STR, GetoptLong::REQUIRED_ARGUMENT ], \
		[ INCL_MD_OP_STR, GetoptLong::NO_ARGUMENT ], \
                [ WSVERSION_OP_STR, GetoptLong::REQUIRED_ARGUMENT ], \
		[ GET_SERVICEINFO_OP_STR, GetoptLong::NO_ARGUMENT ], \
		[ SMD_TAG_OP_STR, GetoptLong::REQUIRED_ARGUMENT ], \
		[ CR_HRDLNK_OP_STR, GetoptLong::REQUIRED_ARGUMENT ], \
		[ UMD_TAG_OP_STR, GetoptLong::REQUIRED_ARGUMENT ], \
		[ NAME_OP_STR, GetoptLong::REQUIRED_ARGUMENT ], \
		[ FORCE_OP_STR, GetoptLong::NO_ARGUMENT ], \
		[ GEN_SIG_OP_STR, GetoptLong::NO_ARGUMENT], \
		[ EXPIRES_OP_STR, GetoptLong::REQUIRED_ARGUMENT ], \
		[ WSCHKSUMALGO_OP_STR, WSCHKSUMALGO_OP, GetoptLong::REQUIRED_ARGUMENT ], \
		[ WSCHKSUMOFFSET_OP_STR, GetoptLong::REQUIRED_ARGUMENT ], \
		[ WSCHKSUMVALUE_OP_STR, GetoptLong::REQUIRED_ARGUMENT ], \
		[ INCLUDE_LAYOUT_OP_STR, GetoptLong::NO_ARGUMENT ])
end


