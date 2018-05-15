--[[
存储代理业务配置模块v1
Author:      杨婷
Mail:        yangting@dnion.com
Version:     1.0
Doc:
Modify：
    2016-07-19  杨婷  初始版本
]]

local enum =require("common.enum")

local _M = {
    _VERSION = '1.00',
}
_M.config = {}

_M.config["aws2_timediff"] = 15
_M.config["aws4_timediff"] = 5
_M.config["MD5SUM"] = true

local admin_op = {
    "GET_DATALOG",
    "GET_USERS",
    "GET_BUCKETS",
    "GET_OBJECTS",

    "POST_FULL_RSYNC",
    "USER_ADMIN", -- for user admin and stats and quota op
    "CREATE_USER",
    "QUERY_USER",
    "DELETE_USER",
    "SET_PERMISSION",
    "SET_SYNC",
}

--admin_OP = CreatEnumTable(admin_OP)
local admin_oprev = enum.CreatEnumTable(admin_op)
function _M.get_admin_op(index)
	return admin_op[index]
end
function _M.get_admin_op_rev(op)
	return admin_oprev[op]
end

local s3_op =
{
    --service
    "LIST_BUCKETS",
    --buckets
      --supposted
    "CREATE_BUCKET",
    "LIST_OBJECTS",
    "DELETE_BUCKET",
    "HEAD_BUCKET",
    "GET_BUCKET_ACL",

    --objects
      --supposted
    "GET_OBJECT",
    "GET_OBJECT_ACL",
    "HEAD_OBJECT",
    "DELETE_OBJECT",
    "PUT_OBJECT",
    "INITIATE_MULTIPART_UPLOAD",
    "UPLOAD_PART",
    "COMPLETE_MULTIPART_UPLOAD",
    "ABORT_MULTIPART_UPLOAD",
    "LIST_PARTS",
    "PUT_BUCKET_LIFECYCLE",
    "GET_BUCKET_LIFECYCLE",
    "LIST_MULTIPART_UPLOADS",
    "DELETE_BUCKET_LIFECYCLE",

      --not supposted
    "DELETE_MULTIPLE_OBJECTS",
    "GET_OBJECT_TORRENT",
    "OPTIONS_OBJECT",
    "POST_OBJECT",
    "POST_OBJECT_RESTORE",
    "PUT_OBJECT_ACL",
    "PUT_OBJECT_COPY",
    "UPLOAD_PART_COPY",
      --not supposted
    "GET_BUCKET_LOCATION",
    "DELETE_BUCKET_CORS",
    "DELETE_BUCKET_POLICY",
    "DELETE_BUCKET_REPLICATION",
    "DELETE_BUCKET_TAGGING",
    "DELETE_BUCKET_WEBSITE",
    "GET_BUCKET_ACCELERATE",
    "GET_BUCKET_CORS",
    "GET_BUCKET_POLICY",
    "GET_BUCKET_LOGGING",
    "GET_BUCKET_NOTIFICATION",
    "GET_BUCKET_REPLICATION",
    "GET_BUCKET_TAGGING",
    "GET_BUCKET_OBJECT_VERSIONS",
    "GET_BUCKET_REQUESTPAYMENT",
    "GET_BUCKET_VERSIONING",
    "GET_BUCKET_WEBSITE",
    "PUT_BUCKET_ACCELERATE",
    "PUT_BUCKET_ACL",
    "PUT_BUCKET_CORS",
    "PUT_BUCKET_POLICY",
    "PUT_BUCKET_LOGGING",
    "PUT_BUCKET_NOTIFICATION",
    "PUT_BUCKET_REPLICATION",
    "PUT_BUCKET_TAGGING",
    "PUT_BUCKET_REQUESTPAYMENT",
    "PUT_BUCKET_VERSIONING",
    "PUT_BUCKET_WEBSITE",
}
--S3_OP = CreatEnumTable(S3_OP)
local s3_oprev = enum.CreatEnumTable(s3_op)
function _M.get_s3_op(index)
	return s3_op[index]
end
function _M.get_s3_op_rev(op)
	return s3_oprev[op]
end
local AWS_AUTH =
{
    "HEADER_V2",
    "HEADER_V4",
    "ARGS_V2",
    "ARGS_V4",
}

local AWS_AUTHrev = enum.CreatEnumTable(AWS_AUTH)
function _M.auth(authtype)
    return AWS_AUTHrev[authtype]
end

--aws2_Algorithm--->base64_HMAC_SHA128
--aws4_Algorithm--->base64_HMAC_SHA256

_M.config["default_hostdomain"] = "s3.dnion.com"

_M.config["sub_resource"] =
{
    "acl",   "lifecycle", "policy", "uploads","partNumber",
    "uploadId", "delete", "location", "logging", "notification",
    "requestPayment", "response-cache-control", "response-content-disposition",
    "response-content-encoding", "response-content-language","response-content-type",
    "response-expires", "torrent","versionId", "versioning", "versions", "website",
}
_M.config["sub_resource"] = enum.CreatEnumTable(_M.config["sub_resource"])

_M.config["list_params"] =
{
	"delimiter",
    "encoding-type",
    "marker",
    "max-keys",
    "prefix",
    --......
}
_M.config["list_params"] = enum.CreatEnumTable(_M.config["list_params"])

_M.config["Error"] = {
--xml
-- <?xml version="1.0" encoding="UTF-8"?>
-- <Error>
-- 	<Code>NoSuchKey</Code>
-- 	<Message>The resource you requested does not exist</Message>
-- 	<Resource>/mybucket/myfoto.jpg</Resource>
-- 	<RequestId>4442587FB7D0A2F9</RequestId>
-- </Error>
--json_error=
-- {
-- 		Code = "NoSuchKey",
-- 		Message = "The resource you requested does not exist",
-- 		Resource = "/mybucket/myfoto.jpg",
-- 		RequestId = "4442587FB7D0A2F9",
-- }
}

_M.config["s3_bucket_option"] = {
	PUT_bucket = {
		hbase_op = "storageproxy_hbase_post",
		-- hbase_request_headers = {},
		s3_response_headers = {},
		-- s3_response_body = {},
	},
	PUT_bucket_acl = {
		hbase_op = "hbase_put(uri, body, headers)",
	},
	GET_bucket = {
		hbase_op = "storageproxy_hbase_get",
		-- hbase_request_headers = {},
		s3_response_headers = {},
		-- s3_response_body = {},
	},
	GET_bucket_acl = {
		hbase_op = "hbase_get(uri, body, headers)",
	},
}
_M.config["s3_bucket_option"]["PUT_bucket"]["s3_response_headers"]["x-amz-request-id"] = "tx00000000000000000027b-0057835f2e-107b-default"

_M.config["s3_GET_Service"] = {
	hbase_op = "storageproxy_hbase_get",
	s3_response_body = {
		ListAllMyBucketsResult = {
			Owner = {
				ID = "",
				DisplayName ="",
			},
			Buckets = {
				--real format[
				--{
				-- 	"Name" = "",
				-- 	"CreationDate" = "",
				-- }
				-- {
				-- 	"Name" = "",
				-- 	"CreationDate" = "",
				-- }]
			},
		},
	},
	s3_response_headers = {},
}
_M.config["s3_GET_Service"]["s3_response_headers"]["x-amz-request-id"] = "tx00000000000000000027b-0057835f2e-107b-default"

_M.config["constants"] = {
    empty_md5 = table.concat({0xd4,0x1d,0x8c,0xd9,0x8f,0x00,0xb2,0x04,0xe9,0x80,0x09,0x98,0xec,0xf8,0x42,0x7e}),
    empty_md5_hex = "d41d8cd98f00b204e9800998ecf8427e",
    empty_md5_base64 = "1B2M2Y8AsgTpgAmY7PhCfg==",
}

_M.config["hbase_config"] = {
    server = "127.0.0.1",
    port = "9090",
    max_scan_once = 1000,
    retry_interval = 0.5,  -- in seconds
    retry_times = 1,
}
_M.config["redis_config"] = {
    servers = {
        "127.0.0.1:6082",
    },
}

_M.config["cache_config"] = {
    rcache = {
        enabled = true,
        expire = 600,
        factor = 2, -- for VERY_STABLE, expire = expire * factor
    },
    lcache = {
        enabled = true,
        items = 2000,
        expire = 60,
        factor = 2, -- for VERY_STABLE, expire = expire * factor
    }
}

_M.config["rgw_config"] = {
    request_timeout = 10,
    retry_times = 1,
    retry_interval = 6,
}

_M.config["chunk_config"] = {
    size = 4194304,   -- 4MB
    hsize = 4096,     -- 4KB
    wconcurrent = 8,  -- how may chunks can be sent to ceph concurrently?
    rconcurrent = 16, -- how may chunks can be read from ceph concurrently?
}

_M.config["s3_config"] = {
    multipart_config = {
        psize = 5242880,   -- 5MB
        p_num_min = 1,
        p_num_max = 10000,
    },
    maxkeys = {
        default = 1000,
        max = 1000,
    },
    maxparts = {
        default = 1000,
        max = 1000,
    },
}

_M.config["trace_time"] = true

_M.config["AWS_v4"] = false

_M.config["AUTH_OPEN"] = true  --true:AUTH enabled,false:AUTH disabled


_M.config["AUTH_DIFFTIME"] = true  --AUTH TIEM DIFF

_M.config["TEST_NOT_HBASE"] = false

_M.config["use_ngx_capture"] = false

_M.config["STATS"] = true
_M.config["inspect"] = false

_M.config["sync_config"] = { 
    role = "master",
    enabled = true,
    clusterid = "erea-beijing-2",
    lock_ttl = 600,
    datalog = {
        max = 1023, --sharing num
        name = "datalog",
        maxkeys = {
            default = 100,
            max = 1000,
        }
    },
    peers= {
        --[[{
            id = "erea-beijing-1",
            ip = "121.14.254.233",
            log_port = 6080,
            data_port = 6081,
            sync_mode = 0,   -- 0:none;   1:user;   2:user+bucket;   3:user+bucket+data;
        },]]
    },
    full = {
        timer_cycle = 300,
    },
}

_M.config["expire"] = {
    enabled = true,
    name = "expire",
    max = 1023, --sharing num
    timer_cycle = 300,
}

_M.config["deleteobj"] = {
    name = "deleteobj",
    max = 1023, --sharing num
    timer_cycle = 300,
}
_M.config["delete_bucket"] = {
    timer_cycle = 300,
}
_M.config["deletemu"] = {
    name = "delete_completed_aborted_multiupload",
    timer_cycle = 600,
}

-- for metadata replication
_M.config["consensus"] = "2pc"
-- alternative option: 2pc,raft,multmerge?
_M.config["root_uid"] = "root"

_M.config["luacov"] = false

return _M
