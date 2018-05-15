--[[
存储代理状态码配置v1
Author:      杨婷
Mail:        yangting@dnion.com
Version:     1.0
Doc:
Modify：
    2016-07-19  杨婷  初始版本
]]

local _M = {
    _VERSION = '1.00',
}

-- 200, "00000000", 鉴权成功，"Success"

-- 200,"20000000",无法获取S3身份验证信息(header/uri_args), 
-- 200,"20000001",请求不匹配代理要求的协议类型(s3_aws2/s3_aws4), "Current request didn't match the protocol--s3_aws2/s3_aws4"
-- 200,"20000002",根据AWSAccessKeyId获取AWS_SecretAccessKey失败,
-- 200,"20000003",获取s3身份验证相关参数失败

-- 200,"20000004",当前请求消息没有时间头, "Current request didn't Date or x-amz-date header"
-- 200,"20000005",当前请求时间不匹配S3协议要求时间(s3_aws2-15minute/s3_aws4-5minute),
-- 200,"20000006",分析请求消息参数失败(body\uri_args),
-- 200,"20000007",分析S3接口类型(service/bucket/object)失败

-- 404, "20000010", 身份验证失败

_M.S3Error = {
	--code  = {
	--s3code 	
    --message
	--httpcode
	-- }                                                          
    ["1001"] = {"AccessDenied",                             "Access Denied",                                                                                                                                                                                                                                                                             "403"},
    ["1002"] = {"AccountProblem",                           "There is a problem with your AWS account that prevents the operation from completing successfully. Please Contact Dnion.",                                                                                                                                                                 "403"},
    ["1003"] = {"AmbiguousGrantByEmailAddress",             "The email address you provided is associated with more than one account.",                                                                                                                                                                                                                  "400"},
    ["1004"] = {"BadDigest",                                "The Content-MD5 you specified did not match what we received.",                                                                                                                                                                                                                             "400"},
    ["1005"] = {"BucketAlreadyExists",                      "The requested bucket name is not available. The bucket namespace is shared by all users of the system. Please select a different name and try again.",                                                                                                                                      "409"},
    ["1006"] = {"BucketAlreadyOwnedByYou",                  "Your previous request to create the named bucket succeeded and you already own it. You get this error in all AWS regions except US East (N. Virginia) region, us-east-1. In us-east-1 region, you will get 200 OK, but it is no-op (if bucket exists it Amazon S3 will not do anything).",  "409"},
    ["1007"] = {"BucketNotEmpty",                           "The bucket you tried to delete is not empty.",                                                                                                                                                                                                                                              "409"},
    ["1008"] = {"CredentialsNotSupported",                  "This request does not support credentials.",                                                                                                                                                                                                                                                "400"},
    ["1009"] = {"CrossLocationLoggingProhibited",           "Cross-location logging not allowed. Buckets in one geographic location cannot log information to a bucket in another location.",                                                                                                                                                            "403"},
    ["1010"] = {"EntityTooSmall",                           "Your proposed upload is smaller than the minimum allowed object size.",                                                                                                                                                                                                                     "400"},
    ["1011"] = {"EntityTooLarge",                           "Your proposed upload exceeds the maximum allowed object size.",                                                                                                                                                                                                                             "400"},
    ["1012"] = {"ExpiredToken",                             "The provided token has expired.",                                                                                                                                                                                                                                                           "400"},
    ["1013"] = {"IllegalVersioningConfigurationException",  "Indicates that the versioning configuration specified in the request is invalid.",                                                                                                                                                                                                          "400"},
    ["1014"] = {"IncompleteBody",                           "You did not provide the number of bytes specified by the Content-Length HTTP header",                                                                                                                                                                                                       "400"},
    ["1015"] = {"IncorrectNumberOfFilesInPostRequest",      "POST requires exactly one file upload per request.",                                                                                                                                                                                                                                        "400"},
    ["1016"] = {"InlineDataTooLarge",                       "Inline data exceeds the maximum allowed size.",                                                                                                                                                                                                                                             "400"},
    ["1017"] = {"InternalError",                            "We encountered an internal error. Please try again.",                                                                                                                                                                                                                                       "500"},
    ["1018"] = {"InvalidAccessKeyId",                       "The AWS access key Id you provided does not exist in our records.",                                                                                                                                                                                                                         "403"},
    ["1019"] = {"InvalidAddressingHeader",                  "You must specify the Anonymous role.",                                                                                                                                                                                                                                                      "N/A"},
    ["1020"] = {"InvalidArgument",                          "Invalid Argument",                                                                                                                                                                                                                                                                          "400"},
    ["1021"] = {"InvalidBucketName",                        "The specified bucket is not valid.",                                                                                                                                                                                                                                                        "400"},
    ["1022"] = {"InvalidBucketState",                       "The request is not valid with the current state of the bucket.",                                                                                                                                                                                                                            "409"},
    ["1023"] = {"InvalidDigest",                            "The Content-MD5 you specified is not valid.",                                                                                                                                                                                                                                               "400"},
    ["1024"] = {"InvalidEncryptionAlgorithmError",          "The encryption request you specified is not valid. The valid value is AES256.",                                                                                                                                                                                                             "400"},
    ["1025"] = {"InvalidLocationConstraint",                "The specified location constraint is not valid. For more information about regions, see How to Select a Region for Your Buckets.",                                                                                                                                                          "400"},
    ["1026"] = {"InvalidObjectState",                       "The operation is not valid for the current state of the object.",                                                                                                                                                                                                                           "403"},
    ["1027"] = {"InvalidPart",                              "One or more of the specified parts could not be found. The part might not have been uploaded, or the specified entity tag might not have matched the part's entity tag.",                                                                                                                   "400"},
    ["1028"] = {"InvalidPartOrder",                         "The list of parts was not in ascending order.Parts list must specified in order by part number.",                                                                                                                                                                                           "400"},
    ["1029"] = {"InvalidPayer",                             "Your access to slave cluster is denied.",                                                                                                                                                                                                                                              "403"},
    ["1030"] = {"InvalidPolicyDocument",                    "The content of the form does not meet the conditions specified in the policy document.",                                                                                                                                                                                                    "400"},
    ["1031"] = {"InvalidRange",                             "The requested range cannot be satisfied.",                                                                                                                                                                                                                                                  "416"},
    ["1032"] = {"InvalidRequest",                           "Please use AWS4-HMAC-SHA256.",                                                                                                                                                                                                                                                              "400"},
    ["1033"] = {"InvalidRequest",                           "SOAP requests must be made over an HTTPS connection.",                                                                                                                                                                                                                                      "400"},
    ["1034"] = {"InvalidRequest",                           "S3 Transfer Acceleration is not supported for buckets with non-DNS compliant names.",                                                                                                                                                                                                       "400"},
    ["1035"] = {"InvalidRequest",                           "S3 Transfer Acceleration is not supported for buckets with periods (.) in their names.",                                                                                                                                                                                                    "400"},
    ["1036"] = {"InvalidRequest",                           "S3 Transfer Accelerate endpoint only supports virtual style requests.",                                                                                                                                                                                                                     "400"},
    ["1037"] = {"InvalidRequest",                           "S3 Transfer Accelerate is not configured on this bucket.",                                                                                                                                                                                                                                  "400"},
    ["1038"] = {"InvalidRequest",                           "S3 Transfer Accelerate is disabled on this bucket.",                                                                                                                                                                                                                                        "400"},
    ["1039"] = {"InvalidRequest",                           "S3 Transfer Acceleration is not supported on this bucket. Contact AWS Support for more information.",                                                                                                                                                                                       "400"},
    ["1040"] = {"InvalidRequest",                           "S3 Transfer Acceleration cannot be enabled on this bucket. Contact AWS Support for more information.",                                                                                                                                                                                      "400"},
    ["1041"] = {"InvalidSecurity",                          "The provided security credentials are not valid.",                                                                                                                                                                                                                                          "403"},
    ["1042"] = {"InvalidSOAPRequest",                       "The SOAP request body is invalid.",                                                                                                                                                                                                                                                         "400"},
    ["1043"] = {"InvalidStorageClass",                      "The storage class you specified is not valid.",                                                                                                                                                                                                                                             "400"},
    ["1044"] = {"InvalidTargetBucketForLogging",            "The target bucket for logging does not exist, is not owned by you, or does not have the appropriate grants for the log-delivery group.",                                                                                                                                                    "400"},
    ["1045"] = {"InvalidToken",                             "The provided token is malformed or otherwise invalid.",                                                                                                                                                                                                                                     "400"},
    ["1046"] = {"InvalidURI",                               "Couldn't parse the specified URI.",                                                                                                                                                                                                                                                         "400"},
    ["1047"] = {"KeyTooLong",                               "Your key is too long.",                                                                                                                                                                                                                                                                     "400"},
    ["1048"] = {"MalformedACLError",                        "The XML you provided was not well-formed or did not validate against our published schema.",                                                                                                                                                                                                "400"},
    ["1049"] = {"MalformedPOSTRequest",                     "The body of your POST request is not well-formed multipart/form-data.",                                                                                                                                                                                                                     "400"},
    ["1050"] = {"MalformedXML",                             "This happens when the user sends malformed xml (xml that doesn't conform to the published xsd) for the configuration. The error message is, \"The XML you provided was not well-formed or did not validate against our published schema.\"",                                                  "400"},
    ["1051"] = {"MaxMessageLengthExceeded",                 "Your request was too big.",                                                                                                                                                                                                                                                                 "400"},
    ["1052"] = {"MaxPostPreDataLengthExceededError",        "Your POST request fields preceding the upload file were too large.",                                                                                                                                                                                                                        "400"},
    ["1053"] = {"MetadataTooLarge",                         "Your metadata headers exceed the maximum allowed metadata size.",                                                                                                                                                                                                                           "400"},
    ["1054"] = {"MethodNotAllowed",                         "The specified method is not allowed against this resource.",                                                                                                                                                                                                                                "405"},
    ["1055"] = {"MissingAttachment",                        "A SOAP attachment was expected, but none were found.",                                                                                                                                                                                                                                      "N/A"},
    ["1056"] = {"MissingContentLength",                     "You must provide the Content-Length HTTP header.",                                                                                                                                                                                                                                          "411"},
    ["1057"] = {"MissingRequestBodyError",                  "This happens when the user sends an empty xml document as a request. The error message is, \"Request body is empty.\"",                                                                                                                                                                       "400"},
    ["1058"] = {"MissingSecurityElement",                   "The SOAP 1.1 request is missing a security element.",                                                                                                                                                                                                                                       "400"},
    ["1059"] = {"MissingSecurityHeader",                    "Your request is missing a required header.",                                                                                                                                                                                                                                                "400"},
    ["1060"] = {"NoLoggingStatusForKey",                    "There is no such thing as a logging status subresource for a key.",                                                                                                                                                                                                                         "400"},
    ["1061"] = {"NoSuchBucket",                           "The specified bucket or related resource does not exist.",                                                                                                                                                                                                                                                      "404"},
    ["1062"] = {"NoSuchKey",                                "The specified key does not exist.",                                                                                                                                                                                                                                                         "404"},
    ["1063"] = {"NoSuchLifecycleConfiguration",             "The lifecycle configuration does not exist.",                                                                                                                                                                                                                                               "404"},
    ["1064"] = {"NoSuchUpload",                             "The specified multipart upload does not exist. The upload ID might be invalid, or the multipart upload might have been aborted or completed.",                                                                                                                                              "404"},
    ["1065"] = {"NoSuchVersion",                            "Indicates that the version ID specified in the request does not match an existing version.",                                                                                                                                                                                                "404"},
    ["1066"] = {"NotImplemented",                           "A header you provided implies functionality that is not implemented.",                                                                                                                                                                                                                      "501"},
    ["1067"] = {"NotSignedUp",                              "Your account is not signed up for the Amazon S3 service. You must sign up before you can use Amazon S3. You can sign up at the following URL: http://aws.amazon.com/s3",                                                                                                                    "403"},
    ["1068"] = {"NoSuchBucketPolicy",                       "The specified bucket does not have a bucket policy.",                                                                                                                                                                                                                                       "404"},
    ["1069"] = {"OperationAborted",                         "A conflicting conditional operation is currently in progress against this resource. Try again.",                                                                                                                                                                                            "409"},
    ["1070"] = {"PermanentRedirect",                        "The bucket you are attempting to access must be addressed using the specified endpoint. Send all future requests to this endpoint.",                                                                                                                                                        "301"},
    ["1071"] = {"PreconditionFailed",                       "At least one of the preconditions you specified did not hold.",                                                                                                                                                                                                                             "412"},
    ["1072"] = {"Redirect",                                 "Temporary redirect.",                                                                                                                                                                                                                                                                       "307"},
    ["1073"] = {"RestoreAlreadyInProgress",                 "Object restore is already in progress.",                                                                                                                                                                                                                                                    "409"},
    ["1074"] = {"RequestIsNotMultiPartContent",             "Bucket POST must be of the enclosure-type multipart/form-data.",                                                                                                                                                                                                                            "400"},
    ["1075"] = {"RequestTimeout",                           "Your socket connection to the server was not read from or written to within the timeout period.",                                                                                                                                                                                           "400"},
    ["1076"] = {"RequestTimeTooSkewed",                     "The difference between the request time and the server's time is too large.",                                                                                                                                                                                                               "403"},
    ["1077"] = {"RequestTorrentOfBucketError",              "Requesting the torrent file of a bucket is not permitted.",                                                                                                                                                                                                                                 "400"},
    ["1078"] = {"SignatureDoesNotMatch",                    "The request signature we calculated does not match the signature you provided. Check your AWS secret access key and signing method. For more information, see REST Authentication and SOAP Authentication for details.",                                                                    "403"},
    ["1079"] = {"ServiceUnavailable",                       "Reduce your request rate.",                                                                                                                                                                                                                                                                 "503"},
    ["1080"] = {"SlowDown",                                 "Reduce your request rate.",                                                                                                                                                                                                                                                                 "503"},
    ["1081"] = {"TemporaryRedirect",                        "You are being redirected to the bucket while DNS updates.",                                                                                                                                                                                                                                 "307"},
    ["1082"] = {"TokenRefreshRequired",                     "The provided token must be refreshed.",                                                                                                                                                                                                                                                     "400"},
    ["1083"] = {"TooManyBuckets",                           "You have attempted to create more buckets than allowed.",                                                                                                                                                                                                                                   "400"},
    ["1084"] = {"UnexpectedContent",                        "This request does not support content.",                                                                                                                                                                                                                                                    "400"},
    ["1085"] = {"UnresolvableGrantByEmailAddress",          "The email address you provided does not match any account on record.",                                                                                                                                                                                                                      "400"},
    ["1086"] = {"UserKeyMustBeSpecified",                   "The bucket POST must contain the specified field name. If it is specified, check the order of the fields.",                                                                                                                                                                                 "400"},
    ["1087"] = {"UnprocessableEntity",                      "unable to process the request entity contained instructions, For example, etag not match",                                                                                                                                                                                                  "422"},
    ["1088"] = {"NoSuchUid",                                "The specified uid does not exist.",                                                                                                                                                                                                                                                         "404"},
    ["1089"] = {"AlreadyExists",                            "The requested resource is existed and conflict with the new request.",                                                                                "409"},
    ["1090"] = {"InvalidUidType",                        "Invalid uid type,please fill a name string to the args uid.",              "400"},
    ["1091"] = {"MissingUid",                            "Your request is missing uid arg",                                                                                                                                                                                                                                                "400"},
    ["1092"] = {"ExceedQuota",                            "size or number of the objects exceed the user or bucket quota",           "403"},
    ["1093"] = {"NoResponse",                             "actual data size not enough for the request length",           "444"},
}

-- low layer common 1000~1099, hbase 1100~1999, rgw 2000~2999, redis 5000~5999  
_M.LowLayerCommon = {
    ["1001"] = "Invalid arguments",
    ["1002"] = "Failed to get lock",
    ["1003"] = "Nginx regular expression error",
}
_M.HbaseError = {
    --thrift interface
    ["1100"] = "Invalid arguments for hbase operation", --makeTGet
    ["1101"] = "Failed to create thrift hbase client", --dequeueclient
    ["1102"] = "HBase Thrift client get failed", --client.get
    ["1103"] = "HBase Thrift client increment failed",
    ["1104"] = "HBase Thrift client put failed",
    ["1105"] = "HBase Thrift client checkAndPut failed",
    ["1106"] = "HBase Thrift client delete failed",
    ["1107"] = "HBase Thrift client checkAndDelete failed",
    ["1108"] = "HBase Thrift client mutateRow failed",
    ["1109"] = "HBase op not allowed for non-COUNTER value",
    ["1110"] = "HBase op not allowed for COUNTER value",
    ["1111"] = "HBase Thrift client openScanner failed",
    ["1112"] = "HBase Thrift client closeScanner failed",
    ["1113"] = "HBase Thrift client scan failed, unknown",
    ["1114"] = "HBase Thrift client scan failed, Invalid scanner Id",
    ["1115"] = "HBase Thrift client quickScan failed",
    ["1116"] = "failed to open Thrit transport", --client.open in dequeue_client func
    ["1117"] = "HBase Thrift enqueueclient error",--todo
    ["1118"] = "HBase Thrift client getAllRegionLocations failed, unknown",
    ["1119"] = "HBase Thrift client checkAndMutateRow failed",
    ["1120"] = "HBase Thrift client checkAndMutateAndGetRow failed",
    ["1121"] = "HBase Thrift client condition check failed for checkAnd* operation",
}

_M.RgwError = {
    --code
    --message
    ["2001"] = "resty http failed to connect",
    ["2002"] = "Initialization parameter error",
    ["2003"] = "you must call rgw create_conn first",
    ["2004"] = "request pipeline failed",
    ["2005"] = "request pipeline failed. responses are empty",
    ["2006"] = "request pipeline failed. response of request i is nil",
    ["2007"] = "request pipeline failed. response of request status or body error (no need to retry)",
    ["2008"] = "request pipeline failed. timed out (need to retry)",
    ["2009"] = "rgw returned nil res",
    ["2010"] = "rgw returned http code error",
    ["2011"] = "failed to create resty http for rgw operations",
    ["2012"] = "capture returned nil when put data into rgw",
    ["2013"] = "capture returned nil when get data from rgw",
    ["2014"] = "capture didn't return 200 when put data into rgw",
    ["2015"] = "capture didn't return 200 or 206 when get data from rgw",
    ["2016"] = "capture returned truncated data",
}

_M.RedisError = {
    ["5000"] = "redis cmd 'get' failed",
    ["5001"] = "redis cmd 'hget' failed",
    ["5002"] = "redis cmd 'set' failed",
    ["5003"] = "redis cmd 'hset' failed",
    ["5004"] = "redis cmd 'del' failed",
    ["5005"] = "redis cmd 'hdel' failed",
    ["5006"] = "redis cmd 'mget' failed",
    ["5007"] = "redis cmd 'hmget' failed",
    ["5008"] = "redis cmd 'mset' failed",
    ["5009"] = "redis cmd 'hmset' failed",
    ["5010"] = "redis cmd 'incr' failed",
    ["5011"] = "redis cmd 'incrby' failed",
    ["5012"] = "redis cmd 'expire' failed",
    ["5013"] = "redis cmd 'getset' failed",
    ["5014"] = "redis cmd 'eval' failed",
    ["5015"] = "redis eval a script: script failed",
    ["5016"] = "redis cmd 'evalsha' failed",
    ["5017"] = "redis evalsha a script: script failed",
}

--  common 0001~0999 Auth 1000~1999 、bucket 2000~2999 、objcet 3000~39999、 multiupload 4000~4999

--multiupload 4000~4999
_M.ProxyError = {
    --code ={
    --s3Code
    --message
    --}
    -- common
    ["0010"] = {"1020", "Initialization parameter error"},
    ["0011"] = {"1017", "Initialization parameter error. InternalError"},
    ["0012"] = {"1061", "no bucket"},
    ["0014"] = {"1017", "httpc get_client_body_reader error"},
    ["0015"] = {"1017", "failed to generated tag"},
    ["0016"] = {"1017", "read header from http body error"},
    ["0017"] = {"1017", "read chunks from http body error"},
    ["0018"] = {"1054", "method not allowed"},
    ["0019"] = {"1031", "request range invalid"},
    ["0020"] = {"1075", "read request timeout"},
    ["0021"] = {"1001", "Access Denied"},
    ["0022"] = {"1056", "Missing ContentLength"},
    ["0023"] = {"1040", "InvalidRequest"},
    ["0024"] = {"1050", "xml is error"},
    ["0025"] = {"1017", "json encode failed"},
    ["0026"] = {"1069", "operation is outdated"},
    ["0027"] = {"1069", "operation is in conflict with another one"},
    ["0028"] = {"1030", "Request parameter error"},
    
    --Auth
    ["1001"] = {"1076", "request time and the server's time is too large"},
    ["1002"] = {"1078", "SignatureDoesNotMatch, The request signature we calculated does not match the signature you provided."},
    ["1003"] = {"1054", "s4 Signaturen Method Not Allowed "},
    ["1004"] = {"1018", "access key does not exist. "}, 
    ["1005"] = {"1023", "MD5 invalid"},
    ["1006"] = {"1001", "MD5 is nil"},
    ["1007"] = {"1029", "request denied in slave cluster"},
    
    --bucket 2000~2999
    ["2001"] = {"1017", "delete bucket, failed to get bucket meta"},
    ["2002"] = {"1007", "delete bucket, BucketNotEmpty"},
    ["2003"] = {"1017", "put bucket to metadb failed"},
    ["2004"] = {"1017", "get bucket from metadb error"},
    ["2005"] = {"1017", "search objects list from metadb error"},
    ["2006"] = {"1005", "bucket already exists"},
    ["2007"] = {"1021", "The specified bucket is not valid."},
    ["2008"] = {"1017", "put bucket lifecycle to metadb failed"},
   
    --object 3000~3999
    ["3001"] = {"1062", "no such object"},
    ["3002"] = {"1017", "get object from metadb error"},
    ["3003"] = {"1017", "object operation rgw create_conn failed"},
    ["3004"] = {"1017", "rgw put object failed"},
    ["3005"] = {"1014", "Data total size doesn't match the content-length"},
    ["3006"] = {"1004", "computed etag doesn't match the supplied content-md5"},
    ["3007"] = {"1017", "metadb put object error"},
    ["3008"] = {"1017", "get hdata from hbase invalid"},
    ["3009"] = {"1017", "get data from rgw error or unexpected results"},
    ["3010"] = {"1093", "length of data read from rgw not match the metadata length"},
    ["3011"] = {"1017", "computed etag doesn't match the metadata etag"},
    ["3012"] = {"1017", "delete old object failed"},
    ["3013"] = {"1056", "MissingContentLength OR Transferencoding"},
    ["3014"] = {"1017", "put obj write datalog faile"},
    ["3015"] = {"1017", "copy obj write datalog faile"},
    ["3016"] = {"1017", "read data from stream failed"},
    ["3017"] = {"1046", "failed parse the object name"},
    ["3018"] = {"1017", "create ancestor dirs failed"},
    ["3019"] = {"1017", "failed to send data to client"},

    --multiupload 4000~4999
    ["4001"] = {"1061", "Failed to get bucket etag"},
    ["4002"] = {"1017", "init upload put metadb failed"},
    ["4003"] = {"1017", "init multipart upload resty uuid failed"},
    ["4004"] = {"1017", "Initialization parameter error"},
    ["4005"] = {"1064", "failed to get temp_object flag"},
    ["4006"] = {"1017", "upload part objcet flag is nil or \'\'"},
    ["4007"] = {"1064", "this upload has been completed or aborted"},
    ["4009"] = {"1017", "upload part to rgw failed"},
    ["4010"] = {"1017", "upload part put into metadb failed"},
    ["4011"] = {"1017", "failed to get all part info"},
    ["4012"] = {"1027", "part info is failed"},
    ["4013"] = {"1017", "complete multipart upload, metadb put failed"},
    ["4014"] = {"1014", "Data total size doesn't match the content-length"},
    ["4015"] = {"1087", "computed etag doesn't match the supplied content-md5"},
    ["4016"] = {"1010", "multipart part data leng is too short"},
    ["4017"] = {"1017", "complete multipart delete temp table failed"},
    ["4018"] = {"1017", "multipart complete delete old object failed"},
    ["4019"] = {"1050", "multipart complete xml is error"},
    -- user admin
    ["4021"] = {"1062", "get non-exister userinfo by accessid  from user table"},
    ["4022"] = {"1017", "failed to encode info"},
    ["4023"] = {"1088", "specified uid does not exist when get, delete userinfo"},
    ["4024"] = {"1091", "not specify user"},
    ["4025"] = {"1090", "should supply the uid name when operate on user,non-string type"},
    ["4026"] = {"1017", "fail to deleterow in hbase"},
    ["4028"] = {"1017", "fail to putrow in hbase"},
    ["4027"] = {"1089", "The requested resource is existed and conflict with the new request"},
    ["4029"] = {"1017", "fail to open urandom"},
    ["4030"] = {"1017", "fail to getrow in hbase"},
    ["4031"] = {"1061", "bucket doesnot exist"},
    ["4034"] = {"1061", "bucket have deleted"},
    ["4032"] = {"1088", "get non-exister userinfo by uid  from userid table"},
    ["4033"] = {"1054", "op not supported"},
    ["4034"] = {"1017", "fail to openscanner in hbase"},
    ["4035"] = {"1017", "fail to scann in hbase"},
    ["4036"] = {"1020", "inavalid para"},
    ["4037"] = {"1017", "peer op fail"},
    ["4038"] = {"1017", "fail to get clusterinfo"},
    ["4039"] = {"1017", "get clusterinfo fail"},
    ["4040"] = {"1017", "key create fail,please retry"},
    ["4041"] = {"1017", "write meta log failed"},

    ["4050"] = {"1017", "get part info failed"},
    ["4051"] = {"1017", "delete part info failed"},
    
    -- admin
    -- admin rsync 5100~5199
    ["5100"] = {"1020", "rsync log param is nil"},
    ["5101"] = {"1017", "rsync log scan db error"},

    -- quota
    ["6100"] = {"1092",  "quota exceed"}, 

    -- ceph clusters admin 
    ["6200"] = {"1020", "'id' is missing for admin/ceph/* operation"},
    ["6201"] = {"1020", "'state' is invliad for admin/ceph/* operation, only 'OK', 'WARN' or 'ERR' is allowed"},
    ["6202"] = {"1020", "'weight' is invalid for admin/ceph/* operation, only 0 or positive number is allowed"},
    ["6203"] = {"1017", "failed to check if ceph exists or not"},
    ["6204"] = {"1017", "failed to create ceph due to hbase failure"},
    ["6205"] = {"1017", "failed to scan table 'ceph' due to hbase failure"},
    ["6206"] = {"1062", "the specified ceph does not exist"},
    ["6207"] = {"1017", "failed to scan table 'rgw' due to hbase failure"},
    ["6208"] = {"1020", "ceph cluster does not exist, cannot modify it"},
    ["6209"] = {"1017", "failed to modify ceph due to hbase failure"},
    ["6210"] = {"1020", "you must modify at least one of 'state' and 'weight'"},
    ["6211"] = {"1020", "'from' is invalid for admin/ceph/get, only 'hbase' or 'memory' is allowed"},
    ["6212"] = {"1020", "'cid' is missing for admin/rgw/* operation"},
    ["6213"] = {"1020", "'server' is missing for admin/rgw/* operation"},
    ["6214"] = {"1020", "'port' is missing for admin/rgw/* operation"},
    ["6215"] = {"1020", "cannot add rgw because the hosting ceph does not exist"},
    ["6216"] = {"1017", "failed to add rgw, due to hbase failure"},
    ["6217"] = {"1017", "failed to delete rgw, due to hbase failure"},

    __index = {"1017", "Unknown internel error"},

}

local mt = {
    __index = function(table, key)
        return {"1017", "Unknown internel error"}
    end
}

setmetatable(_M.ProxyError, mt)

return _M
