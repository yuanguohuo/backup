package main

import (
	"bytes"
	//	"errors"
	"fmt"
	"github.com/docopt/docopt-go"
	"github.com/smartystreets/go-aws-auth"
	"github.com/verdverm/frisby"
	"log"
	"net/http"
	//	"net/http/httputil"
	//	"github.com/aws/aws-sdk-go/aws"
	"github.com/bitly/go-simplejson"
	. "github.com/inevity/s3go/bktobj"
)

const usage = `s3apitests  manage cli.
Usage:
  s3apitests ls [<pattern>]
  s3apitests upload <file> <path>
  s3apitests download <path> <file>
  s3apitests rm <path>
  s3apitests pack <pattern> <file> [--key=<id>] [--context=<k1=v2,k2=v2>]
  s3apitests unpack <file> <path> [--context=<k1=v2,k2=v2>]
  s3apitests rotate [<pattern>]
  s3apitests version
  s3apitests --uid=<name>
Options:
  -h --help  Show this help information.
Environment Variables:
  ROOT_AKEY      The KMS key to use when encrypting secrets.
  ROOT_SKEY      Secret key 
  S3_PATH         Where secrets will be stored (e.g. s3://bucket/path).
`

var (
	version   = "v1"       // version of sneaker
	goVersion = "v1.8.3"   // version of go we build with
	buildTime = "20170718" // time of build
	//server    = "http://121.14.254.234"
	server  = "http://192.168.56.101"
	admport = ":6080"
	//	b         = "http://192.168.56.101:6080/admin/user?uid="
	b = server + admport + "/admin/user?uid="
	//	bkturl    = "http://192.168.56.101:6080/admin/bucket"
	bkturl = server + admport + "/admin/bucket"

//	url =
)

// all admin user use the root accessid to auth

//argument maybe interface.
//func createuser(uid string) (acckey string, seckey string) {
func createuser(uid interface{}) (suid string, acckey string, seckey string, F *frisby.Frisby) {
	var url string
	var user string
	//	var b string
	//if s, ok := args["--uid"].(string); ok {
	if s, ok := uid.(string); ok {
		url = s
		url = b + url + "&quota-type=user&max-size-kb=10000000&max-objects=10000&enabled=-1"
		user = s
	} else {
		//url = "http://192.168.56.101:6080/admin/user?uid=uuuuuuu8&quota-type=user&max-size-kb=10000000&max-objects=10000&enabled=-1"
		url = server + admport + "/admin/user?uid=uuuuuuu8&quota-type=user&max-size-kb=10000000&max-objects=10000&enabled=-1"
	}
	req, _ := http.NewRequest("PUT", url, nil)

	awsauth.SignS3(req, awsauth.Credentials{
		AccessKeyID:     "1WYCCJZ9JRLWZU8JTDQJ",
		SecretAccessKey: "PXhbQDJVeF1PsXw5tsCuIaKY0N8s1BP2J3yCn9K3",
		//	SecurityToken: "Security Token",	// STS (optional)
	}) // Automatically chooses the best signing mechanism for the service

	F = frisby.Create("Test successful user create").Put(url)
	for k, vv := range req.Header {
		for _, n := range vv {
			F.SetHeader(k, n) //concact or first
		}

	}
	F.SetHeader("Content-Type", "").Send().ExpectStatus(200).ExpectContent("keys").ExpectJson("0.keys.user", user)

	F.PrintBody()
	var ok error
	//	var ok error
	//err handling
	//if F.AfterContent Error
	F.AfterJson(func(F *frisby.Frisby, json *simplejson.Json, err error) {
		// nil deference avoid?
		if acckey, ok = json.GetIndex(0).Get("keys").Get("access_key").String(); ok != nil {
			fmt.Println(ok)
			//exit
		}

		if seckey, ok = json.GetIndex(0).Get("keys").Get("secret_key").String(); ok != nil {
			fmt.Println(ok)
		}

		fmt.Println("acckey:", acckey)
		fmt.Println("seckey:", seckey)
	})
	suid = user // must need?
	return suid, acckey, seckey, F

}
func deluser(uid string) (gF *frisby.Frisby) {
	var url string

	if uid != "" {
		url = b + uid
	}
	fmt.Println(url)

	greq, _ := http.NewRequest("DELETE", url, nil)

	awsauth.SignS3(greq, awsauth.Credentials{
		AccessKeyID:     "1WYCCJZ9JRLWZU8JTDQJ",
		SecretAccessKey: "PXhbQDJVeF1PsXw5tsCuIaKY0N8s1BP2J3yCn9K3",
		//	SecurityToken: "Security Token",	// STS (optional)
	}) // Automatically chooses the best signing mechanism for the service

	gF = frisby.Create("Test successful delete user").Delete(url)
	for k, vv := range greq.Header {
		for _, n := range vv {
			gF.SetHeader(k, n) //concact or first
		}
	}
	gF.SetHeader("Content-Type", "").Send().ExpectStatus(200)
	gF.PrintBody()
	return gF
}
func getuserinfo(uid string) (gF *frisby.Frisby) {
	var url string

	if uid != "" {
		url = b + uid
	}
	fmt.Println(url)

	greq, _ := http.NewRequest("GET", url, nil)

	awsauth.SignS3(greq, awsauth.Credentials{
		AccessKeyID:     "1WYCCJZ9JRLWZU8JTDQJ",
		SecretAccessKey: "PXhbQDJVeF1PsXw5tsCuIaKY0N8s1BP2J3yCn9K3",
		//	SecurityToken: "Security Token",	// STS (optional)
	}) // Automatically chooses the best signing mechanism for the service

	gF = frisby.Create("Test successful get userinfo").Get(url)
	for k, vv := range greq.Header {
		for _, n := range vv {
			gF.SetHeader(k, n) //concact or first
		}
	}
	gF.SetHeader("Content-Type", "").Send().ExpectStatus(200).ExpectContent("keys").ExpectJson("0.keys.user", uid)
	gF.PrintBody()
	return gF
}
func getuserstats(uid string, tocheck string, value interface{}, bucket string, name string) (gF *frisby.Frisby) {

	var url string

	if uid != "" {
		url = bkturl + "?uid=" + uid
		if bucket != "" {
			url = url + "&bucket=" + bucket
		}
	} else {
		url = bkturl
	}

	fmt.Println(url)
	req, _ := http.NewRequest("GET", url, nil)

	awsauth.SignS3(req, awsauth.Credentials{
		AccessKeyID:     "1WYCCJZ9JRLWZU8JTDQJ",
		SecretAccessKey: "PXhbQDJVeF1PsXw5tsCuIaKY0N8s1BP2J3yCn9K3",
		//	SecurityToken: "Security Token",	// STS (optional)
	}) // Automatically chooses the best signing mechanism for the service

	//gF = frisby.Create("Test successful get userstats").Get(url)
	gF = frisby.Create(name).Get(url)
	for k, vv := range req.Header {
		for _, n := range vv {
			gF.SetHeader(k, n) //concact or first
		}

	}
	if uid != "" {
		if bucket != "" {
			gF.SetHeader("Content-Type", "").Send().ExpectStatus(200).ExpectJson(tocheck, value) // this one object have put
		} else {
			gF.SetHeader("Content-Type", "").Send().ExpectStatus(200).ExpectJson(tocheck, value) // this one object have put
		}
	} else {
		gF.SetHeader("Content-Type", "").Send().ExpectStatus(200)
	}

	//	// json only one item,this two item.need be index 1.
	gF.PrintBody()
	return gF
}
func main() {
	//	fmt.Println("Frisby!\n")
	args, err := docopt.Parse(usage, nil, true, version, false)
	if err != nil {
		log.Fatal(err)
	}
	var buf bytes.Buffer
	logger := log.New(&buf, "logger: ", log.Lshortfile)
	logger.Print("uid args") //why not output to standard console
	logger.Print("uid args: %s", args["--uid"])
	fmt.Println("buf:", buf)

	if args["version"] == true {
		fmt.Printf(
			"version: %s\ngoversion: %s\nbuildtime: %s\n",
			version, goVersion, buildTime,
		)
		return
	}
	//	manager := loadManager()
	//https://github.com/codahale/sneaker/blob/master/cmd/sneaker/main.go
	//cli write

	// create user,using this acckey as admin key
	user, acckey, seckey, F := createuser(args["--uid"])

	// create bucket, and create object.
	// get above use acc and sec key ,then put bucket and object.
	var dataport = ":6081"
	DoBktObj(acckey, seckey, server+dataport, "newbucket9", "testobject9", 0, 0)

	//create another user,check user stats then put bucket,check user stats,put n object ,the check user stats.
	var emptyuser string
	if user != "" {
		emptyuser = user + "1"
	}
	user1, _, _, eF := createuser(emptyuser)

	//get userinfo

	gF := getuserinfo(user)

	// get user stats test for one object!
	// todo: abstratt this ,(method,url,accessid/key,testname,set_header)
	// "http://{{ ipontest }}:6080/admin/bucket?uid={{ item }}"
	gF = getuserstats(user, "2.user_usage.objects", 1, "", "test user stats ,one object")

	// get user stats test for no bucket
	// todo: abstratt this ,(method,url,accessid/key,testname,set_header)
	// "http://{{ ipontest }}:6080/admin/bucket?uid={{ item }}"
	gF = getuserstats(user1, "1.user_usage.objects", 0, "", "test user stats,no bucket")
	gF = deluser(user1)

	// test all userstats.
	gF = getuserstats("", "", 0, "", "test alluserstats")

	// test a bucket stats by user !

	// need test bucket not exisit,bucket is empty,bucket have objects.
	////	http://{{ ipontest }}:6080/admin/bucket?uid={{ item }}&bucket=bucket1"
	//	b = "http://192.168.56.101:6080/admin/bucket"
	//
	//	if user != "" {
	//		url = b + "?uid=" + user +"&bucket=newbucket"
	//	}
	gF = getuserstats(user, "0.usage.objects", 1, "newbucket9", "test stats by user by bucket")
	//	gF = getuserstats(user, "1.user_usage.objects", 0, "newbucket9", "test stats by user by bucket")

	//todo: test bucket quota?

	frisby.Global.PrintReport()

	errs := F.Errors()
	for _, e := range errs {
		fmt.Println("Error: ", e)
	}
	errs1 := gF.Errors()
	for _, e := range errs1 {
		fmt.Println("Error: ", e)
	}
	errs2 := eF.Errors()
	for _, e := range errs2 {
		fmt.Println("Error: ", e)
	}

}
