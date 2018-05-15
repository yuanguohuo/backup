//package main
//
//import "fmt"
//
//func main() {
//	fmt.Println("vim-go")
//}
package main

import (
	"bytes"
	"fmt"
	"github.com/docopt/docopt-go"
	"github.com/smartystreets/go-aws-auth"
	//	"github.com/verdverm/frisby"
	"log"
	"net/http"
	"net/http/httputil"
)

//const usage = `usage`
//sneaker rm <path>
const usage = `s3  manage cli.
Usage:
  s3 ls [<pattern>]
  s3 upload <file> <path>
  s3 download <path> <file>
  s3 rm <path>
  s3 pack <pattern> <file> [--key=<id>] [--context=<k1=v2,k2=v2>]
  s3 unpack <file> <path> [--context=<k1=v2,k2=v2>]
  s3 rotate [<pattern>]
  s3 version
  s3 --uid=<name>
Options:
  -h --help  Show this help information.
Environment Variables:
  ROOT_AKEY      The KMS key to use when encrypting secrets.
  ROOT_SKEY      Secret key 
  S3_PATH         Where secrets will be stored (e.g. s3://bucket/path).
`

//version := 1
//goVersion := 1.8.3

//buildTime := 2014
var (
	version   = "v1"       // version of sneaker
	goVersion = "v1.8.3"   // version of go we build with
	buildTime = "20170718" // time of build
)

func main() {
	args, err := docopt.Parse(usage, nil, true, version, false)
	if err != nil {
		log.Fatal(err)
	}
	var buf bytes.Buffer
	logger := log.New(&buf, "logger: ", log.Lshortfile)
	logger.Print("uid args") //why not output to standard console
	logger.Print("uid args: %s", args["--uid"])
	//	fmt.Printf("uid args")
	//	fmt.Printf("uid args: %s", args["--uid"])

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
	var url string
	if s, ok := args["--uid"].(string); ok {
		url = s
		var b string
		b = "http://192.168.56.101:6080/admin/user?uid="
		url = b + url + "&quota-type=user&max-size-kb=10000000&max-objects=10000&enabled=-1"
	} else {
		url = "http://192.168.56.101:6080/admin/user?uid=uuuuuuu8&quota-type=user&max-size-kb=10000000&max-objects=10000&enabled=-1"
	}
	//	if args["--uid"] != nil {
	//		var s string
	//		s = "http://192.168.56.101:6080/admin/user?uid="
	//		url = s + args["--uid"].(string) + "&quota-type=user&max-size-kb=10000000&max-objects=10000&enabled=-1"
	//
	//	} else {
	//		url = "http://192.168.56.101:6080/admin/user?uid=uuuuuuu8&quota-type=user&max-size-kb=10000000&max-objects=10000&enabled=-1"
	//	}

	client := new(http.Client)

	//req, err := http.NewRequest("PUT", url, nil)
	req, _ := http.NewRequest("PUT", url, nil)

	//modify req
	//awsauth.Sign(req, awsauth.Credentials{
	awsauth.SignS3(req, awsauth.Credentials{
		AccessKeyID:     "JTLA6O1TF69Z0YB4I7O1",
		SecretAccessKey: "cwoNbM7TZLxYeMcmQxfiwL7n4Pv0JhPRlNG6m1dq",
		//	SecurityToken: "Security Token",	// STS (optional)
	}) // Automatically chooses the best signing mechanism for the service
	requestDump, err := httputil.DumpRequest(req, true)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(string(requestDump))

	//resp, err := client.Do(req)
	_, err1 := client.Do(req)
	if err1 != nil {
		return
	}
}
