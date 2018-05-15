//package main
//
//import "fmt"
//
//func main() {
//	fmt.Println("vim-go")
//}
package bktobj

import (
	. "github.com/inevity/s3go/internal"

	"fmt"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/corehandlers"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type boop struct {
	bucket string
	key    string
}

func DoBktObj(acckey string, seckey string, server string, bucket string, key string, n int, m int) {
	do := boop{
		bucket: bucket,
		key:    key,
	}
	fmt.Print("n:", n)
	if n == 0 && m == 0 {
		do.Onebktobj(acckey, seckey, bucket, key, server)
		// need improve ,use the member of boop
	}
}

//func (ops *boop) Onebktobj(sbucket string, skey string) error {
func (ops *boop) Onebktobj(acckey string, seckey string, sbucket string, skey string, server string) {
	fmt.Println("on Onebktobj!!!!")
	bucket := aws.String(sbucket)
	//	// no need append /.
	key := aws.String(skey)

	// Configure to use dnion Server
	s3Config := &aws.Config{
		Credentials: credentials.NewStaticCredentials(acckey, seckey, ""),
		//		Endpoint:         aws.String("http://192.168.56.101:6081"),
		Endpoint:         aws.String(server),
		Region:           aws.String("us-east-1"),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
	}
	newSession := session.New(s3Config)

	s3Client := s3.New(newSession)
	s3Client.Handlers.Sign.Clear()
	s3Client.Handlers.Sign.PushBack(SignV2) // SignV2 from interal pacakge 's file code
	s3Client.Handlers.Sign.PushBackNamed(corehandlers.BuildContentLengthHandler)

	cparams := &s3.CreateBucketInput{
		Bucket: bucket, // Required
	}

	// Create a new bucket using the CreateBucket call.
	_, err := s3Client.CreateBucket(cparams)
	if err != nil {
		// Message from an error.
		fmt.Println(err.Error())
		return
	}

	// Upload a new object "testobject" with the string "Hello World!" to our "newbucket".
	_, err = s3Client.PutObject(&s3.PutObjectInput{
		Body:   strings.NewReader("Hello from Minio!!"),
		Bucket: bucket,
		Key:    key,
	})
	if err != nil {
		fmt.Printf("Failed to upload data to %s/%s, %s\n", *bucket, *key, err.Error())
		return
	}
	fmt.Printf("Successfully created bucket %s and uploaded data with key %s\n", *bucket, *key)

	// Retrieve our "testobject" from our "newbucket" and store it locally in "testobject_local".
	file, err := os.Create("testobject_local")
	if err != nil {
		fmt.Println("Failed to create file", err)
		return
	}
	defer file.Close()

	//downloader := s3manager.NewDownloader(newSession)
	downloader := s3manager.NewDownloaderWithClient(s3Client)

	numBytes, err := downloader.Download(file,
		&s3.GetObjectInput{
			Bucket: bucket,
			Key:    key,
		})
	if err != nil {
		fmt.Println("Failed to download file", err)
		return
	}
	//	downloader := s3manager.NewDownloader(newSession)
	//	numBytes, err := downloader.Download(file,
	//		&s3.GetObjectInput{
	//			Bucket: bucket,
	//			Key:    key,
	//		})
	//	if err != nil {
	//		fmt.Println("Failed to download file", err)
	//		return
	//	}
	fmt.Println("Downloaded file", file.Name(), numBytes, "bytes")
}
