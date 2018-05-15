//package main
//
//import "fmt"
//
//func main() {
//	fmt.Println("vim-go")
//}
package main

import (
	. "github.com/inevity/s3go/internal"

	"fmt"
	//	"os"
	//	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/corehandlers"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	//	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func main() {
	bucket := aws.String("admin")
	// no need append /.
	key := aws.String("user")

	// Configure to use dnion Server
	s3Config := &aws.Config{
		Credentials:      credentials.NewStaticCredentials("8C9TU7JU9OL1TMGUD7MC", "ZTydkPh5819CwoXy7rteSBeRRqjAAS2Fw8t25jTU", ""),
		Endpoint:         aws.String("http://192.168.56.101:6080"),
		Region:           aws.String("us-east-1"),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
	}
	newSession := session.New(s3Config)

	s3Client := s3.New(newSession)
	s3Client.Handlers.Sign.Clear()
	s3Client.Handlers.Sign.PushBack(SignV2) // SignV2 from interal pacakge 's file code
	s3Client.Handlers.Sign.PushBackNamed(corehandlers.BuildContentLengthHandler)

	//	cparams := &s3.CreateBucketInput{
	//		Bucket: bucket, // Required
	//	}
	//
	//	// Create a new bucket using the CreateBucket call.
	//	_, err := s3Client.CreateBucket(cparams)
	//	if err != nil {
	//		// Message from an error.
	//		fmt.Println(err.Error())
	//		return
	//	}
	//
	// Upload a new object "testobject" with the string "Hello World!" to our "newbucket".
	// create user op ,how
	_, err := s3Client.PutObject(&s3.PutObjectInput{
		Body:   nil,
		Bucket: bucket,
		Key:    key,
		Uid:    aws.String("uuuuuuuuuuuuuuuu"),
	})
	if err != nil {
		fmt.Printf("Failed to creat user to %s/%s, %s\n", *bucket, *key, err.Error())
		return
	}
	fmt.Printf("Successfully created user %s and uploaded data with key %s\n", *bucket, *key)

	//	// Retrieve our "testobject" from our "newbucket" and store it locally in "testobject_local".
	//	file, err := os.Create("testobject_local")
	//	if err != nil {
	//		fmt.Println("Failed to create file", err)
	//		return
	//	}
	//	defer file.Close()

	//	//downloader := s3manager.NewDownloader(newSession)
	//	downloader := s3manager.NewDownloaderWithClient(s3Client)
	//
	//	numBytes, err := downloader.Download(file,
	//		&s3.GetObjectInput{
	//			Bucket: bucket,
	//			Key:    key,
	//		})
	//	if err != nil {
	//		fmt.Println("Failed to download file", err)
	//		return
	//	}
	//	//	downloader := s3manager.NewDownloader(newSession)
	//	//	numBytes, err := downloader.Download(file,
	//	//		&s3.GetObjectInput{
	//	//			Bucket: bucket,
	//	//			Key:    key,
	//	//		})
	//	//	if err != nil {
	//	//		fmt.Println("Failed to download file", err)
	//	//		return
	//	//	}
	//	fmt.Println("Downloaded file", file.Name(), numBytes, "bytes")
}
