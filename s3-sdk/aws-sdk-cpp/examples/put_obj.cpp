#include <unistd.h>

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <iostream>
#include <fstream>

void usage()
{
  std::cout<<"Usage: put_obj -a {addr} -b {bucket} -o {object} -f {local-file} -h"<<std::endl;
}

int main(int argc, char** argv)
{
  Aws::String addr;
  Aws::String bucket;
  Aws::String object;
  Aws::String file;

  int c;
  while ((c = getopt (argc, argv, "a:b:o:f:h")) != -1)
  {
    switch(c)
    {
      case 'a':
        addr = optarg;
        break;
      case 'b':
        bucket = optarg;
        break;
      case 'o':
        object = optarg;
        break;
      case 'f':
        file = optarg;
        break;
      case 'h':
        usage();
        exit(0);
        break;
      case '?':
        usage();
        exit(1);
        break;
    }
  }

  if (addr.size()==0 || bucket.size()==0 || object.size()==0 || file.size()==0)
  {
    std::cout<<"Missing argument."<<std::endl;
    usage();
    exit(1);
  }

  std::cout << "Putting " << file << " to S3 bucket '" << bucket << "' object '" << object << "'" << std::endl;

  Aws::SDKOptions options;
  Aws::InitAPI(options);

  {
    Aws::Client::ClientConfiguration conf;
    conf.proxyHost = addr;
    conf.proxyPort = 6081;
    conf.scheme = Aws::Http::Scheme::HTTP;   //default is HTTPS;
    conf.endpointOverride = "s3.dnion.com";

    Aws::S3::S3Client s3_client(conf,2);     //2 menas aws signature version 2;

    Aws::S3::Model::PutObjectRequest s3_request;
    s3_request.SetBucket(bucket);
    s3_request.SetKey(object);

    auto input_data = Aws::MakeShared<Aws::FStream>("PutObjectInputStream", file.c_str(), std::ios_base::in);

    s3_request.SetBody(input_data);

    auto put_object_outcome = s3_client.PutObject(s3_request);

    if (put_object_outcome.IsSuccess()) 
    {
      std::cout << "Success" << std::endl;
    }
    else 
    {
      std::cout << "Failure: " <<
        put_object_outcome.GetError().GetExceptionName() << " " <<
        put_object_outcome.GetError().GetMessage() << std::endl;
    }
  }

  Aws::ShutdownAPI(options);
  return 0;
}
