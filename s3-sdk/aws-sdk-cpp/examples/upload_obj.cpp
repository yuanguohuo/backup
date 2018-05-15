#include <unistd.h>

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/UploadPartRequest.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CompletedMultipartUpload.h>
#include <aws/s3/model/CompletedPart.h>
#include <aws/s3/model/ListMultipartUploadsRequest.h>
#include <aws/s3/model/MultipartUpload.h>
#include <aws/s3/model/ListPartsRequest.h>
#include <aws/s3/model/Part.h>
#include <aws/s3/model/AbortMultipartUploadRequest.h>
#include <iostream>
#include <fstream>

char buffer[8*1024*1024];

void usage()
{
  std::cout<<"Usage: upload_obj -a {addr} -b {bucket} -o {object} -f {local-file} -h"<<std::endl;
}

//return the multipart upload ID;
Aws::String create_multipart_upload(Aws::S3::S3Client* s3_client, const Aws::String& bucket, const Aws::String& object)
{
  Aws::S3::Model::CreateMultipartUploadRequest create_mu_request;
  create_mu_request.SetBucket(bucket);
  create_mu_request.SetKey(object);

  auto create_mu_outcome = s3_client->CreateMultipartUpload(create_mu_request);

  if (create_mu_outcome.IsSuccess()) 
  {
    std::cout << "Succeeded to create MultipartUpload for Bucket/Object = " << bucket << "/" << object << std::endl;
    return create_mu_outcome.GetResult().GetUploadId();
  }
  else 
  {
    std::cout << "Failed to create MultipartUpload for Bucket/Object = " << bucket << "/" << object << std::endl;
    std::cout << create_mu_outcome.GetError().GetExceptionName() << " " << create_mu_outcome.GetError().GetMessage() << std::endl;
    return "";
  }

  return ""; //unreachable
}

//return the ETag of the uploaded part;
Aws::String upload_part(Aws::S3::S3Client* s3_client, 
    const Aws::String& bucket, 
    const Aws::String& object, 
    const Aws::String& multiUploadId, 
    const int& partNum, 
    const char* buffer,
    const long long int& len)
{
  Aws::S3::Model::UploadPartRequest upload_request;

  upload_request.SetBucket(bucket);
  upload_request.SetKey(object);
  upload_request.SetContentLength(len);
  upload_request.SetPartNumber(partNum);
  upload_request.SetUploadId(multiUploadId);
  upload_request.SetBody(Aws::MakeShared<Aws::StringStream>("UploadPart", Aws::String(buffer, len)));

  auto upload_outcome = s3_client->UploadPart(upload_request);

  if (upload_outcome.IsSuccess()) 
  {
    std::cout << "Succeeded to upload part " << partNum << " of MultipartUpload " << multiUploadId << "\tBucket/Object = " << bucket << "/" << object << std::endl;
    return upload_outcome.GetResult().GetETag();
  }
  else 
  {
    std::cout << "Failed to upload part " << partNum << " of MultipartUpload " << multiUploadId << "\tBucket/Object = " << bucket << "/" << object << std::endl;
    std::cout << upload_outcome.GetError().GetExceptionName() << " " << upload_outcome.GetError().GetMessage() << std::endl;
    return "";
  }

  return "";
}

void list_parts(Aws::S3::S3Client* s3_client, const Aws::String& bucket, const Aws::String& object, const Aws::String& multiUploadId)
{
  Aws::S3::Model::ListPartsRequest list_part_request;
  list_part_request.SetBucket(bucket);
  list_part_request.SetKey(object);
  list_part_request.SetUploadId(object);
  list_part_request.SetUploadId(multiUploadId);

  auto list_part_outcome = s3_client->ListParts(list_part_request);

  if (list_part_outcome.IsSuccess()) 
  {
    std::cout << "\t\tParts of " << multiUploadId << "\tBucket/Object = "<< bucket << "/"<< object << std::endl;
    const Aws::Vector<Aws::S3::Model::Part>& parts = list_part_outcome.GetResult().GetParts();
    for (Aws::Vector<Aws::S3::Model::Part>::const_iterator itr = parts.begin(); itr != parts.end(); ++itr)
    {
      std::cout<<"\t\tPartNum:" << itr->GetPartNumber() << " ETag:" << itr->GetETag() << std::endl;
    }
  }
  else 
  {
    std::cout << "Failed to list parts of " << multiUploadId << "\tBucket/Object = "<< bucket << "/"<< object << std::endl;
    std::cout << list_part_outcome.GetError().GetExceptionName() << " " << list_part_outcome.GetError().GetMessage() << std::endl;
  }
}

void list_multipart_uploads(Aws::S3::S3Client* s3_client, const Aws::String& bucket)
{
  Aws::S3::Model::ListMultipartUploadsRequest list_mu_request;
  list_mu_request.SetBucket(bucket);

  auto list_outcome = s3_client->ListMultipartUploads(list_mu_request);

  if (list_outcome.IsSuccess()) 
  {
    std::cout << "Uncompleted multipart uploads on bucket '" << bucket << "'" << std::endl;

    const Aws::Vector<Aws::S3::Model::MultipartUpload>& uploads = list_outcome.GetResult().GetUploads();
    for(Aws::Vector<Aws::S3::Model::MultipartUpload>::const_iterator itr=uploads.begin(); itr!=uploads.end();++itr)
    {
      Aws::String object = itr->GetKey();
      std::cout<<"\tMultipartUpload ID: " << itr->GetUploadId() << "\tBucket/Object = " << bucket << "/" << object << std::endl;
      list_parts(s3_client, bucket, object, itr->GetUploadId());
    }
  }
  else 
  {
    std::cout << "Failed to list uncompleted multipart uploads on bucket '" << bucket << "'" << std::endl;
  }
}

int complete_multi_upload(Aws::S3::S3Client* s3_client, 
    const Aws::String& bucket, 
    const Aws::String& object, 
    const Aws::String& multiUploadId, 
    const Aws::S3::Model::CompletedMultipartUpload& upload)
{
  Aws::S3::Model::CompleteMultipartUploadRequest object_request;
  object_request.SetBucket(bucket);
  object_request.SetKey(object);
  object_request.SetUploadId(multiUploadId);
  object_request.SetMultipartUpload(upload);

  auto create_mu_outcome = s3_client->CompleteMultipartUpload(object_request);

  if (create_mu_outcome.IsSuccess()) 
  {
    std::cout << "Succeeded to complete Multiupload: " << multiUploadId << "\tBucket/Object = " << bucket << "/" << object << std::endl;
    return 0;
  }
  else 
  {
    std::cout << "Failed to complete Multiupload: " << multiUploadId << "\tBucket/Object = " << bucket << "/" << object << std::endl;
    std::cout << create_mu_outcome.GetError().GetExceptionName() << " " << create_mu_outcome.GetError().GetMessage() << std::endl;
    return 1;
  }

  return 0;
}

int abort_multi_upload(Aws::S3::S3Client * s3_client, const Aws::String & bucket, const Aws::String & object, const Aws::String & multiUploadId)
{

  Aws::S3::Model::AbortMultipartUploadRequest object_request;
  object_request.SetBucket(bucket);
  object_request.SetKey(object);
  object_request.SetUploadId(multiUploadId);

  auto create_mu_outcome = s3_client->AbortMultipartUpload(object_request);

  if (create_mu_outcome.IsSuccess()) 
  {
    std::cout << "Succeeded to abort Multiupload" << std::endl;
    return 0;
  }
  else 
  {
    std::cout << "Failed to abort Multiupload" << std::endl;
    std::cout << create_mu_outcome.GetError().GetExceptionName() << " " << create_mu_outcome.GetError().GetMessage() << std::endl;
    return 1;
  }

  return 0;
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

  std::cout << "Uploading " << file << " to S3 bucket '" << bucket << "' object '" << object << "'" << std::endl;

  Aws::SDKOptions options;
  Aws::InitAPI(options);

  {
    Aws::Client::ClientConfiguration conf;
    conf.proxyHost = addr;
    conf.proxyPort = 6081;
    conf.scheme = Aws::Http::Scheme::HTTP;   //default is HTTPS;
    conf.endpointOverride = "s3.dnion.com";

    Aws::S3::S3Client s3_client(conf,2);     //2 menas aws signature version 2;

    Aws::String multiUploadId = create_multipart_upload(&s3_client, bucket, object);
    if (multiUploadId=="")
      return 1;

    auto input_data = Aws::MakeShared<Aws::FStream>("UploadObjectInputStream", file.c_str(), std::ios_base::in);
    int partNum = 1;

    Aws::S3::Model::CompletedMultipartUpload upload;

    while(true)
    {
      try
      {
        input_data->read(buffer, 8*1024*1024);

        long long int len = input_data->gcount();
        Aws::String etag = upload_part(&s3_client, bucket, object, multiUploadId, partNum, buffer, len);
        if(etag=="")
          return 1;
        std::cout << "etag of part " << partNum << ": " << etag << std::endl;

        Aws::S3::Model::CompletedPart part;
        part.SetPartNumber(partNum);
        part.SetETag(etag);
        upload.AddParts(part);

        partNum++;

        if(input_data->rdstate() & std::ios::eofbit)
        {
          std::cout << "Read finished" << std::endl;
          break;
        }
      }
      catch(...)
      {
        std::cout<<"upload failed"<<std::endl;
        return 1;
      }
    }

    list_multipart_uploads(&s3_client, bucket);

    list_parts(&s3_client, bucket, object, multiUploadId);

    //abort_multi_upload(&s3_client, bucket, object, multiUploadId);

    complete_multi_upload(&s3_client, bucket, object, multiUploadId, upload);
  }

  Aws::ShutdownAPI(options);
  return 0;
}
