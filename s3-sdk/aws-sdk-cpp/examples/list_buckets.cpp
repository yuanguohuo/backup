#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/s3/model/Bucket.h>
#include <aws/core/http/Scheme.h>

int main()
{
  Aws::SDKOptions options;
  Aws::InitAPI(options);

  {
    Aws::Client::ClientConfiguration conf;
    conf.proxyHost = "127.0.0.1";
    conf.proxyPort = 6081;
    conf.scheme = Aws::Http::Scheme::HTTP;   //default is HTTPS;
    conf.endpointOverride = "s3.dnion.com";

    Aws::S3::S3Client s3_client(conf,2);

    auto outcome = s3_client.ListBuckets();

    if (outcome.IsSuccess()) {
      std::cout << "Your Amazon S3 buckets:" << std::endl;

      Aws::Vector<Aws::S3::Model::Bucket> bucket_list =
        outcome.GetResult().GetBuckets();

      for (auto const &bucket: bucket_list) {
        std::cout << "  * " << bucket.GetName() << std::endl;
      }
    } else {
      std::cout << "ListBuckets error: "
        << outcome.GetError().GetExceptionName() << " - "
        << outcome.GetError().GetMessage() << std::endl;
    }
  }

  Aws::ShutdownAPI(options);
}
