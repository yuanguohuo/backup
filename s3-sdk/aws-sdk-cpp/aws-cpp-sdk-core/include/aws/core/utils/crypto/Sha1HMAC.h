/*
 * Yuanguo: implemente AWS Signatrue V2 and the dependent Sha1HMAC;
 * Yuanguo: only for linux;  APPLE is not tested;
 */
#pragma once

#ifdef __APPLE__

#ifdef __clang__
#pragma clang diagnostic ignored "-Wdeprecated-declarations"
#endif // __clang__

#ifdef __GNUC__
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif // __GNUC__

#endif // __APPLE__

#include <aws/core/Core_EXPORTS.h>

#include <aws/core/utils/crypto/HMAC.h>
#include <aws/core/utils/memory/AWSMemory.h>

namespace Aws
{
    namespace Utils
    {
        namespace Crypto
        {
            /**
             * Sha1 HMAC implementation
             */
            class AWS_CORE_API Sha1HMAC : public HMAC
            {
            public:
                /**
                 * initializes platform specific libs.
                 */
                Sha1HMAC();
                virtual ~Sha1HMAC();

                /**
                * Calculates a SHA1 HMAC digest (not hex encoded)
                */
                virtual HashResult Calculate(const Aws::Utils::ByteBuffer& toSign, const Aws::Utils::ByteBuffer& secret) override;

            private:

                std::shared_ptr< HMAC > m_hmacImpl;
            };

        } // namespace Sha1
    } // namespace Utils
} // namespace Aws

