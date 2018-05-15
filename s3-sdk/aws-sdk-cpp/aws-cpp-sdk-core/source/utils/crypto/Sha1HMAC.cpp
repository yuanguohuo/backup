/*
 * Yuanguo: implemente AWS Signatrue V2 and the dependent Sha1HMAC;
 * Yuanguo: only for linux;  APPLE is not tested;
 */

#include <aws/core/utils/crypto/Sha1HMAC.h>
#include <aws/core/utils/crypto/Factories.h>
#include <aws/core/utils/Outcome.h>

namespace Aws
{
namespace Utils
{
namespace Crypto
{

Sha1HMAC::Sha1HMAC() : 
    m_hmacImpl(CreateSha1HMACImplementation())
{
}

Sha1HMAC::~Sha1HMAC()
{
}

HashResult Sha1HMAC::Calculate(const Aws::Utils::ByteBuffer& toSign, const Aws::Utils::ByteBuffer& secret)
{
    return m_hmacImpl->Calculate(toSign, secret);
}

} // namespace Crypto
} // namespace Utils
} // namespace Aws
