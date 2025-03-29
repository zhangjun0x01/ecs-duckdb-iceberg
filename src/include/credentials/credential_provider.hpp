#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/http/HttpClient.h>
#include <aws/core/http/HttpRequest.h>

namespace duckdb {

class DuckDBSecretCredentialProvider : public Aws::Auth::AWSCredentialsProviderChain {
public:
	DuckDBSecretCredentialProvider(const string &key_id, const string &secret, const string &sesh_token) {
		credentials.SetAWSAccessKeyId(key_id);
		credentials.SetAWSSecretKey(secret);
		credentials.SetSessionToken(sesh_token);
	}

	~DuckDBSecretCredentialProvider() = default;

	Aws::Auth::AWSCredentials GetAWSCredentials() override {
		return credentials;
	};

protected:
	Aws::Auth::AWSCredentials credentials;
};

} // namespace duckdb
