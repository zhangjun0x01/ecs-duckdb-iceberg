#include "api_utils.hpp"
#include "credentials/credential_provider.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/http_exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "storage/irc_catalog.hpp"

#include <sys/stat.h>

namespace duckdb {

namespace {

struct HostDecompositionResult {
	string authority;
	vector<string> path_components;
};

HostDecompositionResult DecomposeHost(const string &host) {
	HostDecompositionResult result;

	auto start_of_path = host.find('/');
	if (start_of_path != std::string::npos) {
		//! Authority consists of everything (assuming the host does not contain the scheme) before the first slash
		result.authority = host.substr(0, start_of_path);
		auto remainder = host.substr(start_of_path + 1);
		result.path_components = StringUtil::Split(remainder, '/');
	} else {
		result.authority = host;
	}
	return result;
}

} // namespace

string APIUtils::GetAwsRegion(const string &host) {
	idx_t first_dot = host.find_first_of('.');
	idx_t second_dot = host.find_first_of('.', first_dot + 1);
	return host.substr(first_dot + 1, second_dot - first_dot - 1);
}

string APIUtils::GetAwsService(const string &host) {
	return host.substr(0, host.find_first_of('.'));
}

string APIUtils::GetRequestAws(ClientContext &context, IRCEndpointBuilder endpoint_builder, const string &secret_name) {
	auto clientConfig = make_uniq<Aws::Client::ClientConfiguration>();

	if (!SELECTED_CURL_CERT_PATH.empty()) {
		clientConfig->caFile = SELECTED_CURL_CERT_PATH;
	}

	std::shared_ptr<Aws::Http::HttpClientFactory> MyClientFactory;
	std::shared_ptr<Aws::Http::HttpClient> MyHttpClient;

	MyHttpClient = Aws::Http::CreateHttpClient(*clientConfig);
	Aws::Http::URI uri;

	// TODO move this to IRCatalog::GetBaseURL()
	auto service = GetAwsService(endpoint_builder.GetHost());
	auto region = GetAwsRegion(endpoint_builder.GetHost());

	auto decomposed_host = DecomposeHost(endpoint_builder.GetHost());
	for (auto &component : decomposed_host.path_components) {
		uri.AddPathSegment(component);
	}

	for (auto &component : endpoint_builder.path_components) {
		uri.AddPathSegment(component);
	}

	for (auto &param : endpoint_builder.GetParams()) {
		const char *key = param.first.c_str();
		auto value = param.second.c_str();
		uri.AddQueryStringParameter(key, value);
	}

	Aws::Http::Scheme scheme = Aws::Http::Scheme::HTTPS;
	uri.SetScheme(scheme);
	// set host
	uri.SetAuthority(decomposed_host.authority);

	const Aws::Http::URI uri_const = Aws::Http::URI(uri);
	auto create_http_req = Aws::Http::CreateHttpRequest(uri_const, Aws::Http::HttpMethod::HTTP_GET,
	                                                    Aws::Utils::Stream::DefaultResponseStreamFactoryMethod);

	std::shared_ptr<Aws::Http::HttpRequest> req(create_http_req);

	// Set the user Agent.
	auto &config = DBConfig::GetConfig(context);
	req->SetUserAgent(config.UserAgent());

	// will error if no secret can be found for AWS services
	auto secret_entry = IRCatalog::GetStorageSecret(context, secret_name);
	auto kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_entry->secret);

	std::shared_ptr<Aws::Auth::AWSCredentialsProviderChain> provider;
	provider = std::make_shared<DuckDBSecretCredentialProvider>(
	    kv_secret.secret_map["key_id"].GetValue<string>(), kv_secret.secret_map["secret"].GetValue<string>(),
	    kv_secret.secret_map["session_token"].IsNull() ? "" : kv_secret.secret_map["session_token"].GetValue<string>());
	auto signer = make_uniq<Aws::Client::AWSAuthV4Signer>(provider, service.c_str(), region.c_str());

	signer->SignRequest(*req);
	std::shared_ptr<Aws::Http::HttpResponse> res = MyHttpClient->MakeRequest(req);
	Aws::Http::HttpResponseCode resCode = res->GetResponseCode();
	DUCKDB_LOG_DEBUG(context, "iceberg.Catalog.Aws.HTTPRequest",
	                 "GET %s (response %d) (signed with key_id '%s' for service '%s', in region '%s')",
	                 uri.GetURIString(), resCode, kv_secret.secret_map["key_id"].GetValue<string>(), service.c_str(),
	                 region.c_str());
	if (resCode == Aws::Http::HttpResponseCode::OK) {
		Aws::StringStream resBody;
		resBody << res->GetResponseBody().rdbuf();
		return resBody.str();
	} else {
		Aws::StringStream resBody;
		resBody << res->GetResponseBody().rdbuf();
		throw HTTPException(StringUtil::Format("Failed to query %s, http error %d thrown. Message: %s",
		                                       req->GetUri().GetURIString(true), res->GetResponseCode(),
		                                       resBody.str()));
	}
}

// Look through the the above locations and if one of the files exists, set that as the location curl should use.
bool APIUtils::SelectCurlCertPath() {
	for (string &caFile : certFileLocations) {
		struct stat buf;
		if (stat(caFile.c_str(), &buf) == 0) {
			SELECTED_CURL_CERT_PATH = caFile;
		}
	}
	return false;
}

string APIUtils::DeleteRequest(ClientContext &context, const string &url, RequestInput &request_input,
                               const string &token) {
	// Set the user Agent.
	auto &config = DBConfig::GetConfig(context);
	request_input.AddHeader(StringUtil::Format("User-Agent: %s", config.UserAgent()));
	request_input.SetURL(url);
	request_input.SetCertPath(SELECTED_CURL_CERT_PATH);
	request_input.SetBearerToken(token);

	return request_input.DELETE(context);
}

string APIUtils::PostRequest(ClientContext &context, const string &url, const string &post_data,
                             RequestInput &request_input, const string &content_type, const string &token) {
	auto &config = DBConfig::GetConfig(context);
	request_input.AddHeader(StringUtil::Format("User-Agent: %s", config.UserAgent()));
	request_input.AddHeader("Content-Type: application/" + content_type);
	request_input.SetURL(url);
	request_input.SetCertPath(SELECTED_CURL_CERT_PATH);
	request_input.SetBearerToken(token);

	return request_input.POST(context, post_data);
}

string APIUtils::GetRequest(ClientContext &context, const IRCEndpointBuilder &endpoint_builder,
                            RequestInput &request_input, const string &token) {
	auto url = endpoint_builder.GetURL();
	// Set the user Agent.
	auto &config = DBConfig::GetConfig(context);
	request_input.AddHeader(StringUtil::Format("User-Agent: %s", config.UserAgent()));
	request_input.SetURL(url);
	request_input.SetCertPath(SELECTED_CURL_CERT_PATH);
	request_input.SetBearerToken(token);

	return request_input.GET(context);
}

} // namespace duckdb
