#include "api_utils.hpp"
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
	AWSInput aws_input;
	aws_input.cert_path = SELECTED_CURL_CERT_PATH;
	// Set the user Agent.
	auto &config = DBConfig::GetConfig(context);
	aws_input.user_agent = config.UserAgent();

	aws_input.service = GetAwsService(endpoint_builder.GetHost());
	aws_input.region = GetAwsRegion(endpoint_builder.GetHost());

	auto decomposed_host = DecomposeHost(endpoint_builder.GetHost());

	aws_input.authority = decomposed_host.authority;
	for (auto &component : decomposed_host.path_components) {
		aws_input.path_segments.push_back(component);
	}
	for (auto &component : endpoint_builder.path_components) {
		aws_input.path_segments.push_back(component);
	}

	for (auto &param : endpoint_builder.GetParams()) {
		aws_input.query_string_parameters.emplace_back(param);
	}

	// will error if no secret can be found for AWS services
	auto secret_entry = IRCatalog::GetStorageSecret(context, secret_name);
	auto kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_entry->secret);

	aws_input.key_id = kv_secret.secret_map["key_id"].GetValue<string>();
	aws_input.secret = kv_secret.secret_map["secret"].GetValue<string>();
	aws_input.session_token =
	    kv_secret.secret_map["session_token"].IsNull() ? "" : kv_secret.secret_map["session_token"].GetValue<string>();

	return aws_input.GetRequest(context);
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

	return request_input.DeleteRequest(context);
}

string APIUtils::PostRequest(ClientContext &context, const string &url, const string &post_data,
                             RequestInput &request_input, const string &content_type, const string &token) {
	auto &config = DBConfig::GetConfig(context);
	request_input.AddHeader(StringUtil::Format("User-Agent: %s", config.UserAgent()));
	request_input.AddHeader("Content-Type: application/" + content_type);
	request_input.SetURL(url);
	request_input.SetCertPath(SELECTED_CURL_CERT_PATH);
	request_input.SetBearerToken(token);

	return request_input.PostRequest(context, post_data);
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

	return request_input.GetRequest(context);
}

} // namespace duckdb
