#include "storage/irc_authorization.hpp"
#include "api_utils.hpp"

namespace duckdb {

string IRCAuthorization::GetToken(ClientContext &context, const IRCOAuth2RequestInput &input) {
	vector<string> parameters;
	parameters.push_back(StringUtil::Format("%s=%s", "grant_type", input.grant_type));
	parameters.push_back(StringUtil::Format("%s=%s", "client_id", input.client_id));
	parameters.push_back(StringUtil::Format("%s=%s", "client_secret", input.client_secret));
	parameters.push_back(StringUtil::Format("%s=%s", "scope", input.scope));

	string post_data = StringUtil::Format("%s", StringUtil::Join(parameters, "&"));
	std::unique_ptr<yyjson_doc, YyjsonDocDeleter> doc;
	try {
		string api_result = APIUtils::PostRequest(context, input.uri, post_data);
		doc = std::unique_ptr<yyjson_doc, YyjsonDocDeleter>(ICUtils::api_result_to_doc(api_result));
	} catch (std::exception &ex) {
		ErrorData error(ex);
		throw InvalidConfigurationException("Could not get token from %s, captured error message: %s", input.uri,
		                                    error.RawMessage());
	}
	//! FIXME: the oauth/tokens endpoint returns, on success;
	// { 'access_token', 'token_type', 'expires_in', <issued_token_type>, 'refresh_token', 'scope'}
	auto *root = yyjson_doc_get_root(doc.get());
	auto access_token_val = yyjson_obj_get(root, "access_token");
	auto token_type_val = yyjson_obj_get(root, "token_type");
	if (!access_token_val) {
		throw InvalidConfigurationException("OAuthTokenResponse is missing required property 'access_token'");
	}
	if (!token_type_val) {
		throw InvalidConfigurationException("OAuthTokenResponse is missing required property 'token_type'");
	}
	string token_type = yyjson_get_str(token_type_val);
	if (!StringUtil::CIEquals(token_type, "bearer")) {
		throw NotImplementedException(
		    "token_type return value '%s' is not supported, only supports 'bearer' currently.", token_type);
	}
	string access_token = yyjson_get_str(access_token_val);
	return access_token;
}

unique_ptr<BaseSecret> IRCAuthorization::CreateCatalogSecretFunction(ClientContext &context, CreateSecretInput &input) {
	// apply any overridden settings
	vector<string> prefix_paths;
	auto result = make_uniq<KeyValueSecret>(prefix_paths, "iceberg", "config", input.name);
	result->redact_keys = {"token", "client_id", "client_secret"};

	case_insensitive_set_t accepted_parameters {"client_id",         "client_secret",     "endpoint",
	                                            "oauth2_scope",      "oauth2_server_uri", "oauth2_grant_type",
	                                            "authorization_type"};
	for (const auto &named_param : input.options) {
		auto &param_name = named_param.first;
		auto it = accepted_parameters.find(param_name);
		if (it != accepted_parameters.end()) {
			result->secret_map[param_name] = named_param.second.ToString();
		} else {
			throw InvalidInputException("Unknown named parameter passed to CreateIRCSecretFunction: %s", param_name);
		}
	}

	//! If the bearer token is explicitly given, there is no need to make a request, use it directly.
	auto token_it = result->secret_map.find("token");
	if (token_it != result->secret_map.end()) {
		return std::move(result);
	}

	// Check if we have an oauth2_server_uri, or fall back to the deprecated oauth endpoint
	string server_uri;
	auto oauth2_server_uri_it = result->secret_map.find("oauth2_server_uri");
	auto endpoint_it = result->secret_map.find("endpoint");
	if (oauth2_server_uri_it != result->secret_map.end()) {
		server_uri = oauth2_server_uri_it->second.ToString();
	} else if (endpoint_it != result->secret_map.end()) {
		DUCKDB_LOG_WARN(
		    context, "iceberg",
		    "'oauth2_server_uri' is not set, defaulting to deprecated '{endpoint}/v1/oauth/tokens' oauth2_server_uri");
		server_uri = StringUtil::Format("%s/v1/oauth/tokens", endpoint_it->second.ToString());
	} else {
		throw InvalidInputException(
		    "AUTHORIZATION_TYPE is 'oauth2', yet no 'oauth2_server_uri' was provided, and no 'endpoint' was provided "
		    "to fall back on. Please provide one or change the 'authorization_type'.");
	}

	auto authorization_type_it = result->secret_map.find("authorization_type");
	if (authorization_type_it != result->secret_map.end()) {
		auto authorization_type = authorization_type_it->second.ToString();
		if (!StringUtil::CIEquals(authorization_type, "oauth2")) {
			throw InvalidInputException(
			    "Unsupported option ('%s') for 'authorization_type', only supports 'oauth2' currently",
			    authorization_type);
		}
	} else {
		//! Default to oauth2 authorization type
		result->secret_map["authorization_type"] = "oauth2";
	}

	case_insensitive_set_t required_parameters {"client_id", "client_secret"};
	for (auto &param : required_parameters) {
		if (!result->secret_map.count(param)) {
			throw InvalidInputException("Missing required parameter '%s' for authorization_type 'oauth2'", param);
		}
	}

	auto grant_type_it = result->secret_map.find("oauth2_grant_type");
	if (grant_type_it != result->secret_map.end()) {
		auto grant_type = grant_type_it->second.ToString();
		if (!StringUtil::CIEquals(grant_type, "client_credentials")) {
			throw InvalidInputException(
			    "Unsupported option ('%s') for 'oauth2_grant_type', only supports 'client_credentials' currently",
			    grant_type);
		}
	} else {
		//! Default to client_credentials
		result->secret_map["oauth2_grant_type"] = "client_credentials";
	}

	if (!result->secret_map.count("oauth2_scope")) {
		//! Default to default Polaris role
		result->secret_map["oauth2_scope"] = "PRINCIPAL_ROLE:ALL";
	}

	// Make a request to the oauth2 server uri to get the (bearer) token
	auto oauth2_input = IRCOAuth2RequestInput(
	    result->secret_map["oauth2_grant_type"].ToString(), server_uri, result->secret_map["client_id"].ToString(),
	    result->secret_map["client_secret"].ToString(), result->secret_map["oauth2_scope"].ToString());
	result->secret_map["token"] = IRCAuthorization::GetToken(context, oauth2_input);
	return std::move(result);
}

void IRCAuthorization::SetCatalogSecretParameters(CreateSecretFunction &function) {
	function.named_parameters["client_id"] = LogicalType::VARCHAR;
	function.named_parameters["client_secret"] = LogicalType::VARCHAR;
	function.named_parameters["endpoint"] = LogicalType::VARCHAR;
	function.named_parameters["token"] = LogicalType::VARCHAR;
	function.named_parameters["oauth2_scope"] = LogicalType::VARCHAR;
	function.named_parameters["oauth2_server_uri"] = LogicalType::VARCHAR;
	function.named_parameters["oauth2_grant_type"] = LogicalType::VARCHAR;
	function.named_parameters["authorization_type"] = LogicalType::VARCHAR;
}

} // namespace duckdb
