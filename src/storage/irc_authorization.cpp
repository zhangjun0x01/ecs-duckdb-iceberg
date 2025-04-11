#include "storage/irc_authorization.hpp"
#include "api_utils.hpp"
#include "storage/authorization/oauth2.hpp"

namespace duckdb {

namespace {

//! NOTE: We sadly don't receive the CreateSecretFunction or some other context to deduplicate the recognized options
//! So we use this to deduplicate it instead
static const case_insensitive_map_t<LogicalType> &IcebergSecretOptions() {
	static const case_insensitive_map_t<LogicalType> options {
	    {"client_id", LogicalType::VARCHAR},         {"client_secret", LogicalType::VARCHAR},
	    {"endpoint", LogicalType::VARCHAR},          {"token", LogicalType::VARCHAR},
	    {"oauth2_scope", LogicalType::VARCHAR},      {"oauth2_server_uri", LogicalType::VARCHAR},
	    {"oauth2_grant_type", LogicalType::VARCHAR}, {"authorization_type", LogicalType::VARCHAR}};
	return options;
}

} // namespace

IRCAuthorizationType IRCAuthorization::TypeFromString(const string &type) {
	static const case_insensitive_map_t<IRCAuthorizationType> mapping {{"oauth2", IRCAuthorizationType::OAUTH2},
	                                                                   {"sigv4", IRCAuthorizationType::SIGV4}};

	for (auto it : mapping) {
		if (StringUtil::CIEquals(it.first, type)) {
			return it.second;
		}
	}

	vector<string> accepted_options;
	for (auto it : mapping) {
		accepted_options.push_back(it.first);
	}
	throw InvalidConfigurationException("'authorization_type' '%s' is not supported, valid options are: %s", type,
	                                    StringUtil::Join(accepted_options, ", "));
}

unique_ptr<BaseSecret> IRCAuthorization::CreateCatalogSecretFunction(ClientContext &context, CreateSecretInput &input) {
	// apply any overridden settings
	vector<string> prefix_paths;
	auto result = make_uniq<KeyValueSecret>(prefix_paths, "iceberg", "config", input.name);
	result->redact_keys = {"token", "client_id", "client_secret"};

	auto &accepted_parameters = IcebergSecretOptions();

	for (const auto &named_param : input.options) {
		auto &param_name = named_param.first;
		auto it = accepted_parameters.find(param_name);
		if (it != accepted_parameters.end()) {
			result->secret_map[param_name] = named_param.second.ToString();
		} else {
			throw InvalidInputException("Unknown named parameter passed to CreateIRCSecretFunction: %s", param_name);
		}
	}

	//! ---- Token ----
	auto token_it = result->secret_map.find("token");
	if (token_it != result->secret_map.end()) {
		return std::move(result);
	}

	//! ---- Server URI (and Endpoint) ----
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

	//! ---- Authorization Type ----
	auto authorization_type_it = result->secret_map.find("authorization_type");
	if (authorization_type_it != result->secret_map.end()) {
		auto authorization_type = authorization_type_it->second.ToString();
		if (!StringUtil::CIEquals(authorization_type, "oauth2")) {
			throw InvalidInputException(
			    "Unsupported option ('%s') for 'authorization_type', 'ICEBERG' secret only supports 'oauth2' currently",
			    authorization_type);
		}
	} else {
		//! Default to oauth2 type
		result->secret_map["authorization_type"] = "oauth2";
	}

	//! ---- Client ID + Client Secret ----
	case_insensitive_set_t required_parameters {"client_id", "client_secret"};
	for (auto &param : required_parameters) {
		if (!result->secret_map.count(param)) {
			throw InvalidInputException("Missing required parameter '%s' for authorization_type 'oauth2'", param);
		}
	}

	//! ---- Grant Type ----
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

	//! ---- Scope ----
	if (!result->secret_map.count("oauth2_scope")) {
		//! Default to default Polaris role
		result->secret_map["oauth2_scope"] = "PRINCIPAL_ROLE:ALL";
	}

	// Make a request to the oauth2 server uri to get the (bearer) token
	result->secret_map["token"] = OAuth2Authorization::GetToken(
	    context, result->secret_map["oauth2_grant_type"].ToString(), server_uri,
	    result->secret_map["client_id"].ToString(), result->secret_map["client_secret"].ToString(),
	    result->secret_map["oauth2_scope"].ToString());
	return std::move(result);
}

void IRCAuthorization::SetCatalogSecretParameters(CreateSecretFunction &function) {
	auto &options = IcebergSecretOptions();
	function.named_parameters.insert(options.begin(), options.end());
}

} // namespace duckdb
