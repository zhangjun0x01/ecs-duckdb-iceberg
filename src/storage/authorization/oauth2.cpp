#include "storage/authorization/oauth2.hpp"
#include "storage/irc_catalog.hpp"
#include "api_utils.hpp"
#include "duckdb/common/exception/http_exception.hpp"

namespace duckdb {

OAuth2Authorization::OAuth2Authorization() : IRCAuthorization(IRCAuthorizationType::OAUTH2) {
}

OAuth2Authorization::OAuth2Authorization(const string &grant_type, const string &uri, const string &client_id,
                                         const string &client_secret, const string &scope)
    : IRCAuthorization(IRCAuthorizationType::OAUTH2), grant_type(grant_type), uri(uri), client_id(client_id),
      client_secret(client_secret), scope(scope) {
}

string OAuth2Authorization::GetToken(ClientContext &context, const string &grant_type, const string &uri,
                                     const string &client_id, const string &client_secret, const string &scope) {
	vector<string> parameters;
	parameters.push_back(StringUtil::Format("%s=%s", "grant_type", grant_type));
	parameters.push_back(StringUtil::Format("%s=%s", "client_id", client_id));
	parameters.push_back(StringUtil::Format("%s=%s", "client_secret", client_secret));
	parameters.push_back(StringUtil::Format("%s=%s", "scope", scope));

	string post_data = StringUtil::Format("%s", StringUtil::Join(parameters, "&"));
	std::unique_ptr<yyjson_doc, YyjsonDocDeleter> doc;
	try {
		string api_result = APIUtils::PostRequest(context, uri, post_data);
		doc = std::unique_ptr<yyjson_doc, YyjsonDocDeleter>(ICUtils::api_result_to_doc(api_result));
	} catch (std::exception &ex) {
		ErrorData error(ex);
		throw InvalidConfigurationException("Could not get token from %s, captured error message: %s", uri,
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

string OAuth2Authorization::GetRequest(ClientContext &context, const IRCEndpointBuilder &endpoint_builder,
                                       curl_slist *extra_headers) {
	return APIUtils::GetRequest(context, endpoint_builder, token);
}

unique_ptr<OAuth2Authorization> OAuth2Authorization::FromAttachOptions(ClientContext &context,
                                                                       IcebergAttachOptions &input) {
	auto result = make_uniq<OAuth2Authorization>();

	unordered_map<string, Value> remaining_options;
	string secret;
	for (auto &entry : input.options) {
		auto lower_name = StringUtil::Lower(entry.first);
		if (lower_name == "secret") {
			secret = entry.second.ToString();
		} else if (lower_name == "oauth2_scope") {
			result->scope = entry.second.ToString();
		} else if (lower_name == "oauth2_server_uri") {
			result->uri = entry.second.ToString();
		} else if (lower_name == "client_id") {
			result->client_id = entry.second.ToString();
		} else if (lower_name == "client_secret") {
			result->client_secret = entry.second.ToString();
		} else if (lower_name == "oauth2_grant_type") {
			result->grant_type = entry.second.ToString();
		} else {
			remaining_options.emplace(std::move(entry));
		}
	}

	Value token;
	// Check if any of the options are given that indicate intent to provide options inline, rather than use a secret
	bool user_intends_to_use_secret = true;
	if (!result->scope.empty() || !result->uri.empty() || !result->client_id.empty() ||
	    !result->client_secret.empty()) {
		user_intends_to_use_secret = false;
	}

	auto iceberg_secret = IRCatalog::GetIcebergSecret(context, secret, user_intends_to_use_secret);
	if (iceberg_secret) {
		//! The catalog secret (iceberg secret) will already have acquired a token, these additional settings in the
		//! attach options will not be used. Better to explicitly throw than to just ignore the options and cause
		//! confusion for the user.
		if (!result->scope.empty()) {
			throw InvalidConfigurationException(
			    "Both an 'oauth2_scope' and a 'secret' are provided, these are mutually exclusive.");
		}
		if (!result->uri.empty()) {
			throw InvalidConfigurationException("Both an 'oauth2_server_uri' and a 'secret' are "
			                                    "provided, these are mutually exclusive.");
		}
		if (!result->client_id.empty()) {
			throw InvalidConfigurationException(
			    "Please provide either a 'client_id'+'client_secret' pair, or 'secret', "
			    "these options are mutually exclusive");
		}
		if (!result->client_secret.empty()) {
			throw InvalidConfigurationException("Please provide either a client_id+client_secret pair, or 'secret', "
			                                    "these options are mutually exclusive");
		}

		auto &kv_iceberg_secret = dynamic_cast<const KeyValueSecret &>(*iceberg_secret->secret);
		token = kv_iceberg_secret.TryGetValue("token");
		result->uri = kv_iceberg_secret.TryGetValue("token").ToString();
	} else {
		if (!secret.empty()) {
			throw InvalidConfigurationException("No ICEBERG secret by the name of '%s' could be found", secret);
		}

		if (result->scope.empty()) {
			//! Default to the Polaris scope: 'PRINCIPAL_ROLE:ALL'
			result->scope = "PRINCIPAL_ROLE:ALL";
		}

		if (result->grant_type.empty()) {
			result->grant_type = "client_credentials";
		}

		if (result->uri.empty()) {
			//! If no oauth2_server_uri is provided, default to the (deprecated) REST API endpoint for it
			DUCKDB_LOG_WARN(context, "iceberg",
			                "'oauth2_server_uri' is not set, defaulting to deprecated '{endpoint}/v1/oauth/tokens' "
			                "oauth2_server_uri");
			result->uri = StringUtil::Format("%s/v1/oauth/tokens", input.endpoint);
		}

		if (result->client_id.empty() || result->client_secret.empty()) {
			throw InvalidConfigurationException("Please provide either a 'client_id' + 'client_secret' pair or the "
			                                    "name of an ICEBERG secret as 'secret'");
		}

		CreateSecretInput create_secret_input;
		create_secret_input.options["oauth2_server_uri"] = result->uri;
		create_secret_input.options["oauth2_scope"] = result->scope;
		create_secret_input.options["oauth2_grant_type"] = result->grant_type;
		create_secret_input.options["client_id"] = result->client_id;
		create_secret_input.options["client_secret"] = result->client_secret;
		create_secret_input.options["endpoint"] = input.endpoint;
		create_secret_input.options["authorization_type"] = "oauth2";

		auto new_secret = IRCAuthorization::CreateCatalogSecretFunction(context, create_secret_input);
		auto &kv_iceberg_secret = dynamic_cast<KeyValueSecret &>(*new_secret);
		token = kv_iceberg_secret.TryGetValue("token");
	}
	if (token.IsNull()) {
		throw HTTPException(StringUtil::Format("Failed to retrieve OAuth2 token from %s", result->uri));
	}
	result->token = token.ToString();

	input.options = std::move(remaining_options);
	return result;
}

} // namespace duckdb
