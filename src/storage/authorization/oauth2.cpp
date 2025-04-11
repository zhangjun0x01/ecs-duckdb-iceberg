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
	return APIUtils::GetRequest(context, endpoint_builder, token, extra_headers);
}

unique_ptr<OAuth2Authorization> OAuth2Authorization::FromAttachOptions(ClientContext &context,
                                                                       IcebergAttachOptions &input) {
	auto result = make_uniq<OAuth2Authorization>();

	unordered_map<string, Value> remaining_options;
	case_insensitive_map_t<Value> create_secret_options;
	string secret;
	Value token;

	static const unordered_set<string> recognized_create_secret_options {
	    "oauth2_scope", "oauth2_server_uri", "oauth2_grant_type", "token", "client_id", "client_secret"};

	for (auto &entry : input.options) {
		auto lower_name = StringUtil::Lower(entry.first);
		if (lower_name == "secret") {
			secret = entry.second.ToString();
		} else if (recognized_create_secret_options.count(lower_name)) {
			create_secret_options.emplace(std::move(entry));
		} else {
			remaining_options.emplace(std::move(entry));
		}
	}

	unique_ptr<SecretEntry> iceberg_secret;
	if (create_secret_options.empty()) {
		//! Look up an ICEBERG secret
		iceberg_secret = IRCatalog::GetIcebergSecret(context, secret);
		if (!iceberg_secret) {
			if (!secret.empty()) {
				throw InvalidConfigurationException("No ICEBERG secret by the name of '%s' could be found", secret);
			} else {
				throw InvalidConfigurationException("No ICEBERG secret exists, and no OAuth2 options were provided");
			}
		}
		auto &kv_iceberg_secret = dynamic_cast<const KeyValueSecret &>(*iceberg_secret->secret);
		token = kv_iceberg_secret.TryGetValue("token");
	} else {
		CreateSecretInput create_secret_input;
		create_secret_options["endpoint"] = input.endpoint;
		create_secret_input.options = std::move(create_secret_options);
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
