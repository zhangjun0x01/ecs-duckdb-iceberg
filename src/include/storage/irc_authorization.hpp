#pragma once

#include "duckdb/main/secret/secret.hpp"
#include "catalog_utils.hpp"

namespace duckdb {

enum class IRCAuthorizationType : uint8_t { OAUTH2, SIGV4, INVALID };

struct IRCOAuth2RequestInput {
public:
	IRCOAuth2RequestInput(const string &grant_type, const string &uri, const string &client_id,
	                      const string &client_secret, const string &scope)
	    : grant_type(grant_type), uri(uri), client_id(client_id), client_secret(client_secret), scope(scope) {
	}

public:
	string grant_type;
	string uri;
	string client_id;
	string client_secret;
	string scope;
};

struct IRCAuthorization {
public:
	IRCAuthorization() = delete;

public:
	static void SetCatalogSecretParameters(CreateSecretFunction &function);
	static unique_ptr<BaseSecret> CreateCatalogSecretFunction(ClientContext &context, CreateSecretInput &input);
	static string GetToken(ClientContext &context, const IRCOAuth2RequestInput &input);
};

} // namespace duckdb
