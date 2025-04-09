#define DUCKDB_EXTENSION_MAIN

#include "iceberg_extension.hpp"
#include "storage/irc_catalog.hpp"
#include "storage/irc_transaction_manager.hpp"
#include "duckdb.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/http_exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/catalog/catalog_entry/macro_catalog_entry.hpp"
#include "duckdb/catalog/default/default_functions.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "iceberg_functions.hpp"
#include "storage/irc_authorization.hpp"
#include "yyjson.hpp"
#include "catalog_api.hpp"
#include "aws/core/Aws.h"
#include "aws/s3/S3Client.h"
#include "duckdb/main/extension_helper.hpp"

namespace duckdb {

static bool SanityCheckGlueWarehouse(string warehouse) {
	// valid glue catalog warehouse is <account_id>:s3tablescatalog/<bucket>
	auto end_account_id = warehouse.find_first_of(':');
	bool account_id_correct = end_account_id == 12;
	auto bucket_sep = warehouse.find_first_of('/');
	bool bucket_sep_correct = bucket_sep == 28;
	if (!account_id_correct) {
		throw InvalidConfigurationException("Invalid Glue Catalog Format: '%s'. Expect 12 digits for account_id.",
		                                    warehouse);
	}
	if (bucket_sep_correct) {
		return true;
	}
	throw InvalidConfigurationException(
	    "Invalid Glue Catalog Format: '%s'. Expected '<account_id>:s3tablescatalog/<bucket>", warehouse);
}

static unique_ptr<Catalog> IcebergCatalogAttach(StorageExtensionInfo *storage_info, ClientContext &context,
                                                AttachedDatabase &db, const string &name, AttachInfo &info,
                                                AccessMode access_mode) {
	IRCCredentials credentials;
	IRCEndpointBuilder endpoint_builder;

	string endpoint_type;
	string endpoint;
	string oauth2_server_uri;
	string client_id;
	string client_secret;
	string grant_type;
	auto &oauth2_scope = credentials.oauth2_scope;

	// First process all the options passed to attach
	string secret;
	for (auto &entry : info.options) {
		auto lower_name = StringUtil::Lower(entry.first);
		if (lower_name == "type" || lower_name == "read_only") {
			// already handled
		} else if (lower_name == "secret") {
			if (!secret.empty()) {
				throw InvalidInputException("Duplicate 'secret' option detected!");
			}
			secret = StringUtil::Lower(entry.second.ToString());
		} else if (lower_name == "endpoint_type") {
			endpoint_type = StringUtil::Lower(entry.second.ToString());
		} else if (lower_name == "authorization_type") {
			auto val = entry.second.ToString();
			if (!StringUtil::CIEquals(val, "oauth2")) {
				throw InvalidInputException(
				    "Unsupported option ('%s') for 'authorization_type', only supports 'oauth2' currently", val);
			}
		} else if (lower_name == "endpoint") {
			endpoint = StringUtil::Lower(entry.second.ToString());
			StringUtil::RTrim(endpoint, "/");
		} else if (lower_name == "oauth2_scope") {
			oauth2_scope = entry.second.ToString();
		} else if (lower_name == "oauth2_server_uri") {
			oauth2_server_uri = entry.second.ToString();
		} else if (lower_name == "client_id") {
			client_id = entry.second.ToString();
		} else if (lower_name == "client_secret") {
			client_secret = entry.second.ToString();
		} else if (lower_name == "oauth2_grant_type") {
			grant_type = entry.second.ToString();
		} else {
			throw BinderException("Unrecognized option for Iceberg attach: %s", entry.first);
		}
	}

	auto warehouse = info.path;
	if (endpoint_type == "glue" || endpoint_type == "s3_tables") {
		string service;
		ICEBERG_CATALOG_TYPE catalog_type;

		if (endpoint_type == "s3_tables") {
			service = "s3tables";
			catalog_type = ICEBERG_CATALOG_TYPE::AWS_S3TABLES;
		} else {
			service = endpoint_type;
			catalog_type = ICEBERG_CATALOG_TYPE::AWS_GLUE;
		}
		// look up any s3 secret

		// if there is no secret, an error will be thrown
		auto secret_entry = IRCatalog::GetStorageSecret(context, secret);
		auto kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_entry->secret);
		auto region = kv_secret.TryGetValue("region");

		if (region.IsNull()) {
			throw InvalidConfigurationException("Assumed catalog secret '%s' for catalog '%s' does not have a region",
			                                    secret_entry->secret->GetName(), name);
		}

		switch (catalog_type) {
		case ICEBERG_CATALOG_TYPE::AWS_S3TABLES: {
			// extract region from the amazon ARN
			auto substrings = StringUtil::Split(warehouse, ":");
			if (substrings.size() != 6) {
				throw InvalidInputException("Could not parse S3 Tables ARN warehouse value");
			}
			//! FIXME: Why are we throwing above if 'region' in the assigned secret is NULL, when we override it here
			//! anyways??
			region = Value::CreateValue<string>(substrings[3]);
			break;
		}
		case ICEBERG_CATALOG_TYPE::AWS_GLUE:
			SanityCheckGlueWarehouse(warehouse);
			break;
		default:
			throw NotImplementedException("Unsupported AWS catalog type");
		}

		//! NOTE: The 'v1' here is for the AWS Service API version, not the Iceberg or IRC version
		endpoint = StringUtil::Format("%s.%s.amazonaws.com/v1/iceberg", service, region.ToString());
		auto catalog = make_uniq<IRCatalog>(db, access_mode, credentials, warehouse, endpoint, secret);
		catalog->GetConfig(context);
		return std::move(catalog);
	}
	// Default IRC path - using OAuth2 to authorize to the catalog

	// Check no endpoint type has been passed.
	if (!endpoint_type.empty()) {
		throw InvalidConfigurationException("Unrecognized endpoint_type: %s. Expected either S3_TABLES or GLUE",
		                                    endpoint_type);
	}
	if (endpoint.empty()) {
		throw InvalidConfigurationException("No 'endpoint_type' or 'endpoint' provided");
	}

	// Check if any of the options are given that indicate intent to provide options inline, rather than use a secret
	bool user_intends_to_use_secret = true;
	if (!oauth2_scope.empty() || !oauth2_server_uri.empty() || !client_id.empty() || !client_secret.empty()) {
		user_intends_to_use_secret = false;
	}

	Value token;
	auto iceberg_secret = IRCatalog::GetIcebergSecret(context, secret, user_intends_to_use_secret);
	if (iceberg_secret) {
		//! The catalog secret (iceberg secret) will already have acquired a token, these additional settings in the
		//! attach options will not be used. Better to explicitly throw than to just ignore the options and cause
		//! confusion for the user.
		if (!oauth2_scope.empty()) {
			throw InvalidConfigurationException(
			    "Both an 'oauth2_scope' and a 'secret' are provided, these are mutually exclusive.");
		}
		if (!oauth2_server_uri.empty()) {
			throw InvalidConfigurationException("Both an 'oauth2_server_uri' and a 'secret' are "
			                                    "provided, these are mutually exclusive.");
		}
		if (!client_id.empty()) {
			throw InvalidConfigurationException(
			    "Please provide either a 'client_id'+'client_secret' pair, or 'secret', "
			    "these options are mutually exclusive");
		}
		if (!client_secret.empty()) {
			throw InvalidConfigurationException("Please provide either a client_id+client_secret pair, or 'secret', "
			                                    "these options are mutually exclusive");
		}

		auto &kv_iceberg_secret = dynamic_cast<const KeyValueSecret &>(*iceberg_secret->secret);
		token = kv_iceberg_secret.TryGetValue("token");
		oauth2_server_uri = kv_iceberg_secret.TryGetValue("token").ToString();
	} else {
		if (!secret.empty()) {
			throw InvalidConfigurationException("No ICEBERG secret by the name of '%s' could be found", secret);
		}

		if (oauth2_scope.empty()) {
			//! Default to the Polaris scope: 'PRINCIPAL_ROLE:ALL'
			oauth2_scope = "PRINCIPAL_ROLE:ALL";
		}

		if (grant_type.empty()) {
			grant_type = "client_credentials";
		}

		if (oauth2_server_uri.empty()) {
			//! If no oauth2_server_uri is provided, default to the (deprecated) REST API endpoint for it
			DUCKDB_LOG_WARN(context, "iceberg",
			                "'oauth2_server_uri' is not set, defaulting to deprecated '{endpoint}/v1/oauth/tokens' "
			                "oauth2_server_uri");
			oauth2_server_uri = StringUtil::Format("%s/v1/oauth/tokens", endpoint);
		}

		if (client_id.empty() || client_secret.empty()) {
			throw InvalidConfigurationException("Please provide either a 'client_id' + 'client_secret' pair or the "
			                                    "name of an ICEBERG secret as 'secret'");
		}

		CreateSecretInput create_secret_input;
		create_secret_input.options["oauth2_server_uri"] = oauth2_server_uri;
		create_secret_input.options["oauth2_scope"] = oauth2_scope;
		create_secret_input.options["oauth2_grant_type"] = grant_type;
		create_secret_input.options["client_id"] = client_id;
		create_secret_input.options["client_secret"] = client_secret;
		create_secret_input.options["endpoint"] = endpoint;
		create_secret_input.options["authorization_type"] = "oauth2";

		auto new_secret = IRCAuthorization::CreateCatalogSecretFunction(context, create_secret_input);
		auto &kv_iceberg_secret = dynamic_cast<KeyValueSecret &>(*new_secret);
		token = kv_iceberg_secret.TryGetValue("token");
	}
	if (token.IsNull()) {
		throw HTTPException(StringUtil::Format("Failed to retrieve oath token from %s", oauth2_server_uri));
	}
	credentials.token = token.ToString();

	auto catalog = make_uniq<IRCatalog>(db, access_mode, credentials, warehouse, endpoint, secret);
	catalog->GetConfig(context);
	return std::move(catalog);
}

static unique_ptr<TransactionManager> CreateTransactionManager(StorageExtensionInfo *storage_info, AttachedDatabase &db,
                                                               Catalog &catalog) {
	auto &ic_catalog = catalog.Cast<IRCatalog>();
	return make_uniq<ICTransactionManager>(db, ic_catalog);
}

class IRCStorageExtension : public StorageExtension {
public:
	IRCStorageExtension() {
		attach = IcebergCatalogAttach;
		create_transaction_manager = CreateTransactionManager;
	}
};

static void LoadInternal(DatabaseInstance &instance) {
	Aws::SDKOptions options;
	Aws::InitAPI(options); // Should only be called once.

	ExtensionHelper::AutoLoadExtension(instance, "avro");
	if (!instance.ExtensionIsLoaded("avro")) {
		throw MissingExtensionException("The iceberg extension requires the avro extension to be loaded!");
	}
	ExtensionHelper::AutoLoadExtension(instance, "parquet");
	if (!instance.ExtensionIsLoaded("parquet")) {
		throw MissingExtensionException("The iceberg extension requires the parquet extension to be loaded!");
	}

	auto &config = DBConfig::GetConfig(instance);

	config.AddExtensionOption("unsafe_enable_version_guessing",
	                          "Enable globbing the filesystem (if possible) to find the latest version metadata. This "
	                          "could result in reading an uncommitted version.",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(false));

	// Iceberg Table Functions
	for (auto &fun : IcebergFunctions::GetTableFunctions(instance)) {
		ExtensionUtil::RegisterFunction(instance, fun);
	}

	// Iceberg Scalar Functions
	for (auto &fun : IcebergFunctions::GetScalarFunctions()) {
		ExtensionUtil::RegisterFunction(instance, fun);
	}

	IRCAPI::InitializeCurl();

	SecretType secret_type;
	secret_type.name = "iceberg";
	secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	secret_type.default_provider = "config";

	ExtensionUtil::RegisterSecretType(instance, secret_type);
	CreateSecretFunction secret_function = {"iceberg", "config", IRCAuthorization::CreateCatalogSecretFunction};
	IRCAuthorization::SetCatalogSecretParameters(secret_function);
	ExtensionUtil::RegisterFunction(instance, secret_function);

	config.storage_extensions["iceberg"] = make_uniq<IRCStorageExtension>();
}

void IcebergExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string IcebergExtension::Name() {
	return "iceberg";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void iceberg_init(duckdb::DatabaseInstance &db) {
	LoadInternal(db);
}

DUCKDB_EXTENSION_API const char *iceberg_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
