#include "catalog_api.hpp"
#include "catalog_utils.hpp"
#include "storage/irc_catalog.hpp"
#include "yyjson.hpp"
#include "iceberg_utils.hpp"
#include "request_utils.hpp"
#include <curl/curl.h>
#include <sys/stat.h>
#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/http/HttpClient.h>
#include <aws/core/http/HttpRequest.h>
#include <duckdb/main/secret/secret.hpp>
#include <duckdb/main/secret/secret_manager.hpp>

using namespace duckdb_yyjson;
namespace duckdb {



static string GetTableMetadata(ClientContext &context, IRCatalog &catalog, const string &schema, const string &table, const string &secret_name) {
	struct curl_slist *extra_headers = NULL;
	auto url = catalog.GetBaseUrl();
	url.AddPathComponent("namespaces");
	url.AddPathComponent(schema);
	url.AddPathComponent("tables");
	url.AddPathComponent(table);
	extra_headers = curl_slist_append(extra_headers, "X-Iceberg-Access-Delegation: vended-credentials");
	string api_result = RequestUtils::GetRequest(
		context,
		url,
		secret_name,
		catalog.credentials.token,
		extra_headers);

	catalog.SetCachedValue(url.GetURL(), api_result);
	curl_slist_free_all(extra_headers);
	return api_result;
}

static string GetTableMetadataCached(ClientContext &context, IRCatalog &catalog, const string &schema, const string &table, const string &secret_name) {
	struct curl_slist *extra_headers = NULL;
	auto url = catalog.GetBaseUrl();
	url.AddPathComponent("namespaces");
	url.AddPathComponent(schema);
	url.AddPathComponent("tables");
	url.AddPathComponent(table);
	if (catalog.HasCachedValue(url.GetURL())) {
		return catalog.GetCachedValue(url.GetURL());
	}
	return GetTableMetadata(context, catalog, schema, table, secret_name);
}

void IRCAPI::InitializeCurl() {
	RequestUtils::SelectCurlCertPath();
}

vector<string> IRCAPI::GetCatalogs(ClientContext &context, IRCatalog &catalog, IRCCredentials credentials) {
	throw NotImplementedException("ICAPI::GetCatalogs");
}

static IRCAPIColumnDefinition ParseColumnDefinition(yyjson_val *column_def) {
	IRCAPIColumnDefinition result;
	result.name = IcebergUtils::TryGetStrFromObject(column_def, "name");
	result.type_text = IcebergUtils::TryGetStrFromObject(column_def, "type");
	result.precision = (result.type_text == "decimal") ? IcebergUtils::TryGetNumFromObject(column_def, "type_precision") : -1;
	result.scale = (result.type_text == "decimal") ? IcebergUtils::TryGetNumFromObject(column_def, "type_scale") : -1;
	result.position = IcebergUtils::TryGetNumFromObject(column_def, "id") - 1;
	return result;
}

IRCAPITableCredentials IRCAPI::GetTableCredentials(ClientContext &context, IRCatalog &catalog, const string &schema, const string &table, IRCCredentials credentials) {
	IRCAPITableCredentials result;
	string api_result = GetTableMetadataCached(context, catalog, schema, table, catalog.secret_name);
	std::unique_ptr<yyjson_doc, YyjsonDocDeleter> doc(ICUtils::api_result_to_doc(api_result));
	auto *root = yyjson_doc_get_root(doc.get());
	auto *warehouse_credentials = yyjson_obj_get(root, "config");
	auto credential_size = yyjson_obj_size(warehouse_credentials);
	auto catalog_credentials = IRCatalog::GetSecret(context, catalog.secret_name);
	if (warehouse_credentials && credential_size > 0) {
		result.key_id = IcebergUtils::TryGetStrFromObject(warehouse_credentials, "s3.access-key-id", false);
		result.secret = IcebergUtils::TryGetStrFromObject(warehouse_credentials, "s3.secret-access-key",  false);
		result.session_token = IcebergUtils::TryGetStrFromObject(warehouse_credentials, "s3.session-token", false);
		if (catalog_credentials) {
       		auto kv_secret = dynamic_cast<const KeyValueSecret &>(*catalog_credentials->secret);
			auto region = kv_secret.TryGetValue("region").ToString();
			result.region = region;
		}
	}
	return result;
}

string IRCAPI::GetToken(ClientContext &context, string id, string secret, string endpoint) {
	string post_data = "grant_type=client_credentials&client_id=" + id + "&client_secret=" + secret + "&scope=PRINCIPAL_ROLE:ALL";
	string api_result = RequestUtils::PostRequest(context, endpoint + "/v1/oauth/tokens", post_data);
	std::unique_ptr<yyjson_doc, YyjsonDocDeleter> doc(ICUtils::api_result_to_doc(api_result));
	auto *root = yyjson_doc_get_root(doc.get());
	return IcebergUtils::TryGetStrFromObject(root, "access_token");
}

static void populateTableMetadata(IRCAPITable &table, yyjson_val *metadata_root) {
	table.storage_location = IcebergUtils::TryGetStrFromObject(metadata_root, "metadata-location");
	auto *metadata = yyjson_obj_get(metadata_root, "metadata");
	//table_result.table_id = IcebergUtils::TryGetStrFromObject(metadata, "table-uuid");

	uint64_t current_schema_id = IcebergUtils::TryGetNumFromObject(metadata, "current-schema-id");
	auto *schemas = yyjson_obj_get(metadata, "schemas");
	yyjson_val *schema;
	size_t schema_idx, schema_max;
	bool found = false;
	yyjson_arr_foreach(schemas, schema_idx, schema_max, schema) {
		uint64_t schema_id = IcebergUtils::TryGetNumFromObject(schema, "schema-id");
		if (schema_id == current_schema_id) {
			found = true;
			auto *columns = yyjson_obj_get(schema, "fields");
			yyjson_val *col;
			size_t col_idx, col_max;
			yyjson_arr_foreach(columns, col_idx, col_max, col) {
				auto column_definition = ParseColumnDefinition(col);
				table.columns.push_back(column_definition);
			}
		}
	}

	if (!found) {
		throw InternalException("Current schema not found");
	}
}

static IRCAPITable createTable(IRCatalog &catalog, const string &schema, const string &table_name) {
	IRCAPITable table_result;
	table_result.catalog_name = catalog.GetName();
	table_result.schema_name = schema;
	table_result.name = table_name;
	table_result.data_source_format = "ICEBERG";
	table_result.table_id = "uuid-" + schema + "-" + "table";
	std::replace(table_result.table_id.begin(), table_result.table_id.end(), '_', '-');
	return table_result;
}

IRCAPITable IRCAPI::GetTable(ClientContext &context,
	IRCatalog &catalog, const string &schema, const string &table_name, optional_ptr<IRCCredentials> credentials) {
	IRCAPITable table_result = createTable(catalog, schema, table_name);
	if (credentials) {
		string result = GetTableMetadata(context, catalog, schema, table_result.name, catalog.secret_name);
		std::unique_ptr<yyjson_doc, YyjsonDocDeleter> doc(ICUtils::api_result_to_doc(result));
		auto *metadata_root = yyjson_doc_get_root(doc.get());
		populateTableMetadata(table_result, metadata_root);
	} else {
		// Skip fetching metadata, we'll do it later when we access the table
		IRCAPIColumnDefinition col;
		col.name = "__";
		col.type_text = "int";
		col.precision = -1;
		col.scale = -1;
		col.position = 0;
		table_result.columns.push_back(col);
	}

	return table_result;
}

// TODO: handle out-of-order columns using position property
vector<IRCAPITable> IRCAPI::GetTables(ClientContext &context, IRCatalog &catalog, const string &schema) {
	vector<IRCAPITable> result;
	auto url = catalog.GetBaseUrl();
	url.AddPathComponent("namespaces");
	url.AddPathComponent(schema);
	url.AddPathComponent("tables");
	string api_result = RequestUtils::GetRequest(context, url, catalog.secret_name, catalog.credentials.token);
	std::unique_ptr<yyjson_doc, YyjsonDocDeleter> doc(ICUtils::api_result_to_doc(api_result));
	auto *root = yyjson_doc_get_root(doc.get());
	auto *tables = yyjson_obj_get(root, "identifiers");
	size_t idx, max;
	yyjson_val *table;
	yyjson_arr_foreach(tables, idx, max, table) {
		auto table_result = GetTable(context, catalog, schema, IcebergUtils::TryGetStrFromObject(table, "name"), nullptr);
		result.push_back(table_result);
	}

	return result;
}

vector<IRCAPISchema> IRCAPI::GetSchemas(ClientContext &context, IRCatalog &catalog, IRCCredentials credentials) {
	vector<IRCAPISchema> result;
	auto endpoint_builder = catalog.GetBaseUrl();
	endpoint_builder.AddPathComponent("namespaces");
	string api_result =
	    RequestUtils::GetRequest(context, endpoint_builder, catalog.secret_name, catalog.credentials.token);
	std::unique_ptr<yyjson_doc, YyjsonDocDeleter> doc(ICUtils::api_result_to_doc(api_result));
	auto *root = yyjson_doc_get_root(doc.get());
	auto *schemas = yyjson_obj_get(root, "namespaces");
	size_t idx, max;
	yyjson_val *schema;
	yyjson_arr_foreach(schemas, idx, max, schema) {
		IRCAPISchema schema_result;
		schema_result.catalog_name = catalog.GetName();
		yyjson_val *value = yyjson_arr_get(schema, 0);
		schema_result.schema_name = yyjson_get_str(value);
		result.push_back(schema_result);
	}

	return result;
}

IRCAPISchema IRCAPI::CreateSchema(ClientContext &context, IRCatalog &catalog, const string &internal, const string &schema, IRCCredentials credentials) {
	throw NotImplementedException("IRCAPI::Create Schema not Implemented");
}

void IRCAPI::DropSchema(ClientContext &context, const string &internal, const string &schema, IRCCredentials credentials) {
	throw NotImplementedException("IRCAPI Drop Schema not Implemented");
}

void IRCAPI::DropTable(ClientContext &context, IRCatalog &catalog, const string &internal, const string &schema, string &table_name, IRCCredentials credentials) {
	throw NotImplementedException("IRCAPI Drop Table not Implemented");
}

static std::string json_to_string(yyjson_mut_doc *doc, yyjson_write_flag flags = YYJSON_WRITE_PRETTY) {
    char *json_chars = yyjson_mut_write(doc, flags, NULL);
    std::string json_str(json_chars);
    free(json_chars);
    return json_str;
}

IRCAPITable IRCAPI::CreateTable(ClientContext &context, IRCatalog &catalog, const string &internal, const string &schema, IRCCredentials credentials, CreateTableInfo *table_info) {
	throw NotImplementedException("IRCAPI Create Table not Implemented");
}

} // namespace duckdb
