
#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

namespace duckdb {
struct ICRCredentials;

struct ICRAPIColumnDefinition {
	string name;
	string type_text;
	idx_t precision;
	idx_t scale;
	idx_t position;
};

struct ICRAPITable {
	string table_id;

	string name;
	string catalog_name;
	string schema_name;
	string table_type;
	string data_source_format;
	string storage_location;

	vector<ICRAPIColumnDefinition> columns;
};

struct ICRAPISchema {
	string schema_name;
	string catalog_name;
};

struct ICAPITableCredentials {
	string key_id;
	string secret;
	string session_token;
};

class ICRAPI {
public:
	static const string API_VERSION_1;

  	//! WARNING: not thread-safe. To be called once on extension initialization
  	static void InitializeCurl();

	// The {prefix} for a catalog is always optional according to the iceberg spec. So no need to
	// add it if it is not defined.
	static string GetOptionallyPrefixedURL(const string &api_version, const string &prefix);
	static ICAPITableCredentials GetTableCredentials(const string &internal, const string &schema, const string &table, ICRCredentials credentials);
	static vector<string> GetCatalogs(const string &catalog, ICRCredentials credentials);
	static vector<ICRAPITable> GetTables(const string &catalog, const string &internal, const string &schema, ICRCredentials credentials);
	static ICRAPITable GetTable(const string &catalog, const string &internal, const string &schema, const string &table_name, std::optional<ICRCredentials> credentials);
	static vector<ICRAPISchema> GetSchemas(const string &catalog, const string &internal, ICRCredentials credentials);
	static vector<ICRAPITable> GetTablesInSchema(const string &catalog, const string &schema, ICRCredentials credentials);
	static string GetToken(string id, string secret, string endpoint);
	static ICRAPISchema CreateSchema(const string &catalog, const string &internal, const string &schema, ICRCredentials credentials);
	static void DropSchema(const string &internal, const string &schema, ICRCredentials credentials);
	static ICRAPITable CreateTable(const string &catalog, const string &internal, const string &schema, ICRCredentials credentials, CreateTableInfo *table_info);
	static void DropTable(const string &catalog, const string &internal, const string &schema, string &table_name, ICRCredentials credentials);
};

} // namespace duckdb
