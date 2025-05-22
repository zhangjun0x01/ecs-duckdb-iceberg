
#pragma once

#include "storage/irc_table_entry.hpp"

namespace duckdb {
struct CreateTableInfo;
class ICResult;
class IRCSchemaEntry;

struct IcebergTableInformation {
public:
	IcebergTableInformation(IRCatalog &catalog, IRCSchemaEntry &schema, const string &name);

public:
	optional_ptr<CatalogEntry> GetSchemaVersion(optional_ptr<BoundAtClause> at);
	optional_ptr<CatalogEntry> _CreateCatalogEntry(IcebergTableSchema &table_schema);

public:
	IRCatalog &catalog;
	IRCSchemaEntry &schema;
	string name;
	string table_id;

	rest_api_objects::LoadTableResult load_table_result;
	IcebergTableMetadata table_metadata;
	unordered_map<int32_t, unique_ptr<ICTableEntry>> schema_versions;
	int32_t current_schema_id;
};

class ICTableSet {
public:
	explicit ICTableSet(IRCSchemaEntry &schema);

public:
	static unique_ptr<ICTableInfo> GetTableInfo(ClientContext &context, IRCSchemaEntry &schema,
	                                            const string &table_name);
	optional_ptr<CatalogEntry> GetEntry(ClientContext &context, const EntryLookupInfo &lookup);
	void Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback);

protected:
	void LoadEntries(ClientContext &context);
	void FillEntry(ClientContext &context, IcebergTableInformation &table);

protected:
	IRCSchemaEntry &schema;
	Catalog &catalog;
	case_insensitive_map_t<IcebergTableInformation> entries;

private:
	mutex entry_lock;
};

} // namespace duckdb
