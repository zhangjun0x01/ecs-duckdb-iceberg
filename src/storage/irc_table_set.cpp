#include "catalog_api.hpp"
#include "catalog_utils.hpp"

#include "storage/irc_catalog.hpp"
#include "storage/irc_table_set.hpp"
#include "storage/irc_transaction.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "storage/irc_schema_entry.hpp"
#include "duckdb/parser/parser.hpp"

namespace duckdb {

IcebergTableInformation::IcebergTableInformation(IRCatalog &catalog, IRCSchemaEntry &schema, const string &name)
    : catalog(catalog), schema(schema), name(name) {
	table_id = "uuid-" + schema.name + "-" + name;
}

optional_ptr<CatalogEntry> IcebergTableInformation::_CreateCatalogEntry(IcebergTableSchema &table_schema) {
	CreateTableInfo info;
	info.table = name;
	for (auto &col : table_schema.columns) {
		info.columns.AddColumn(ColumnDefinition(col->name, col->type));
	}

	auto table_entry = make_uniq<ICTableEntry>(*this, catalog, schema, info);
	if (!table_entry->internal) {
		table_entry->internal = schema.internal;
	}
	auto result = table_entry.get();
	if (result->name.empty()) {
		throw InternalException("ICTableSet::CreateEntry called with empty name");
	}
	schema_versions.emplace(table_schema.schema_id, std::move(table_entry));
	return result;
}

optional_ptr<CatalogEntry> IcebergTableInformation::GetSchemaVersion(optional_ptr<BoundAtClause> at) {
	D_ASSERT(!schema_versions.empty());
	auto snapshot_lookup = IcebergSnapshotLookup::FromAtClause(at);

	int32_t schema_id;
	if (snapshot_lookup.IsLatest()) {
		schema_id = table_metadata.current_schema_id;
	} else {
		auto snapshot = table_metadata.GetSnapshot(snapshot_lookup);
		D_ASSERT(snapshot);
		schema_id = snapshot->schema_id;
	}
	return schema_versions[schema_id].get();
}

ICTableSet::ICTableSet(IRCSchemaEntry &schema) : schema(schema), catalog(schema.ParentCatalog()) {
}

void ICTableSet::FillEntry(ClientContext &context, IcebergTableInformation &table) {
	if (!table.schema_versions.empty()) {
		//! Already filled
		return;
	}

	auto &ic_catalog = catalog.Cast<IRCatalog>();
	table.load_table_result = IRCAPI::GetTable(context, ic_catalog, schema.name, table.name);
	table.table_metadata = IcebergTableMetadata::FromTableMetadata(table.load_table_result.metadata);
	auto &schemas = table.table_metadata.schemas;

	//! It should be impossible to have a metadata file without any schema
	D_ASSERT(!schemas.empty());
	for (auto &table_schema : schemas) {
		table._CreateCatalogEntry(*table_schema.second);
	}
	table.current_schema_id = table.table_metadata.current_schema_id;
}

void ICTableSet::Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback) {
	lock_guard<mutex> l(entry_lock);
	LoadEntries(context);
	for (auto &entry : entries) {
		for (auto &schema : entry.second.schema_versions) {
			callback(*schema.second);
		}
	}
}

void ICTableSet::LoadEntries(ClientContext &context) {
	if (!entries.empty()) {
		return;
	}

	auto &ic_catalog = catalog.Cast<IRCatalog>();
	// TODO: handle out-of-order columns using position property
	auto tables = IRCAPI::GetTables(context, ic_catalog, schema.name);

	for (auto &table : tables) {
		entries.emplace(table.name, IcebergTableInformation(ic_catalog, schema, table.name));
	}
}

unique_ptr<ICTableInfo> ICTableSet::GetTableInfo(ClientContext &context, IRCSchemaEntry &schema,
                                                 const string &table_name) {
	throw NotImplementedException("ICTableSet::GetTableInfo");
}

optional_ptr<CatalogEntry> ICTableSet::GetEntry(ClientContext &context, const EntryLookupInfo &lookup) {
	LoadEntries(context);
	lock_guard<mutex> l(entry_lock);
	auto entry = entries.find(lookup.GetEntryName());
	if (entry == entries.end()) {
		return nullptr;
	}
	FillEntry(context, entry->second);
	return entry->second.GetSchemaVersion(lookup.GetAtClause());
}

} // namespace duckdb
