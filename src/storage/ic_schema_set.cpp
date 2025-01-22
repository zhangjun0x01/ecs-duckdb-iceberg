#include "catalog_api.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "storage/ic_catalog.hpp"
#include "storage/ic_schema_set.hpp"
#include "storage/ic_transaction.hpp"

namespace duckdb {

IBSchemaSet::IBSchemaSet(Catalog &catalog) : IBCatalogSet(catalog) {
}

static bool IsInternalTable(const string &catalog, const string &schema) {
	if (schema == "information_schema") {
		return true;
	}
	return false;
}

void IBSchemaSet::LoadEntries(ClientContext &context) {
	if (!entries.empty()) {
		return;
	}

	auto &ic_catalog = catalog.Cast<IBCatalog>();
	auto schemas = IBAPI::GetSchemas(catalog.GetName(), ic_catalog.internal_name, ic_catalog.credentials);
	for (const auto &schema : schemas) {
		CreateSchemaInfo info;
		info.schema = schema.schema_name;
		info.internal = IsInternalTable(schema.catalog_name, schema.schema_name);
		auto schema_entry = make_uniq<IBSchemaEntry>(catalog, info);
		schema_entry->schema_data = make_uniq<IBAPISchema>(schema);
		CreateEntry(std::move(schema_entry));
	}
}

void IBSchemaSet::FillEntry(ClientContext &context, unique_ptr<CatalogEntry> &entry) {
	// Nothing to do
}

optional_ptr<CatalogEntry> IBSchemaSet::CreateSchema(ClientContext &context, CreateSchemaInfo &info) {
	auto &ic_catalog = catalog.Cast<IBCatalog>();
	auto schema = IBAPI::CreateSchema(catalog.GetName(), ic_catalog.internal_name, info.schema, ic_catalog.credentials);
	auto schema_entry = make_uniq<IBSchemaEntry>(catalog, info);
	schema_entry->schema_data = make_uniq<IBAPISchema>(schema);
	return CreateEntry(std::move(schema_entry));
}

void IBSchemaSet::DropSchema(ClientContext &context, DropInfo &info) {
	auto &ic_catalog = catalog.Cast<IBCatalog>();
	IBAPI::DropSchema(ic_catalog.internal_name, info.name, ic_catalog.credentials);
	DropEntry(context, info);
}

} // namespace duckdb
