#include "storage/ic_schema_set.hpp"
#include "storage/ic_catalog.hpp"
#include "catalog_api.hpp"
#include "storage/ic_transaction.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/catalog/catalog.hpp"

#include <iostream>

namespace duckdb {

IBSchemaSet::IBSchemaSet(Catalog &catalog) : IBCatalogSet(catalog), is_loaded(false) {
}

static bool IsInternalTable(const string &catalog, const string &schema) {
	if (schema == "information_schema") {
		return true;
	}
	return false;
}

void IBSchemaSet::LoadEntries(ClientContext &context) {
	if (is_loaded) {
		return;
	}

	is_loaded = true;
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
	// TODO: throw NotImplementedException("Schema creation");
	std::cout << " >> Create schema" << std::endl;
	// CreateEntry(...)
}

void IBSchemaSet::DropSchema(ClientContext &context, DropInfo &info) {
	// TODO
	std::cout << " >> Drop schema" << std::endl;

	DropEntry(context, info);
}


} // namespace duckdb
