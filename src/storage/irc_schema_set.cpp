#include "catalog_api.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "storage/irc_catalog.hpp"
#include "storage/irc_schema_set.hpp"
#include "storage/irc_transaction.hpp"

namespace duckdb {

ICSchemaSet::ICSchemaSet(Catalog &catalog) : ICCatalogSet(catalog) {
}

void ICSchemaSet::LoadEntries(ClientContext &context) {
	if (!entries.empty()) {
		return;
	}

	auto &ic_catalog = catalog.Cast<IRCatalog>();
	auto schemas = ICRAPI::GetSchemas(catalog.GetName(), ic_catalog.internal_name, ic_catalog.credentials);
	for (const auto &schema : schemas) {
		CreateSchemaInfo info;
		info.schema = schema.schema_name;
		info.internal = false;
		auto schema_entry = make_uniq<ICSchemaEntry>(catalog, info);
		schema_entry->schema_data = make_uniq<ICRAPISchema>(schema);
		CreateEntry(std::move(schema_entry));
	}
}

void ICSchemaSet::FillEntry(ClientContext &context, unique_ptr<CatalogEntry> &entry) {
	// Nothing to do
}

optional_ptr<CatalogEntry> ICSchemaSet::CreateSchema(ClientContext &context, CreateSchemaInfo &info) {
	auto &ic_catalog = catalog.Cast<IRCatalog>();
	auto schema = ICRAPI::CreateSchema(catalog.GetName(), ic_catalog.internal_name, info.schema, ic_catalog.credentials);
	auto schema_entry = make_uniq<ICSchemaEntry>(catalog, info);
	schema_entry->schema_data = make_uniq<ICRAPISchema>(schema);
	return CreateEntry(std::move(schema_entry));
}

void ICSchemaSet::DropSchema(ClientContext &context, DropInfo &info) {
	auto &ic_catalog = catalog.Cast<IRCatalog>();
	ICRAPI::DropSchema(ic_catalog.internal_name, info.name, ic_catalog.credentials);
	DropEntry(context, info);
}

} // namespace duckdb
