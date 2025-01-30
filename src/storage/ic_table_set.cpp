#include "catalog_api.hpp"
#include "catalog_utils.hpp"

#include "storage/ic_catalog.hpp"
#include "storage/ic_table_set.hpp"
#include "storage/ic_transaction.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "storage/ic_schema_entry.hpp"
#include "duckdb/parser/parser.hpp"

namespace duckdb {

ICTableSet::ICTableSet(ICSchemaEntry &schema) : ICInSchemaSet(schema) {
}

static ColumnDefinition CreateColumnDefinition(ClientContext &context, ICRAPIColumnDefinition &coldef) {
	return {coldef.name, ICUtils::TypeToLogicalType(context, coldef.type_text)};
}

unique_ptr<CatalogEntry> ICTableSet::_CreateCatalogEntry(ClientContext &context, ICRAPITable table) {
	D_ASSERT(schema.name == table.schema_name);
	CreateTableInfo info;
	info.table = table.name;

	for (auto &col : table.columns) {
		info.columns.AddColumn(CreateColumnDefinition(context, col));
	}

	auto table_entry = make_uniq<ICTableEntry>(catalog, schema, info);
	table_entry->table_data = make_uniq<ICRAPITable>(table);
	return table_entry;
}

void ICTableSet::FillEntry(ClientContext &context, unique_ptr<CatalogEntry> &entry) {
	auto* derived = static_cast<ICTableEntry*>(entry.get());
	if (!derived->table_data->storage_location.empty()) {
		return;
	}
		
	auto &ic_catalog = catalog.Cast<ICRCatalog>();
	auto table = ICRAPI::GetTable(catalog.GetName(), catalog.GetDBPath(), schema.name, entry->name, ic_catalog.credentials);
	entry = _CreateCatalogEntry(context, table);
}

void ICTableSet::LoadEntries(ClientContext &context) {
	if (!entries.empty()) {
		return;
	}

	auto &ic_catalog = catalog.Cast<ICRCatalog>();
	// TODO: handle out-of-order columns using position property
	auto tables = ICRAPI::GetTables(catalog.GetName(), catalog.GetDBPath(), schema.name, ic_catalog.credentials);

	for (auto &table : tables) {
		auto entry = _CreateCatalogEntry(context, table);
		CreateEntry(std::move(entry));
	}
}

optional_ptr<CatalogEntry> ICTableSet::RefreshTable(ClientContext &context, const string &table_name) {
	auto table_info = GetTableInfo(context, schema, table_name);
	auto table_entry = make_uniq<ICTableEntry>(catalog, schema, *table_info);
	auto table_ptr = table_entry.get();
	CreateEntry(std::move(table_entry));
	return table_ptr;
}

unique_ptr<ICTableInfo> ICTableSet::GetTableInfo(ClientContext &context, ICSchemaEntry &schema,
                                                 const string &table_name) {
	throw NotImplementedException("ICTableSet::GetTableInfo");
}

optional_ptr<CatalogEntry> ICTableSet::CreateTable(ClientContext &context, BoundCreateTableInfo &info) {
	auto &ic_catalog = catalog.Cast<ICRCatalog>();
	auto *table_info = dynamic_cast<CreateTableInfo *>(info.base.get());
	auto table = ICRAPI::CreateTable(catalog.GetName(), ic_catalog.internal_name, schema.name, ic_catalog.credentials, table_info);
	auto entry = _CreateCatalogEntry(context, table);
	return CreateEntry(std::move(entry));
}

void ICTableSet::DropTable(ClientContext &context, DropInfo &info) {
	auto &ic_catalog = catalog.Cast<ICRCatalog>();
	ICRAPI::DropTable(catalog.GetName(), ic_catalog.internal_name, schema.name, info.name, ic_catalog.credentials);	
}

void ICTableSet::AlterTable(ClientContext &context, RenameTableInfo &info) {
	throw NotImplementedException("ICTableSet::AlterTable");
}

void ICTableSet::AlterTable(ClientContext &context, RenameColumnInfo &info) {
	throw NotImplementedException("ICTableSet::AlterTable");
}

void ICTableSet::AlterTable(ClientContext &context, AddColumnInfo &info) {
	throw NotImplementedException("ICTableSet::AlterTable");
}

void ICTableSet::AlterTable(ClientContext &context, RemoveColumnInfo &info) {
	throw NotImplementedException("ICTableSet::AlterTable");
}

void ICTableSet::AlterTable(ClientContext &context, AlterTableInfo &alter) {
	throw NotImplementedException("ICTableSet::AlterTable");
}

} // namespace duckdb
