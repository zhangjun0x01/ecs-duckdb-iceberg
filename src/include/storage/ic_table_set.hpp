
#pragma once

#include "storage/ic_catalog_set.hpp"
#include "storage/ic_table_entry.hpp"

namespace duckdb {
struct CreateTableInfo;
class UCResult;
class IBSchemaEntry;

class UCTableSet : public UCInSchemaSet {
public:
	explicit UCTableSet(IBSchemaEntry &schema);

public:
	optional_ptr<CatalogEntry> CreateTable(ClientContext &context, BoundCreateTableInfo &info);

	static unique_ptr<UCTableInfo> GetTableInfo(ClientContext &context, IBSchemaEntry &schema,
	                                            const string &table_name);
	optional_ptr<CatalogEntry> RefreshTable(ClientContext &context, const string &table_name);

	void AlterTable(ClientContext &context, AlterTableInfo &info);

protected:
	void LoadEntries(ClientContext &context) override;

	void AlterTable(ClientContext &context, RenameTableInfo &info);
	void AlterTable(ClientContext &context, RenameColumnInfo &info);
	void AlterTable(ClientContext &context, AddColumnInfo &info);
	void AlterTable(ClientContext &context, RemoveColumnInfo &info);

	static void AddColumn(ClientContext &context, UCResult &result, UCTableInfo &table_info, idx_t column_offset = 0);
};

} // namespace duckdb
