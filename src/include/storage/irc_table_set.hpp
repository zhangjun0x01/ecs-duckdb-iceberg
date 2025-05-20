
#pragma once

#include "storage/irc_table_entry.hpp"

namespace duckdb {
struct CreateTableInfo;
class ICResult;
class IRCSchemaEntry;

class ICTableSet {
public:
	explicit ICTableSet(IRCSchemaEntry &schema);

public:
	optional_ptr<CatalogEntry> CreateTable(ClientContext &context, BoundCreateTableInfo &info);
	static unique_ptr<ICTableInfo> GetTableInfo(ClientContext &context, IRCSchemaEntry &schema,
	                                            const string &table_name);
	void AlterTable(ClientContext &context, AlterTableInfo &info);
	void DropTable(ClientContext &context, DropInfo &info);
	optional_ptr<CatalogEntry> GetEntry(ClientContext &context, const string &name);
	void DropEntry(ClientContext &context, DropInfo &info);
	void Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback);

protected:
	optional_ptr<CatalogEntry> CreateEntryInternal(ClientContext &context, unique_ptr<CatalogEntry> entry);

	void LoadEntries(ClientContext &context);
	void FillEntry(ClientContext &context, unique_ptr<CatalogEntry> &entry);

	void AlterTable(ClientContext &context, RenameTableInfo &info);
	void AlterTable(ClientContext &context, RenameColumnInfo &info);
	void AlterTable(ClientContext &context, AddColumnInfo &info);
	void AlterTable(ClientContext &context, RemoveColumnInfo &info);

private:
	unique_ptr<CatalogEntry> _CreateCatalogEntry(ClientContext &context, IRCAPITable &&table);

protected:
	IRCSchemaEntry &schema;
	Catalog &catalog;
	case_insensitive_map_t<unique_ptr<CatalogEntry>> entries;

private:
	mutex entry_lock;
};

} // namespace duckdb
