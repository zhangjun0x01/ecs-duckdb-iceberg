
#pragma once

#include "storage/irc_catalog_set.hpp"
#include "storage/irc_schema_entry.hpp"

namespace duckdb {
struct CreateSchemaInfo;

class IRCSchemaSet : public IRCCatalogSet {
public:
	explicit IRCSchemaSet(Catalog &catalog);

public:
	optional_ptr<CatalogEntry> CreateSchema(ClientContext &context, CreateSchemaInfo &info);
	void DropSchema(ClientContext &context, DropInfo &info);

protected:
	void LoadEntries(ClientContext &context) override;
	void FillEntry(ClientContext &context, unique_ptr<CatalogEntry> &entry) override;
};

} // namespace duckdb
