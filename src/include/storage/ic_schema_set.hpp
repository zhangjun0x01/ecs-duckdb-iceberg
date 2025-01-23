
#pragma once

#include "storage/ic_catalog_set.hpp"
#include "storage/ic_schema_entry.hpp"

namespace duckdb {
struct CreateSchemaInfo;

class ICSchemaSet : public ICCatalogSet {
public:
	explicit ICSchemaSet(Catalog &catalog);

public:
	optional_ptr<CatalogEntry> CreateSchema(ClientContext &context, CreateSchemaInfo &info);
	void DropSchema(ClientContext &context, DropInfo &info);

protected:
	void LoadEntries(ClientContext &context) override;
	void FillEntry(ClientContext &context, unique_ptr<CatalogEntry> &entry) override;
};

} // namespace duckdb
