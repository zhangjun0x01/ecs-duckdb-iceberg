
#pragma once

#include "storage/ic_catalog_set.hpp"
#include "storage/ic_schema_entry.hpp"

namespace duckdb {
struct CreateSchemaInfo;

class UCSchemaSet : public IBCatalogSet {
public:
	explicit UCSchemaSet(Catalog &catalog);

public:
	optional_ptr<CatalogEntry> CreateSchema(ClientContext &context, CreateSchemaInfo &info);

protected:
	void LoadEntries(ClientContext &context) override;
};

} // namespace duckdb
