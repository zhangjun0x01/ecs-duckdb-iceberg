#pragma once

#include "metadata/iceberg_column_definition.hpp"
#include "rest_catalog/objects/schema.hpp"

namespace duckdb {

class IcebergTableSchema {
public:
	static shared_ptr<IcebergTableSchema> ParseSchema(rest_api_objects::Schema &schema);

public:
	int32_t schema_id;
	vector<unique_ptr<IcebergColumnDefinition>> columns;
};

} // namespace duckdb
