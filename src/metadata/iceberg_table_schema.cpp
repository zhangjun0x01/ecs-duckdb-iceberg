#include "metadata/iceberg_table_schema.hpp"

#include "iceberg_metadata.hpp"
#include "iceberg_utils.hpp"
#include "rest_catalog/objects/list.hpp"

namespace duckdb {

shared_ptr<IcebergTableSchema> IcebergTableSchema::ParseSchema(rest_api_objects::Schema &schema) {
	auto res = make_shared_ptr<IcebergTableSchema>();
	res->schema_id = schema.object_1.schema_id;
	for (auto &field : schema.struct_type.fields) {
		res->columns.push_back(IcebergColumnDefinition::ParseStructField(*field));
	}
	return res;
}

} // namespace duckdb
