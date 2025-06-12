#include "metadata/iceberg_manifest.hpp"

namespace duckdb {

Value IcebergManifestEntry::ToDataFileStruct(const LogicalType &type) const {
	vector<Value> children;

	// content: int - 134
	children.push_back(Value::INTEGER(static_cast<int32_t>(content)));
	// file_path: string - 100
	children.push_back(Value(file_path));
	// file_format: string - 101
	children.push_back(Value(file_format));
	// partition: struct(...) - 102
	children.push_back(partition);
	// record_count: long - 103
	children.push_back(Value::BIGINT(record_count));
	// file_size_in_bytes: long - 104
	children.push_back(Value::BIGINT(file_size_in_bytes));

	return Value::STRUCT(type, children);
}

} // namespace duckdb
