#pragma once

#include "metadata/iceberg_column_definition.hpp"
#include "rest_catalog/objects/snapshot.hpp"

namespace duckdb {

struct IcebergTableMetadata;

//! An Iceberg snapshot https://iceberg.apache.org/spec/#snapshots
class IcebergSnapshot {
public:
	IcebergSnapshot() {
	}
	static IcebergSnapshot ParseSnapshot(rest_api_objects::Snapshot &snapshot, IcebergTableMetadata &metadata);

public:
	bool IsEmptySnapshot() {
		return snapshot_id == NumericLimits<int32_t>::Maximum();
	}

public:
	//! Snapshot metadata
	int32_t snapshot_id = NumericLimits<int32_t>::Maximum();
	int64_t sequence_number;
	int32_t schema_id;
	timestamp_t timestamp_ms;
	string manifest_list;
};

} // namespace duckdb
