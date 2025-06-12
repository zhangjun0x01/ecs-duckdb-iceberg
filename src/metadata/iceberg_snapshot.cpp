#include "metadata/iceberg_snapshot.hpp"
#include "metadata/iceberg_table_metadata.hpp"

namespace duckdb {

rest_api_objects::Snapshot IcebergSnapshot::ToRESTObject() {
	rest_api_objects::Snapshot res;

	res.snapshot_id = snapshot_id;
	res.timestamp_ms = Timestamp::GetEpochMs(timestamp_ms);
	res.manifest_list = manifest_list;
	//! FIXME: does this mean we can't mix multiple UPDATE / DELETE / INSERTS in the same transaction???
	//! valid values are:
	// - append
	// - replace
	// - overwrite
	// - delete
	res.summary.operation = "append";

	//! TODO: add the parent snapshot id to IcebergSnapshot
	res.has_parent_snapshot_id = false;

	res.has_sequence_number = true;
	res.sequence_number = sequence_number;

	res.has_schema_id = true;
	res.schema_id = schema_id;

	return res;
}

IcebergSnapshot IcebergSnapshot::ParseSnapshot(rest_api_objects::Snapshot &snapshot, IcebergTableMetadata &metadata) {
	IcebergSnapshot ret;
	if (metadata.iceberg_version == 1) {
		ret.sequence_number = 0;
	} else if (metadata.iceberg_version == 2) {
		D_ASSERT(snapshot.has_sequence_number);
		ret.sequence_number = snapshot.sequence_number;
	}

	ret.snapshot_id = snapshot.snapshot_id;
	ret.timestamp_ms = Timestamp::FromEpochMs(snapshot.timestamp_ms);
	D_ASSERT(snapshot.has_schema_id);
	ret.schema_id = snapshot.schema_id;
	ret.manifest_list = snapshot.manifest_list;
	return ret;
}

} // namespace duckdb
