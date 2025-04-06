
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/metadata_log.hpp"
#include "rest_catalog/objects/partition_spec.hpp"
#include "rest_catalog/objects/partition_statistics_file.hpp"
#include "rest_catalog/objects/schema.hpp"
#include "rest_catalog/objects/snapshot.hpp"
#include "rest_catalog/objects/snapshot_log.hpp"
#include "rest_catalog/objects/snapshot_references.hpp"
#include "rest_catalog/objects/sort_order.hpp"
#include "rest_catalog/objects/statistics_file.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class TableMetadata {
public:
	TableMetadata();

public:
	static TableMetadata FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	int64_t format_version;
	string table_uuid;
	string location;
	int64_t last_updated_ms;
	case_insensitive_map_t<string> properties;
	vector<Schema> schemas;
	int64_t current_schema_id;
	int64_t last_column_id;
	vector<PartitionSpec> partition_specs;
	int64_t default_spec_id;
	int64_t last_partition_id;
	vector<SortOrder> sort_orders;
	int64_t default_sort_order_id;
	vector<Snapshot> snapshots;
	SnapshotReferences refs;
	int64_t current_snapshot_id;
	int64_t last_sequence_number;
	SnapshotLog snapshot_log;
	MetadataLog metadata_log;
	vector<StatisticsFile> statistics;
	vector<PartitionStatisticsFile> partition_statistics;
};

} // namespace rest_api_objects
} // namespace duckdb
