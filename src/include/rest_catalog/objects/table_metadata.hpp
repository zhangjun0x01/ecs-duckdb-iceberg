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
	static TableMetadata FromJSON(yyjson_val *obj) {
		TableMetadata result;

		auto current_schema_id_val = yyjson_obj_get(obj, "current-schema-id");
		if (current_schema_id_val) {
			result.current_schema_id = yyjson_get_sint(current_schema_id_val);
		}

		auto current_snapshot_id_val = yyjson_obj_get(obj, "current-snapshot-id");
		if (current_snapshot_id_val) {
			result.current_snapshot_id = yyjson_get_sint(current_snapshot_id_val);
		}

		auto default_sort_order_id_val = yyjson_obj_get(obj, "default-sort-order-id");
		if (default_sort_order_id_val) {
			result.default_sort_order_id = yyjson_get_sint(default_sort_order_id_val);
		}

		auto default_spec_id_val = yyjson_obj_get(obj, "default-spec-id");
		if (default_spec_id_val) {
			result.default_spec_id = yyjson_get_sint(default_spec_id_val);
		}

		auto format_version_val = yyjson_obj_get(obj, "format-version");
		if (format_version_val) {
			result.format_version = yyjson_get_sint(format_version_val);
		}
		else {
			throw IOException("TableMetadata required property 'format-version' is missing");
		}

		auto last_column_id_val = yyjson_obj_get(obj, "last-column-id");
		if (last_column_id_val) {
			result.last_column_id = yyjson_get_sint(last_column_id_val);
		}

		auto last_partition_id_val = yyjson_obj_get(obj, "last-partition-id");
		if (last_partition_id_val) {
			result.last_partition_id = yyjson_get_sint(last_partition_id_val);
		}

		auto last_sequence_number_val = yyjson_obj_get(obj, "last-sequence-number");
		if (last_sequence_number_val) {
			result.last_sequence_number = yyjson_get_sint(last_sequence_number_val);
		}

		auto last_updated_ms_val = yyjson_obj_get(obj, "last-updated-ms");
		if (last_updated_ms_val) {
			result.last_updated_ms = yyjson_get_sint(last_updated_ms_val);
		}

		auto location_val = yyjson_obj_get(obj, "location");
		if (location_val) {
			result.location = yyjson_get_str(location_val);
		}

		auto metadata_log_val = yyjson_obj_get(obj, "metadata-log");
		if (metadata_log_val) {
			result.metadata_log = MetadataLog::FromJSON(metadata_log_val);
		}

		auto partition_specs_val = yyjson_obj_get(obj, "partition-specs");
		if (partition_specs_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(partition_specs_val, idx, max, val) {
				result.partition_specs.push_back(PartitionSpec::FromJSON(val));
			}
		}

		auto partition_statistics_val = yyjson_obj_get(obj, "partition-statistics");
		if (partition_statistics_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(partition_statistics_val, idx, max, val) {
				result.partition_statistics.push_back(PartitionStatisticsFile::FromJSON(val));
			}
		}

		auto properties_val = yyjson_obj_get(obj, "properties");
		if (properties_val) {
			result.properties = parse_object_of_strings(properties_val);
		}

		auto refs_val = yyjson_obj_get(obj, "refs");
		if (refs_val) {
			result.refs = SnapshotReferences::FromJSON(refs_val);
		}

		auto schemas_val = yyjson_obj_get(obj, "schemas");
		if (schemas_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(schemas_val, idx, max, val) {
				result.schemas.push_back(Schema::FromJSON(val));
			}
		}

		auto snapshot_log_val = yyjson_obj_get(obj, "snapshot-log");
		if (snapshot_log_val) {
			result.snapshot_log = SnapshotLog::FromJSON(snapshot_log_val);
		}

		auto snapshots_val = yyjson_obj_get(obj, "snapshots");
		if (snapshots_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(snapshots_val, idx, max, val) {
				result.snapshots.push_back(Snapshot::FromJSON(val));
			}
		}

		auto sort_orders_val = yyjson_obj_get(obj, "sort-orders");
		if (sort_orders_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(sort_orders_val, idx, max, val) {
				result.sort_orders.push_back(SortOrder::FromJSON(val));
			}
		}

		auto statistics_val = yyjson_obj_get(obj, "statistics");
		if (statistics_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(statistics_val, idx, max, val) {
				result.statistics.push_back(StatisticsFile::FromJSON(val));
			}
		}

		auto table_uuid_val = yyjson_obj_get(obj, "table-uuid");
		if (table_uuid_val) {
			result.table_uuid = yyjson_get_str(table_uuid_val);
		}
		else {
			throw IOException("TableMetadata required property 'table-uuid' is missing");
		}

		return result;
	}

public:
	int64_t current_schema_id;
	int64_t current_snapshot_id;
	int64_t default_sort_order_id;
	int64_t default_spec_id;
	int64_t format_version;
	int64_t last_column_id;
	int64_t last_partition_id;
	int64_t last_sequence_number;
	int64_t last_updated_ms;
	string location;
	MetadataLog metadata_log;
	vector<PartitionSpec> partition_specs;
	vector<PartitionStatisticsFile> partition_statistics;
	case_insensitive_map_t<string> properties;
	SnapshotReferences refs;
	vector<Schema> schemas;
	SnapshotLog snapshot_log;
	vector<Snapshot> snapshots;
	vector<SortOrder> sort_orders;
	vector<StatisticsFile> statistics;
	string table_uuid;
};
} // namespace rest_api_objects
} // namespace duckdb