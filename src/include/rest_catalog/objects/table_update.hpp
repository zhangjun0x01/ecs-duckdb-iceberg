#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class TableUpdate {
public:
	static TableUpdate FromJSON(yyjson_val *obj) {
		TableUpdate result;
		if (yyjson_is_obj(obj)) {
			if (yyjson_obj_get(obj, "uuid")) {
				result.assign_uuidupdate = AssignUUIDUpdate::FromJSON(obj);
				result.has_assign_uuidupdate = true;
			}
			if (yyjson_obj_get(obj, "format-version")) {
				result.upgrade_format_version_update = UpgradeFormatVersionUpdate::FromJSON(obj);
				result.has_upgrade_format_version_update = true;
			}
			if (yyjson_obj_get(obj, "schema")) {
				result.add_schema_update = AddSchemaUpdate::FromJSON(obj);
				result.has_add_schema_update = true;
			}
			if (yyjson_obj_get(obj, "schema-id")) {
				result.set_current_schema_update = SetCurrentSchemaUpdate::FromJSON(obj);
				result.has_set_current_schema_update = true;
			}
			if (yyjson_obj_get(obj, "spec")) {
				result.add_partition_spec_update = AddPartitionSpecUpdate::FromJSON(obj);
				result.has_add_partition_spec_update = true;
			}
			if (yyjson_obj_get(obj, "spec-id")) {
				result.set_default_spec_update = SetDefaultSpecUpdate::FromJSON(obj);
				result.has_set_default_spec_update = true;
			}
			if (yyjson_obj_get(obj, "sort-order")) {
				result.add_sort_order_update = AddSortOrderUpdate::FromJSON(obj);
				result.has_add_sort_order_update = true;
			}
			if (yyjson_obj_get(obj, "sort-order-id")) {
				result.set_default_sort_order_update = SetDefaultSortOrderUpdate::FromJSON(obj);
				result.has_set_default_sort_order_update = true;
			}
			if (yyjson_obj_get(obj, "snapshot")) {
				result.add_snapshot_update = AddSnapshotUpdate::FromJSON(obj);
				result.has_add_snapshot_update = true;
			}
			if (yyjson_obj_get(obj, "ref-name")) {
				result.set_snapshot_ref_update = SetSnapshotRefUpdate::FromJSON(obj);
				result.has_set_snapshot_ref_update = true;
			}
			if (yyjson_obj_get(obj, "snapshot-ids")) {
				result.remove_snapshots_update = RemoveSnapshotsUpdate::FromJSON(obj);
				result.has_remove_snapshots_update = true;
			}
			if (yyjson_obj_get(obj, "ref-name")) {
				result.remove_snapshot_ref_update = RemoveSnapshotRefUpdate::FromJSON(obj);
				result.has_remove_snapshot_ref_update = true;
			}
			if (yyjson_obj_get(obj, "location")) {
				result.set_location_update = SetLocationUpdate::FromJSON(obj);
				result.has_set_location_update = true;
			}
			if (yyjson_obj_get(obj, "updates")) {
				result.set_properties_update = SetPropertiesUpdate::FromJSON(obj);
				result.has_set_properties_update = true;
			}
			if (yyjson_obj_get(obj, "removals")) {
				result.remove_properties_update = RemovePropertiesUpdate::FromJSON(obj);
				result.has_remove_properties_update = true;
			}
			if (yyjson_obj_get(obj, "statistics")) {
				result.set_statistics_update = SetStatisticsUpdate::FromJSON(obj);
				result.has_set_statistics_update = true;
			}
			if (yyjson_obj_get(obj, "snapshot-id")) {
				result.remove_statistics_update = RemoveStatisticsUpdate::FromJSON(obj);
				result.has_remove_statistics_update = true;
			}
			if (yyjson_obj_get(obj, "spec-ids")) {
				result.remove_partition_specs_update = RemovePartitionSpecsUpdate::FromJSON(obj);
				result.has_remove_partition_specs_update = true;
			}
			if (yyjson_obj_get(obj, "schema-ids")) {
				result.remove_schemas_update = RemoveSchemasUpdate::FromJSON(obj);
				result.has_remove_schemas_update = true;
			}
			if (!(result.has_assign_uuidupdate || result.has_upgrade_format_version_update ||
			      result.has_add_schema_update || result.has_set_current_schema_update ||
			      result.has_add_partition_spec_update || result.has_set_default_spec_update ||
			      result.has_add_sort_order_update || result.has_set_default_sort_order_update ||
			      result.has_add_snapshot_update || result.has_set_snapshot_ref_update ||
			      result.has_remove_snapshots_update || result.has_remove_snapshot_ref_update ||
			      result.has_set_location_update || result.has_set_properties_update ||
			      result.has_remove_properties_update || result.has_set_statistics_update ||
			      result.has_remove_statistics_update || result.has_remove_partition_specs_update ||
			      result.has_remove_schemas_update || result.has_enable_row_lineage_update)) {
				throw IOException("TableUpdate failed to parse, none of the accepted schemas found");
			}
		} else {
			throw IOException("TableUpdate must be an object");
		}
		return result;
	}

public:
	AssignUUIDUpdate assign_uuidupdate;
	bool has_assign_uuidupdate = false;
	UpgradeFormatVersionUpdate upgrade_format_version_update;
	bool has_upgrade_format_version_update = false;
	AddSchemaUpdate add_schema_update;
	bool has_add_schema_update = false;
	SetCurrentSchemaUpdate set_current_schema_update;
	bool has_set_current_schema_update = false;
	AddPartitionSpecUpdate add_partition_spec_update;
	bool has_add_partition_spec_update = false;
	SetDefaultSpecUpdate set_default_spec_update;
	bool has_set_default_spec_update = false;
	AddSortOrderUpdate add_sort_order_update;
	bool has_add_sort_order_update = false;
	SetDefaultSortOrderUpdate set_default_sort_order_update;
	bool has_set_default_sort_order_update = false;
	AddSnapshotUpdate add_snapshot_update;
	bool has_add_snapshot_update = false;
	SetSnapshotRefUpdate set_snapshot_ref_update;
	bool has_set_snapshot_ref_update = false;
	RemoveSnapshotsUpdate remove_snapshots_update;
	bool has_remove_snapshots_update = false;
	RemoveSnapshotRefUpdate remove_snapshot_ref_update;
	bool has_remove_snapshot_ref_update = false;
	SetLocationUpdate set_location_update;
	bool has_set_location_update = false;
	SetPropertiesUpdate set_properties_update;
	bool has_set_properties_update = false;
	RemovePropertiesUpdate remove_properties_update;
	bool has_remove_properties_update = false;
	SetStatisticsUpdate set_statistics_update;
	bool has_set_statistics_update = false;
	RemoveStatisticsUpdate remove_statistics_update;
	bool has_remove_statistics_update = false;
	RemovePartitionSpecsUpdate remove_partition_specs_update;
	bool has_remove_partition_specs_update = false;
	RemoveSchemasUpdate remove_schemas_update;
	bool has_remove_schemas_update = false;
	EnableRowLineageUpdate enable_row_lineage_update;
	bool has_enable_row_lineage_update = false;
};
} // namespace rest_api_objects
} // namespace duckdb
