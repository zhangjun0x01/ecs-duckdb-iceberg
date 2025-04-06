
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/add_partition_spec_update.hpp"
#include "rest_catalog/objects/add_schema_update.hpp"
#include "rest_catalog/objects/add_snapshot_update.hpp"
#include "rest_catalog/objects/add_sort_order_update.hpp"
#include "rest_catalog/objects/assign_uuidupdate.hpp"
#include "rest_catalog/objects/enable_row_lineage_update.hpp"
#include "rest_catalog/objects/remove_partition_specs_update.hpp"
#include "rest_catalog/objects/remove_properties_update.hpp"
#include "rest_catalog/objects/remove_schemas_update.hpp"
#include "rest_catalog/objects/remove_snapshot_ref_update.hpp"
#include "rest_catalog/objects/remove_snapshots_update.hpp"
#include "rest_catalog/objects/remove_statistics_update.hpp"
#include "rest_catalog/objects/set_current_schema_update.hpp"
#include "rest_catalog/objects/set_default_sort_order_update.hpp"
#include "rest_catalog/objects/set_default_spec_update.hpp"
#include "rest_catalog/objects/set_location_update.hpp"
#include "rest_catalog/objects/set_properties_update.hpp"
#include "rest_catalog/objects/set_snapshot_ref_update.hpp"
#include "rest_catalog/objects/set_statistics_update.hpp"
#include "rest_catalog/objects/upgrade_format_version_update.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class TableUpdate {
public:
	TableUpdate() {
	}

public:
	static TableUpdate FromJSON(yyjson_val *obj) {
		TableUpdate res;
		auto error = res.TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return res;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		error = assign_uuidupdate.TryFromJSON(obj);
		if (error.empty()) {
			has_assign_uuidupdate = true;
		}

		error = upgrade_format_version_update.TryFromJSON(obj);
		if (error.empty()) {
			has_upgrade_format_version_update = true;
		}

		error = add_schema_update.TryFromJSON(obj);
		if (error.empty()) {
			has_add_schema_update = true;
		}

		error = set_current_schema_update.TryFromJSON(obj);
		if (error.empty()) {
			has_set_current_schema_update = true;
		}

		error = add_partition_spec_update.TryFromJSON(obj);
		if (error.empty()) {
			has_add_partition_spec_update = true;
		}

		error = set_default_spec_update.TryFromJSON(obj);
		if (error.empty()) {
			has_set_default_spec_update = true;
		}

		error = add_sort_order_update.TryFromJSON(obj);
		if (error.empty()) {
			has_add_sort_order_update = true;
		}

		error = set_default_sort_order_update.TryFromJSON(obj);
		if (error.empty()) {
			has_set_default_sort_order_update = true;
		}

		error = add_snapshot_update.TryFromJSON(obj);
		if (error.empty()) {
			has_add_snapshot_update = true;
		}

		error = set_snapshot_ref_update.TryFromJSON(obj);
		if (error.empty()) {
			has_set_snapshot_ref_update = true;
		}

		error = remove_snapshots_update.TryFromJSON(obj);
		if (error.empty()) {
			has_remove_snapshots_update = true;
		}

		error = remove_snapshot_ref_update.TryFromJSON(obj);
		if (error.empty()) {
			has_remove_snapshot_ref_update = true;
		}

		error = set_location_update.TryFromJSON(obj);
		if (error.empty()) {
			has_set_location_update = true;
		}

		error = set_properties_update.TryFromJSON(obj);
		if (error.empty()) {
			has_set_properties_update = true;
		}

		error = remove_properties_update.TryFromJSON(obj);
		if (error.empty()) {
			has_remove_properties_update = true;
		}

		error = set_statistics_update.TryFromJSON(obj);
		if (error.empty()) {
			has_set_statistics_update = true;
		}

		error = remove_statistics_update.TryFromJSON(obj);
		if (error.empty()) {
			has_remove_statistics_update = true;
		}

		error = remove_partition_specs_update.TryFromJSON(obj);
		if (error.empty()) {
			has_remove_partition_specs_update = true;
		}

		error = remove_schemas_update.TryFromJSON(obj);
		if (error.empty()) {
			has_remove_schemas_update = true;
		}

		error = enable_row_lineage_update.TryFromJSON(obj);
		if (error.empty()) {
			has_enable_row_lineage_update = true;
		}

		if (!has_add_partition_spec_update && !has_add_schema_update && !has_add_snapshot_update &&
		    !has_add_sort_order_update && !has_assign_uuidupdate && !has_enable_row_lineage_update &&
		    !has_remove_partition_specs_update && !has_remove_properties_update && !has_remove_schemas_update &&
		    !has_remove_snapshot_ref_update && !has_remove_snapshots_update && !has_remove_statistics_update &&
		    !has_set_current_schema_update && !has_set_default_sort_order_update && !has_set_default_spec_update &&
		    !has_set_location_update && !has_set_properties_update && !has_set_snapshot_ref_update &&
		    !has_set_statistics_update && !has_upgrade_format_version_update) {
			return "TableUpdate failed to parse, none of the anyOf candidates matched";
		}

		return string();
	}

public:
	SetStatisticsUpdate set_statistics_update;
	SetSnapshotRefUpdate set_snapshot_ref_update;
	AddPartitionSpecUpdate add_partition_spec_update;
	SetDefaultSortOrderUpdate set_default_sort_order_update;
	RemovePartitionSpecsUpdate remove_partition_specs_update;
	RemoveStatisticsUpdate remove_statistics_update;
	RemoveSnapshotRefUpdate remove_snapshot_ref_update;
	SetDefaultSpecUpdate set_default_spec_update;
	SetCurrentSchemaUpdate set_current_schema_update;
	RemovePropertiesUpdate remove_properties_update;
	SetPropertiesUpdate set_properties_update;
	SetLocationUpdate set_location_update;
	AddSchemaUpdate add_schema_update;
	AddSnapshotUpdate add_snapshot_update;
	RemoveSnapshotsUpdate remove_snapshots_update;
	AddSortOrderUpdate add_sort_order_update;
	EnableRowLineageUpdate enable_row_lineage_update;
	UpgradeFormatVersionUpdate upgrade_format_version_update;
	AssignUUIDUpdate assign_uuidupdate;
	RemoveSchemasUpdate remove_schemas_update;

public:
	bool has_assign_uuidupdate = false;
	bool has_upgrade_format_version_update = false;
	bool has_add_schema_update = false;
	bool has_set_current_schema_update = false;
	bool has_add_partition_spec_update = false;
	bool has_set_default_spec_update = false;
	bool has_add_sort_order_update = false;
	bool has_set_default_sort_order_update = false;
	bool has_add_snapshot_update = false;
	bool has_set_snapshot_ref_update = false;
	bool has_remove_snapshots_update = false;
	bool has_remove_snapshot_ref_update = false;
	bool has_set_location_update = false;
	bool has_set_properties_update = false;
	bool has_remove_properties_update = false;
	bool has_set_statistics_update = false;
	bool has_remove_statistics_update = false;
	bool has_remove_partition_specs_update = false;
	bool has_remove_schemas_update = false;
	bool has_enable_row_lineage_update = false;
};

} // namespace rest_api_objects
} // namespace duckdb
