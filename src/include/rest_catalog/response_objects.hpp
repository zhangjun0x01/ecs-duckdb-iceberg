#pragma once

#include "yyjson.hpp"

using namespace duckdb_yyjson;
namespace duckdb {

namespace rest_api_objects {

template<typename T>
vector<T> parse_obj_array(yyjson_val *arr) {
    vector<T> result;
    size_t idx, max;
    yyjson_val *val;
    yyjson_arr_foreach(arr, idx, max, val) {
        result.push_back(T::FromJSON(val));
    }
    return result;
}

inline vector<string> parse_str_array(yyjson_val *arr) {
    vector<string> result;
    size_t idx, max;
    yyjson_val *val;
    yyjson_arr_foreach(arr, idx, max, val) {
        result.push_back(yyjson_get_str(val));
    }
    return result;
}

class ObjectOfStrings {
public:
    static ObjectOfStrings FromJSON(yyjson_val *obj) {
        ObjectOfStrings result;
        yyjson_val *key, *val;
        yyjson_obj_iter iter = yyjson_obj_iter_with(obj);
        while ((key = yyjson_obj_iter_next(&iter))) {
            val = yyjson_obj_iter_get_val(key);
            result.properties[yyjson_get_str(key)] = yyjson_get_str(val);
        }
        return result;
    }
public:
    ObjectOfStrings() {}
public:
    case_insensitive_map_t<string> properties;
};

class ErrorModel {
public:
	static ErrorModel FromJSON(yyjson_val *obj) {
		ErrorModel result;
		auto message_val = yyjson_obj_get(obj, "message");
		if (message_val) {
			result.message = yyjson_get_str(message_val);
		} else {
			throw IOException("ErrorModel required property 'message' is missing");
		}
		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = yyjson_get_str(type_val);
		} else {
			throw IOException("ErrorModel required property 'type' is missing");
		}
		auto code_val = yyjson_obj_get(obj, "code");
		if (code_val) {
			result.code = yyjson_get_sint(code_val);
		} else {
			throw IOException("ErrorModel required property 'code' is missing");
		}
		auto stack_val = yyjson_obj_get(obj, "stack");
		if (stack_val) {
			result.stack = parse_str_array(stack_val);
		}
		return result;
	}
public:
	ErrorModel() {}
public:
	string message;
	string type;
	int64_t code;
	vector<string> stack;
};

class CatalogConfig {
public:
	static CatalogConfig FromJSON(yyjson_val *obj) {
		CatalogConfig result;
		auto overrides_val = yyjson_obj_get(obj, "overrides");
		if (overrides_val) {
			result.overrides = ObjectOfStrings::FromJSON(overrides_val);
		} else {
			throw IOException("CatalogConfig required property 'overrides' is missing");
		}
		auto defaults_val = yyjson_obj_get(obj, "defaults");
		if (defaults_val) {
			result.defaults = ObjectOfStrings::FromJSON(defaults_val);
		} else {
			throw IOException("CatalogConfig required property 'defaults' is missing");
		}
		auto endpoints_val = yyjson_obj_get(obj, "endpoints");
		if (endpoints_val) {
			result.endpoints = parse_str_array(endpoints_val);
		}
		return result;
	}
public:
	CatalogConfig() {}
public:
	ObjectOfStrings overrides;
	ObjectOfStrings defaults;
	vector<string> endpoints;
};

class UpdateNamespacePropertiesRequest {
public:
	static UpdateNamespacePropertiesRequest FromJSON(yyjson_val *obj) {
		UpdateNamespacePropertiesRequest result;
		auto removals_val = yyjson_obj_get(obj, "removals");
		if (removals_val) {
			result.removals = parse_str_array(removals_val);
		}
		auto updates_val = yyjson_obj_get(obj, "updates");
		if (updates_val) {
			result.updates = ObjectOfStrings::FromJSON(updates_val);
		}
		return result;
	}
public:
	UpdateNamespacePropertiesRequest() {}
public:
	vector<string> removals;
	ObjectOfStrings updates;
};

class Namespace {
public:
	static Namespace FromJSON(yyjson_val *obj) {
		Namespace result;
		return result;
	}
public:
	Namespace() {}
public:
};

class PageToken {
public:
	static PageToken FromJSON(yyjson_val *obj) {
		PageToken result;
		return result;
	}
public:
	PageToken() {}
public:
};

class TableIdentifier {
public:
	static TableIdentifier FromJSON(yyjson_val *obj) {
		TableIdentifier result;
		auto _namespace_val = yyjson_obj_get(obj, "namespace");
		if (_namespace_val) {
			result._namespace = Namespace::FromJSON(_namespace_val);
		} else {
			throw IOException("TableIdentifier required property 'namespace' is missing");
		}
		auto name_val = yyjson_obj_get(obj, "name");
		if (name_val) {
			result.name = yyjson_get_str(name_val);
		} else {
			throw IOException("TableIdentifier required property 'name' is missing");
		}
		return result;
	}
public:
	TableIdentifier() {}
public:
	Namespace _namespace;
	string name;
};

class PrimitiveType {
public:
	static PrimitiveType FromJSON(yyjson_val *obj) {
		PrimitiveType result;
		return result;
	}
public:
	PrimitiveType() {}
public:
};

class StructType {
public:
	static StructType FromJSON(yyjson_val *obj) {
		StructType result;
		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = yyjson_get_str(type_val);
		} else {
			throw IOException("StructType required property 'type' is missing");
		}
		auto fields_val = yyjson_obj_get(obj, "fields");
		if (fields_val) {
			result.fields = parse_obj_array<StructField>(fields_val);
		} else {
			throw IOException("StructType required property 'fields' is missing");
		}
		return result;
	}
public:
	StructType() {}
public:
	string type;
	vector<StructField> fields;
};

class Type {
public:
	static Type FromJSON(yyjson_val *obj) {
		Type result;
		return result;
	}
public:
	Type() {}
public:
};

class Schema {
public:
	static Schema FromJSON(yyjson_val *obj) {
		Schema result;
		auto schema_id_val = yyjson_obj_get(obj, "schema-id");
		if (schema_id_val) {
			result.schema_id = yyjson_get_sint(schema_id_val);
		}
		auto identifier_field_ids_val = yyjson_obj_get(obj, "identifier-field-ids");
		if (identifier_field_ids_val) {
			result.identifier_field_ids = parse_obj_array<int64_t>(identifier_field_ids_val);
		}
		return result;
	}
public:
	Schema() {}
public:
	int64_t schema_id;
	vector<int64_t> identifier_field_ids;
};

class Expression {
public:
	static Expression FromJSON(yyjson_val *obj) {
		Expression result;
		return result;
	}
public:
	Expression() {}
public:
};

class ExpressionType {
public:
	static ExpressionType FromJSON(yyjson_val *obj) {
		ExpressionType result;
		return result;
	}
public:
	ExpressionType() {}
public:
};

class TrueExpression {
public:
	static TrueExpression FromJSON(yyjson_val *obj) {
		TrueExpression result;
		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = ExpressionType::FromJSON(type_val);
		} else {
			throw IOException("TrueExpression required property 'type' is missing");
		}
		return result;
	}
public:
	TrueExpression() {}
public:
	ExpressionType type;
};

class FalseExpression {
public:
	static FalseExpression FromJSON(yyjson_val *obj) {
		FalseExpression result;
		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = ExpressionType::FromJSON(type_val);
		} else {
			throw IOException("FalseExpression required property 'type' is missing");
		}
		return result;
	}
public:
	FalseExpression() {}
public:
	ExpressionType type;
};

class AndOrExpression {
public:
	static AndOrExpression FromJSON(yyjson_val *obj) {
		AndOrExpression result;
		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = ExpressionType::FromJSON(type_val);
		} else {
			throw IOException("AndOrExpression required property 'type' is missing");
		}
		auto left_val = yyjson_obj_get(obj, "left");
		if (left_val) {
			result.left = Expression::FromJSON(left_val);
		} else {
			throw IOException("AndOrExpression required property 'left' is missing");
		}
		auto right_val = yyjson_obj_get(obj, "right");
		if (right_val) {
			result.right = Expression::FromJSON(right_val);
		} else {
			throw IOException("AndOrExpression required property 'right' is missing");
		}
		return result;
	}
public:
	AndOrExpression() {}
public:
	ExpressionType type;
	Expression left;
	Expression right;
};

class NotExpression {
public:
	static NotExpression FromJSON(yyjson_val *obj) {
		NotExpression result;
		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = ExpressionType::FromJSON(type_val);
		} else {
			throw IOException("NotExpression required property 'type' is missing");
		}
		auto child_val = yyjson_obj_get(obj, "child");
		if (child_val) {
			result.child = Expression::FromJSON(child_val);
		} else {
			throw IOException("NotExpression required property 'child' is missing");
		}
		return result;
	}
public:
	NotExpression() {}
public:
	ExpressionType type;
	Expression child;
};

class Term {
public:
	static Term FromJSON(yyjson_val *obj) {
		Term result;
		return result;
	}
public:
	Term() {}
public:
};

class Reference {
public:
	static Reference FromJSON(yyjson_val *obj) {
		Reference result;
		return result;
	}
public:
	Reference() {}
public:
};

class Transform {
public:
	static Transform FromJSON(yyjson_val *obj) {
		Transform result;
		return result;
	}
public:
	Transform() {}
public:
};

class PartitionField {
public:
	static PartitionField FromJSON(yyjson_val *obj) {
		PartitionField result;
		auto field_id_val = yyjson_obj_get(obj, "field-id");
		if (field_id_val) {
			result.field_id = yyjson_get_sint(field_id_val);
		}
		auto source_id_val = yyjson_obj_get(obj, "source-id");
		if (source_id_val) {
			result.source_id = yyjson_get_sint(source_id_val);
		} else {
			throw IOException("PartitionField required property 'source-id' is missing");
		}
		auto name_val = yyjson_obj_get(obj, "name");
		if (name_val) {
			result.name = yyjson_get_str(name_val);
		} else {
			throw IOException("PartitionField required property 'name' is missing");
		}
		auto transform_val = yyjson_obj_get(obj, "transform");
		if (transform_val) {
			result.transform = Transform::FromJSON(transform_val);
		} else {
			throw IOException("PartitionField required property 'transform' is missing");
		}
		return result;
	}
public:
	PartitionField() {}
public:
	int64_t field_id;
	int64_t source_id;
	string name;
	Transform transform;
};

class PartitionSpec {
public:
	static PartitionSpec FromJSON(yyjson_val *obj) {
		PartitionSpec result;
		auto spec_id_val = yyjson_obj_get(obj, "spec-id");
		if (spec_id_val) {
			result.spec_id = yyjson_get_sint(spec_id_val);
		}
		auto fields_val = yyjson_obj_get(obj, "fields");
		if (fields_val) {
			result.fields = parse_obj_array<PartitionField>(fields_val);
		} else {
			throw IOException("PartitionSpec required property 'fields' is missing");
		}
		return result;
	}
public:
	PartitionSpec() {}
public:
	int64_t spec_id;
	vector<PartitionField> fields;
};

class SortDirection {
public:
	static SortDirection FromJSON(yyjson_val *obj) {
		SortDirection result;
		return result;
	}
public:
	SortDirection() {}
public:
};

class NullOrder {
public:
	static NullOrder FromJSON(yyjson_val *obj) {
		NullOrder result;
		return result;
	}
public:
	NullOrder() {}
public:
};

class SortField {
public:
	static SortField FromJSON(yyjson_val *obj) {
		SortField result;
		auto source_id_val = yyjson_obj_get(obj, "source-id");
		if (source_id_val) {
			result.source_id = yyjson_get_sint(source_id_val);
		} else {
			throw IOException("SortField required property 'source-id' is missing");
		}
		auto transform_val = yyjson_obj_get(obj, "transform");
		if (transform_val) {
			result.transform = Transform::FromJSON(transform_val);
		} else {
			throw IOException("SortField required property 'transform' is missing");
		}
		auto direction_val = yyjson_obj_get(obj, "direction");
		if (direction_val) {
			result.direction = SortDirection::FromJSON(direction_val);
		} else {
			throw IOException("SortField required property 'direction' is missing");
		}
		auto null_order_val = yyjson_obj_get(obj, "null-order");
		if (null_order_val) {
			result.null_order = NullOrder::FromJSON(null_order_val);
		} else {
			throw IOException("SortField required property 'null-order' is missing");
		}
		return result;
	}
public:
	SortField() {}
public:
	int64_t source_id;
	Transform transform;
	SortDirection direction;
	NullOrder null_order;
};

class SortOrder {
public:
	static SortOrder FromJSON(yyjson_val *obj) {
		SortOrder result;
		auto order_id_val = yyjson_obj_get(obj, "order-id");
		if (order_id_val) {
			result.order_id = yyjson_get_sint(order_id_val);
		} else {
			throw IOException("SortOrder required property 'order-id' is missing");
		}
		auto fields_val = yyjson_obj_get(obj, "fields");
		if (fields_val) {
			result.fields = parse_obj_array<SortField>(fields_val);
		} else {
			throw IOException("SortOrder required property 'fields' is missing");
		}
		return result;
	}
public:
	SortOrder() {}
public:
	int64_t order_id;
	vector<SortField> fields;
};

class Snapshot {
public:
	static Snapshot FromJSON(yyjson_val *obj) {
		Snapshot result;
		auto snapshot_id_val = yyjson_obj_get(obj, "snapshot-id");
		if (snapshot_id_val) {
			result.snapshot_id = yyjson_get_sint(snapshot_id_val);
		} else {
			throw IOException("Snapshot required property 'snapshot-id' is missing");
		}
		auto parent_snapshot_id_val = yyjson_obj_get(obj, "parent-snapshot-id");
		if (parent_snapshot_id_val) {
			result.parent_snapshot_id = yyjson_get_sint(parent_snapshot_id_val);
		}
		auto sequence_number_val = yyjson_obj_get(obj, "sequence-number");
		if (sequence_number_val) {
			result.sequence_number = yyjson_get_sint(sequence_number_val);
		}
		auto timestamp_ms_val = yyjson_obj_get(obj, "timestamp-ms");
		if (timestamp_ms_val) {
			result.timestamp_ms = yyjson_get_sint(timestamp_ms_val);
		} else {
			throw IOException("Snapshot required property 'timestamp-ms' is missing");
		}
		auto manifest_list_val = yyjson_obj_get(obj, "manifest-list");
		if (manifest_list_val) {
			result.manifest_list = yyjson_get_str(manifest_list_val);
		} else {
			throw IOException("Snapshot required property 'manifest-list' is missing");
		}
		auto summary_val = yyjson_obj_get(obj, "summary");
		if (summary_val) {
			result.summary = ObjectOfStrings::FromJSON(summary_val);
		} else {
			throw IOException("Snapshot required property 'summary' is missing");
		}
		auto schema_id_val = yyjson_obj_get(obj, "schema-id");
		if (schema_id_val) {
			result.schema_id = yyjson_get_sint(schema_id_val);
		}
		return result;
	}
public:
	Snapshot() {}
public:
	int64_t snapshot_id;
	int64_t parent_snapshot_id;
	int64_t sequence_number;
	int64_t timestamp_ms;
	string manifest_list;
	ObjectOfStrings summary;
	int64_t schema_id;
};

class SnapshotReference {
public:
	static SnapshotReference FromJSON(yyjson_val *obj) {
		SnapshotReference result;
		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = yyjson_get_str(type_val);
		} else {
			throw IOException("SnapshotReference required property 'type' is missing");
		}
		auto snapshot_id_val = yyjson_obj_get(obj, "snapshot-id");
		if (snapshot_id_val) {
			result.snapshot_id = yyjson_get_sint(snapshot_id_val);
		} else {
			throw IOException("SnapshotReference required property 'snapshot-id' is missing");
		}
		auto max_ref_age_ms_val = yyjson_obj_get(obj, "max-ref-age-ms");
		if (max_ref_age_ms_val) {
			result.max_ref_age_ms = yyjson_get_sint(max_ref_age_ms_val);
		}
		auto max_snapshot_age_ms_val = yyjson_obj_get(obj, "max-snapshot-age-ms");
		if (max_snapshot_age_ms_val) {
			result.max_snapshot_age_ms = yyjson_get_sint(max_snapshot_age_ms_val);
		}
		auto min_snapshots_to_keep_val = yyjson_obj_get(obj, "min-snapshots-to-keep");
		if (min_snapshots_to_keep_val) {
			result.min_snapshots_to_keep = yyjson_get_sint(min_snapshots_to_keep_val);
		}
		return result;
	}
public:
	SnapshotReference() {}
public:
	string type;
	int64_t snapshot_id;
	int64_t max_ref_age_ms;
	int64_t max_snapshot_age_ms;
	int64_t min_snapshots_to_keep;
};

class SnapshotReferences {
public:
	static SnapshotReferences FromJSON(yyjson_val *obj) {
		SnapshotReferences result;
		return result;
	}
public:
	SnapshotReferences() {}
public:
};

class SnapshotLog {
public:
	static SnapshotLog FromJSON(yyjson_val *obj) {
		SnapshotLog result;
		return result;
	}
public:
	SnapshotLog() {}
public:
};

class MetadataLog {
public:
	static MetadataLog FromJSON(yyjson_val *obj) {
		MetadataLog result;
		return result;
	}
public:
	MetadataLog() {}
public:
};

class TableMetadata {
public:
	static TableMetadata FromJSON(yyjson_val *obj) {
		TableMetadata result;
		auto format_version_val = yyjson_obj_get(obj, "format-version");
		if (format_version_val) {
			result.format_version = yyjson_get_sint(format_version_val);
		} else {
			throw IOException("TableMetadata required property 'format-version' is missing");
		}
		auto table_uuid_val = yyjson_obj_get(obj, "table-uuid");
		if (table_uuid_val) {
			result.table_uuid = yyjson_get_str(table_uuid_val);
		} else {
			throw IOException("TableMetadata required property 'table-uuid' is missing");
		}
		auto location_val = yyjson_obj_get(obj, "location");
		if (location_val) {
			result.location = yyjson_get_str(location_val);
		}
		auto last_updated_ms_val = yyjson_obj_get(obj, "last-updated-ms");
		if (last_updated_ms_val) {
			result.last_updated_ms = yyjson_get_sint(last_updated_ms_val);
		}
		auto properties_val = yyjson_obj_get(obj, "properties");
		if (properties_val) {
			result.properties = ObjectOfStrings::FromJSON(properties_val);
		}
		auto schemas_val = yyjson_obj_get(obj, "schemas");
		if (schemas_val) {
			result.schemas = parse_obj_array<Schema>(schemas_val);
		}
		auto current_schema_id_val = yyjson_obj_get(obj, "current-schema-id");
		if (current_schema_id_val) {
			result.current_schema_id = yyjson_get_sint(current_schema_id_val);
		}
		auto last_column_id_val = yyjson_obj_get(obj, "last-column-id");
		if (last_column_id_val) {
			result.last_column_id = yyjson_get_sint(last_column_id_val);
		}
		auto partition_specs_val = yyjson_obj_get(obj, "partition-specs");
		if (partition_specs_val) {
			result.partition_specs = parse_obj_array<PartitionSpec>(partition_specs_val);
		}
		auto default_spec_id_val = yyjson_obj_get(obj, "default-spec-id");
		if (default_spec_id_val) {
			result.default_spec_id = yyjson_get_sint(default_spec_id_val);
		}
		auto last_partition_id_val = yyjson_obj_get(obj, "last-partition-id");
		if (last_partition_id_val) {
			result.last_partition_id = yyjson_get_sint(last_partition_id_val);
		}
		auto sort_orders_val = yyjson_obj_get(obj, "sort-orders");
		if (sort_orders_val) {
			result.sort_orders = parse_obj_array<SortOrder>(sort_orders_val);
		}
		auto default_sort_order_id_val = yyjson_obj_get(obj, "default-sort-order-id");
		if (default_sort_order_id_val) {
			result.default_sort_order_id = yyjson_get_sint(default_sort_order_id_val);
		}
		auto snapshots_val = yyjson_obj_get(obj, "snapshots");
		if (snapshots_val) {
			result.snapshots = parse_obj_array<Snapshot>(snapshots_val);
		}
		auto refs_val = yyjson_obj_get(obj, "refs");
		if (refs_val) {
			result.refs = SnapshotReferences::FromJSON(refs_val);
		}
		auto current_snapshot_id_val = yyjson_obj_get(obj, "current-snapshot-id");
		if (current_snapshot_id_val) {
			result.current_snapshot_id = yyjson_get_sint(current_snapshot_id_val);
		}
		auto last_sequence_number_val = yyjson_obj_get(obj, "last-sequence-number");
		if (last_sequence_number_val) {
			result.last_sequence_number = yyjson_get_sint(last_sequence_number_val);
		}
		auto snapshot_log_val = yyjson_obj_get(obj, "snapshot-log");
		if (snapshot_log_val) {
			result.snapshot_log = SnapshotLog::FromJSON(snapshot_log_val);
		}
		auto metadata_log_val = yyjson_obj_get(obj, "metadata-log");
		if (metadata_log_val) {
			result.metadata_log = MetadataLog::FromJSON(metadata_log_val);
		}
		auto statistics_val = yyjson_obj_get(obj, "statistics");
		if (statistics_val) {
			result.statistics = parse_obj_array<StatisticsFile>(statistics_val);
		}
		auto partition_statistics_val = yyjson_obj_get(obj, "partition-statistics");
		if (partition_statistics_val) {
			result.partition_statistics = parse_obj_array<PartitionStatisticsFile>(partition_statistics_val);
		}
		return result;
	}
public:
	TableMetadata() {}
public:
	int64_t format_version;
	string table_uuid;
	string location;
	int64_t last_updated_ms;
	ObjectOfStrings properties;
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

class SQLViewRepresentation {
public:
	static SQLViewRepresentation FromJSON(yyjson_val *obj) {
		SQLViewRepresentation result;
		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = yyjson_get_str(type_val);
		} else {
			throw IOException("SQLViewRepresentation required property 'type' is missing");
		}
		auto sql_val = yyjson_obj_get(obj, "sql");
		if (sql_val) {
			result.sql = yyjson_get_str(sql_val);
		} else {
			throw IOException("SQLViewRepresentation required property 'sql' is missing");
		}
		auto dialect_val = yyjson_obj_get(obj, "dialect");
		if (dialect_val) {
			result.dialect = yyjson_get_str(dialect_val);
		} else {
			throw IOException("SQLViewRepresentation required property 'dialect' is missing");
		}
		return result;
	}
public:
	SQLViewRepresentation() {}
public:
	string type;
	string sql;
	string dialect;
};

class ViewRepresentation {
public:
	static ViewRepresentation FromJSON(yyjson_val *obj) {
		ViewRepresentation result;
		return result;
	}
public:
	ViewRepresentation() {}
public:
};

class ViewHistoryEntry {
public:
	static ViewHistoryEntry FromJSON(yyjson_val *obj) {
		ViewHistoryEntry result;
		auto version_id_val = yyjson_obj_get(obj, "version-id");
		if (version_id_val) {
			result.version_id = yyjson_get_sint(version_id_val);
		} else {
			throw IOException("ViewHistoryEntry required property 'version-id' is missing");
		}
		auto timestamp_ms_val = yyjson_obj_get(obj, "timestamp-ms");
		if (timestamp_ms_val) {
			result.timestamp_ms = yyjson_get_sint(timestamp_ms_val);
		} else {
			throw IOException("ViewHistoryEntry required property 'timestamp-ms' is missing");
		}
		return result;
	}
public:
	ViewHistoryEntry() {}
public:
	int64_t version_id;
	int64_t timestamp_ms;
};

class ViewVersion {
public:
	static ViewVersion FromJSON(yyjson_val *obj) {
		ViewVersion result;
		auto version_id_val = yyjson_obj_get(obj, "version-id");
		if (version_id_val) {
			result.version_id = yyjson_get_sint(version_id_val);
		} else {
			throw IOException("ViewVersion required property 'version-id' is missing");
		}
		auto timestamp_ms_val = yyjson_obj_get(obj, "timestamp-ms");
		if (timestamp_ms_val) {
			result.timestamp_ms = yyjson_get_sint(timestamp_ms_val);
		} else {
			throw IOException("ViewVersion required property 'timestamp-ms' is missing");
		}
		auto schema_id_val = yyjson_obj_get(obj, "schema-id");
		if (schema_id_val) {
			result.schema_id = yyjson_get_sint(schema_id_val);
		} else {
			throw IOException("ViewVersion required property 'schema-id' is missing");
		}
		auto summary_val = yyjson_obj_get(obj, "summary");
		if (summary_val) {
			result.summary = ObjectOfStrings::FromJSON(summary_val);
		} else {
			throw IOException("ViewVersion required property 'summary' is missing");
		}
		auto representations_val = yyjson_obj_get(obj, "representations");
		if (representations_val) {
			result.representations = parse_obj_array<ViewRepresentation>(representations_val);
		} else {
			throw IOException("ViewVersion required property 'representations' is missing");
		}
		auto default_catalog_val = yyjson_obj_get(obj, "default-catalog");
		if (default_catalog_val) {
			result.default_catalog = yyjson_get_str(default_catalog_val);
		}
		auto default_namespace_val = yyjson_obj_get(obj, "default-namespace");
		if (default_namespace_val) {
			result.default_namespace = Namespace::FromJSON(default_namespace_val);
		} else {
			throw IOException("ViewVersion required property 'default-namespace' is missing");
		}
		return result;
	}
public:
	ViewVersion() {}
public:
	int64_t version_id;
	int64_t timestamp_ms;
	int64_t schema_id;
	ObjectOfStrings summary;
	vector<ViewRepresentation> representations;
	string default_catalog;
	Namespace default_namespace;
};

class ViewMetadata {
public:
	static ViewMetadata FromJSON(yyjson_val *obj) {
		ViewMetadata result;
		auto view_uuid_val = yyjson_obj_get(obj, "view-uuid");
		if (view_uuid_val) {
			result.view_uuid = yyjson_get_str(view_uuid_val);
		} else {
			throw IOException("ViewMetadata required property 'view-uuid' is missing");
		}
		auto format_version_val = yyjson_obj_get(obj, "format-version");
		if (format_version_val) {
			result.format_version = yyjson_get_sint(format_version_val);
		} else {
			throw IOException("ViewMetadata required property 'format-version' is missing");
		}
		auto location_val = yyjson_obj_get(obj, "location");
		if (location_val) {
			result.location = yyjson_get_str(location_val);
		} else {
			throw IOException("ViewMetadata required property 'location' is missing");
		}
		auto current_version_id_val = yyjson_obj_get(obj, "current-version-id");
		if (current_version_id_val) {
			result.current_version_id = yyjson_get_sint(current_version_id_val);
		} else {
			throw IOException("ViewMetadata required property 'current-version-id' is missing");
		}
		auto versions_val = yyjson_obj_get(obj, "versions");
		if (versions_val) {
			result.versions = parse_obj_array<ViewVersion>(versions_val);
		} else {
			throw IOException("ViewMetadata required property 'versions' is missing");
		}
		auto version_log_val = yyjson_obj_get(obj, "version-log");
		if (version_log_val) {
			result.version_log = parse_obj_array<ViewHistoryEntry>(version_log_val);
		} else {
			throw IOException("ViewMetadata required property 'version-log' is missing");
		}
		auto schemas_val = yyjson_obj_get(obj, "schemas");
		if (schemas_val) {
			result.schemas = parse_obj_array<Schema>(schemas_val);
		} else {
			throw IOException("ViewMetadata required property 'schemas' is missing");
		}
		auto properties_val = yyjson_obj_get(obj, "properties");
		if (properties_val) {
			result.properties = ObjectOfStrings::FromJSON(properties_val);
		}
		return result;
	}
public:
	ViewMetadata() {}
public:
	string view_uuid;
	int64_t format_version;
	string location;
	int64_t current_version_id;
	vector<ViewVersion> versions;
	vector<ViewHistoryEntry> version_log;
	vector<Schema> schemas;
	ObjectOfStrings properties;
};

class BaseUpdate {
public:
	static BaseUpdate FromJSON(yyjson_val *obj) {
		BaseUpdate result;
		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			result.action = yyjson_get_str(action_val);
		} else {
			throw IOException("BaseUpdate required property 'action' is missing");
		}
		return result;
	}
public:
	BaseUpdate() {}
public:
	string action;
};

class AssignUUIDUpdate {
public:
	static AssignUUIDUpdate FromJSON(yyjson_val *obj) {
		AssignUUIDUpdate result;
		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			result.action = yyjson_get_str(action_val);
		}
		auto uuid_val = yyjson_obj_get(obj, "uuid");
		if (uuid_val) {
			result.uuid = yyjson_get_str(uuid_val);
		} else {
			throw IOException("AssignUUIDUpdate required property 'uuid' is missing");
		}
		return result;
	}
public:
	AssignUUIDUpdate() {}
public:
	string action;
	string uuid;
};

class UpgradeFormatVersionUpdate {
public:
	static UpgradeFormatVersionUpdate FromJSON(yyjson_val *obj) {
		UpgradeFormatVersionUpdate result;
		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			result.action = yyjson_get_str(action_val);
		}
		auto format_version_val = yyjson_obj_get(obj, "format-version");
		if (format_version_val) {
			result.format_version = yyjson_get_sint(format_version_val);
		} else {
			throw IOException("UpgradeFormatVersionUpdate required property 'format-version' is missing");
		}
		return result;
	}
public:
	UpgradeFormatVersionUpdate() {}
public:
	string action;
	int64_t format_version;
};

class AddSchemaUpdate {
public:
	static AddSchemaUpdate FromJSON(yyjson_val *obj) {
		AddSchemaUpdate result;
		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			result.action = yyjson_get_str(action_val);
		}
		auto schema_val = yyjson_obj_get(obj, "schema");
		if (schema_val) {
			result.schema = Schema::FromJSON(schema_val);
		} else {
			throw IOException("AddSchemaUpdate required property 'schema' is missing");
		}
		auto last_column_id_val = yyjson_obj_get(obj, "last-column-id");
		if (last_column_id_val) {
			result.last_column_id = yyjson_get_sint(last_column_id_val);
		}
		return result;
	}
public:
	AddSchemaUpdate() {}
public:
	string action;
	Schema schema;
	int64_t last_column_id;
};

class SetCurrentSchemaUpdate {
public:
	static SetCurrentSchemaUpdate FromJSON(yyjson_val *obj) {
		SetCurrentSchemaUpdate result;
		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			result.action = yyjson_get_str(action_val);
		}
		auto schema_id_val = yyjson_obj_get(obj, "schema-id");
		if (schema_id_val) {
			result.schema_id = yyjson_get_sint(schema_id_val);
		} else {
			throw IOException("SetCurrentSchemaUpdate required property 'schema-id' is missing");
		}
		return result;
	}
public:
	SetCurrentSchemaUpdate() {}
public:
	string action;
	int64_t schema_id;
};

class AddPartitionSpecUpdate {
public:
	static AddPartitionSpecUpdate FromJSON(yyjson_val *obj) {
		AddPartitionSpecUpdate result;
		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			result.action = yyjson_get_str(action_val);
		}
		auto spec_val = yyjson_obj_get(obj, "spec");
		if (spec_val) {
			result.spec = PartitionSpec::FromJSON(spec_val);
		} else {
			throw IOException("AddPartitionSpecUpdate required property 'spec' is missing");
		}
		return result;
	}
public:
	AddPartitionSpecUpdate() {}
public:
	string action;
	PartitionSpec spec;
};

class SetDefaultSpecUpdate {
public:
	static SetDefaultSpecUpdate FromJSON(yyjson_val *obj) {
		SetDefaultSpecUpdate result;
		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			result.action = yyjson_get_str(action_val);
		}
		auto spec_id_val = yyjson_obj_get(obj, "spec-id");
		if (spec_id_val) {
			result.spec_id = yyjson_get_sint(spec_id_val);
		} else {
			throw IOException("SetDefaultSpecUpdate required property 'spec-id' is missing");
		}
		return result;
	}
public:
	SetDefaultSpecUpdate() {}
public:
	string action;
	int64_t spec_id;
};

class AddSortOrderUpdate {
public:
	static AddSortOrderUpdate FromJSON(yyjson_val *obj) {
		AddSortOrderUpdate result;
		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			result.action = yyjson_get_str(action_val);
		}
		auto sort_order_val = yyjson_obj_get(obj, "sort-order");
		if (sort_order_val) {
			result.sort_order = SortOrder::FromJSON(sort_order_val);
		} else {
			throw IOException("AddSortOrderUpdate required property 'sort-order' is missing");
		}
		return result;
	}
public:
	AddSortOrderUpdate() {}
public:
	string action;
	SortOrder sort_order;
};

class SetDefaultSortOrderUpdate {
public:
	static SetDefaultSortOrderUpdate FromJSON(yyjson_val *obj) {
		SetDefaultSortOrderUpdate result;
		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			result.action = yyjson_get_str(action_val);
		}
		auto sort_order_id_val = yyjson_obj_get(obj, "sort-order-id");
		if (sort_order_id_val) {
			result.sort_order_id = yyjson_get_sint(sort_order_id_val);
		} else {
			throw IOException("SetDefaultSortOrderUpdate required property 'sort-order-id' is missing");
		}
		return result;
	}
public:
	SetDefaultSortOrderUpdate() {}
public:
	string action;
	int64_t sort_order_id;
};

class AddSnapshotUpdate {
public:
	static AddSnapshotUpdate FromJSON(yyjson_val *obj) {
		AddSnapshotUpdate result;
		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			result.action = yyjson_get_str(action_val);
		}
		auto snapshot_val = yyjson_obj_get(obj, "snapshot");
		if (snapshot_val) {
			result.snapshot = Snapshot::FromJSON(snapshot_val);
		} else {
			throw IOException("AddSnapshotUpdate required property 'snapshot' is missing");
		}
		return result;
	}
public:
	AddSnapshotUpdate() {}
public:
	string action;
	Snapshot snapshot;
};

class SetSnapshotRefUpdate {
public:
	static SetSnapshotRefUpdate FromJSON(yyjson_val *obj) {
		SetSnapshotRefUpdate result;
		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			result.action = yyjson_get_str(action_val);
		}
		auto ref_name_val = yyjson_obj_get(obj, "ref-name");
		if (ref_name_val) {
			result.ref_name = yyjson_get_str(ref_name_val);
		} else {
			throw IOException("SetSnapshotRefUpdate required property 'ref-name' is missing");
		}
		return result;
	}
public:
	SetSnapshotRefUpdate() {}
public:
	string action;
	string ref_name;
};

class RemoveSnapshotsUpdate {
public:
	static RemoveSnapshotsUpdate FromJSON(yyjson_val *obj) {
		RemoveSnapshotsUpdate result;
		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			result.action = yyjson_get_str(action_val);
		}
		auto snapshot_ids_val = yyjson_obj_get(obj, "snapshot-ids");
		if (snapshot_ids_val) {
			result.snapshot_ids = parse_obj_array<int64_t>(snapshot_ids_val);
		} else {
			throw IOException("RemoveSnapshotsUpdate required property 'snapshot-ids' is missing");
		}
		return result;
	}
public:
	RemoveSnapshotsUpdate() {}
public:
	string action;
	vector<int64_t> snapshot_ids;
};

class RemoveSnapshotRefUpdate {
public:
	static RemoveSnapshotRefUpdate FromJSON(yyjson_val *obj) {
		RemoveSnapshotRefUpdate result;
		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			result.action = yyjson_get_str(action_val);
		}
		auto ref_name_val = yyjson_obj_get(obj, "ref-name");
		if (ref_name_val) {
			result.ref_name = yyjson_get_str(ref_name_val);
		} else {
			throw IOException("RemoveSnapshotRefUpdate required property 'ref-name' is missing");
		}
		return result;
	}
public:
	RemoveSnapshotRefUpdate() {}
public:
	string action;
	string ref_name;
};

class SetLocationUpdate {
public:
	static SetLocationUpdate FromJSON(yyjson_val *obj) {
		SetLocationUpdate result;
		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			result.action = yyjson_get_str(action_val);
		}
		auto location_val = yyjson_obj_get(obj, "location");
		if (location_val) {
			result.location = yyjson_get_str(location_val);
		} else {
			throw IOException("SetLocationUpdate required property 'location' is missing");
		}
		return result;
	}
public:
	SetLocationUpdate() {}
public:
	string action;
	string location;
};

class SetPropertiesUpdate {
public:
	static SetPropertiesUpdate FromJSON(yyjson_val *obj) {
		SetPropertiesUpdate result;
		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			result.action = yyjson_get_str(action_val);
		}
		auto updates_val = yyjson_obj_get(obj, "updates");
		if (updates_val) {
			result.updates = ObjectOfStrings::FromJSON(updates_val);
		} else {
			throw IOException("SetPropertiesUpdate required property 'updates' is missing");
		}
		return result;
	}
public:
	SetPropertiesUpdate() {}
public:
	string action;
	ObjectOfStrings updates;
};

class RemovePropertiesUpdate {
public:
	static RemovePropertiesUpdate FromJSON(yyjson_val *obj) {
		RemovePropertiesUpdate result;
		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			result.action = yyjson_get_str(action_val);
		}
		auto removals_val = yyjson_obj_get(obj, "removals");
		if (removals_val) {
			result.removals = parse_str_array(removals_val);
		} else {
			throw IOException("RemovePropertiesUpdate required property 'removals' is missing");
		}
		return result;
	}
public:
	RemovePropertiesUpdate() {}
public:
	string action;
	vector<string> removals;
};

class AddViewVersionUpdate {
public:
	static AddViewVersionUpdate FromJSON(yyjson_val *obj) {
		AddViewVersionUpdate result;
		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			result.action = yyjson_get_str(action_val);
		}
		auto view_version_val = yyjson_obj_get(obj, "view-version");
		if (view_version_val) {
			result.view_version = ViewVersion::FromJSON(view_version_val);
		} else {
			throw IOException("AddViewVersionUpdate required property 'view-version' is missing");
		}
		return result;
	}
public:
	AddViewVersionUpdate() {}
public:
	string action;
	ViewVersion view_version;
};

class SetCurrentViewVersionUpdate {
public:
	static SetCurrentViewVersionUpdate FromJSON(yyjson_val *obj) {
		SetCurrentViewVersionUpdate result;
		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			result.action = yyjson_get_str(action_val);
		}
		auto view_version_id_val = yyjson_obj_get(obj, "view-version-id");
		if (view_version_id_val) {
			result.view_version_id = yyjson_get_sint(view_version_id_val);
		} else {
			throw IOException("SetCurrentViewVersionUpdate required property 'view-version-id' is missing");
		}
		return result;
	}
public:
	SetCurrentViewVersionUpdate() {}
public:
	string action;
	int64_t view_version_id;
};

class RemoveStatisticsUpdate {
public:
	static RemoveStatisticsUpdate FromJSON(yyjson_val *obj) {
		RemoveStatisticsUpdate result;
		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			result.action = yyjson_get_str(action_val);
		}
		auto snapshot_id_val = yyjson_obj_get(obj, "snapshot-id");
		if (snapshot_id_val) {
			result.snapshot_id = yyjson_get_sint(snapshot_id_val);
		} else {
			throw IOException("RemoveStatisticsUpdate required property 'snapshot-id' is missing");
		}
		return result;
	}
public:
	RemoveStatisticsUpdate() {}
public:
	string action;
	int64_t snapshot_id;
};

class RemovePartitionStatisticsUpdate {
public:
	static RemovePartitionStatisticsUpdate FromJSON(yyjson_val *obj) {
		RemovePartitionStatisticsUpdate result;
		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			result.action = yyjson_get_str(action_val);
		}
		auto snapshot_id_val = yyjson_obj_get(obj, "snapshot-id");
		if (snapshot_id_val) {
			result.snapshot_id = yyjson_get_sint(snapshot_id_val);
		} else {
			throw IOException("RemovePartitionStatisticsUpdate required property 'snapshot-id' is missing");
		}
		return result;
	}
public:
	RemovePartitionStatisticsUpdate() {}
public:
	string action;
	int64_t snapshot_id;
};

class RemovePartitionSpecsUpdate {
public:
	static RemovePartitionSpecsUpdate FromJSON(yyjson_val *obj) {
		RemovePartitionSpecsUpdate result;
		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			result.action = yyjson_get_str(action_val);
		}
		auto spec_ids_val = yyjson_obj_get(obj, "spec-ids");
		if (spec_ids_val) {
			result.spec_ids = parse_obj_array<int64_t>(spec_ids_val);
		} else {
			throw IOException("RemovePartitionSpecsUpdate required property 'spec-ids' is missing");
		}
		return result;
	}
public:
	RemovePartitionSpecsUpdate() {}
public:
	string action;
	vector<int64_t> spec_ids;
};

class RemoveSchemasUpdate {
public:
	static RemoveSchemasUpdate FromJSON(yyjson_val *obj) {
		RemoveSchemasUpdate result;
		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			result.action = yyjson_get_str(action_val);
		}
		auto schema_ids_val = yyjson_obj_get(obj, "schema-ids");
		if (schema_ids_val) {
			result.schema_ids = parse_obj_array<int64_t>(schema_ids_val);
		} else {
			throw IOException("RemoveSchemasUpdate required property 'schema-ids' is missing");
		}
		return result;
	}
public:
	RemoveSchemasUpdate() {}
public:
	string action;
	vector<int64_t> schema_ids;
};

class EnableRowLineageUpdate {
public:
	static EnableRowLineageUpdate FromJSON(yyjson_val *obj) {
		EnableRowLineageUpdate result;
		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			result.action = yyjson_get_str(action_val);
		}
		return result;
	}
public:
	EnableRowLineageUpdate() {}
public:
	string action;
};

class TableUpdate {
public:
	static TableUpdate FromJSON(yyjson_val *obj) {
		TableUpdate result;
		return result;
	}
public:
	TableUpdate() {}
public:
};

class ViewUpdate {
public:
	static ViewUpdate FromJSON(yyjson_val *obj) {
		ViewUpdate result;
		return result;
	}
public:
	ViewUpdate() {}
public:
};

class TableRequirement {
public:
	static TableRequirement FromJSON(yyjson_val *obj) {
		TableRequirement result;
		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = yyjson_get_str(type_val);
		} else {
			throw IOException("TableRequirement required property 'type' is missing");
		}
		return result;
	}
public:
	TableRequirement() {}
public:
	string type;
};

class AssertCreate {
public:
	static AssertCreate FromJSON(yyjson_val *obj) {
		AssertCreate result;
		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = yyjson_get_str(type_val);
		} else {
			throw IOException("AssertCreate required property 'type' is missing");
		}
		return result;
	}
public:
	AssertCreate() {}
public:
	string type;
};

class AssertTableUUID {
public:
	static AssertTableUUID FromJSON(yyjson_val *obj) {
		AssertTableUUID result;
		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = yyjson_get_str(type_val);
		} else {
			throw IOException("AssertTableUUID required property 'type' is missing");
		}
		auto uuid_val = yyjson_obj_get(obj, "uuid");
		if (uuid_val) {
			result.uuid = yyjson_get_str(uuid_val);
		} else {
			throw IOException("AssertTableUUID required property 'uuid' is missing");
		}
		return result;
	}
public:
	AssertTableUUID() {}
public:
	string type;
	string uuid;
};

class AssertRefSnapshotId {
public:
	static AssertRefSnapshotId FromJSON(yyjson_val *obj) {
		AssertRefSnapshotId result;
		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = yyjson_get_str(type_val);
		}
		auto ref_val = yyjson_obj_get(obj, "ref");
		if (ref_val) {
			result.ref = yyjson_get_str(ref_val);
		} else {
			throw IOException("AssertRefSnapshotId required property 'ref' is missing");
		}
		auto snapshot_id_val = yyjson_obj_get(obj, "snapshot-id");
		if (snapshot_id_val) {
			result.snapshot_id = yyjson_get_sint(snapshot_id_val);
		} else {
			throw IOException("AssertRefSnapshotId required property 'snapshot-id' is missing");
		}
		return result;
	}
public:
	AssertRefSnapshotId() {}
public:
	string type;
	string ref;
	int64_t snapshot_id;
};

class AssertLastAssignedFieldId {
public:
	static AssertLastAssignedFieldId FromJSON(yyjson_val *obj) {
		AssertLastAssignedFieldId result;
		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = yyjson_get_str(type_val);
		}
		auto last_assigned_field_id_val = yyjson_obj_get(obj, "last-assigned-field-id");
		if (last_assigned_field_id_val) {
			result.last_assigned_field_id = yyjson_get_sint(last_assigned_field_id_val);
		} else {
			throw IOException("AssertLastAssignedFieldId required property 'last-assigned-field-id' is missing");
		}
		return result;
	}
public:
	AssertLastAssignedFieldId() {}
public:
	string type;
	int64_t last_assigned_field_id;
};

class AssertCurrentSchemaId {
public:
	static AssertCurrentSchemaId FromJSON(yyjson_val *obj) {
		AssertCurrentSchemaId result;
		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = yyjson_get_str(type_val);
		}
		auto current_schema_id_val = yyjson_obj_get(obj, "current-schema-id");
		if (current_schema_id_val) {
			result.current_schema_id = yyjson_get_sint(current_schema_id_val);
		} else {
			throw IOException("AssertCurrentSchemaId required property 'current-schema-id' is missing");
		}
		return result;
	}
public:
	AssertCurrentSchemaId() {}
public:
	string type;
	int64_t current_schema_id;
};

class AssertLastAssignedPartitionId {
public:
	static AssertLastAssignedPartitionId FromJSON(yyjson_val *obj) {
		AssertLastAssignedPartitionId result;
		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = yyjson_get_str(type_val);
		}
		auto last_assigned_partition_id_val = yyjson_obj_get(obj, "last-assigned-partition-id");
		if (last_assigned_partition_id_val) {
			result.last_assigned_partition_id = yyjson_get_sint(last_assigned_partition_id_val);
		} else {
			throw IOException("AssertLastAssignedPartitionId required property 'last-assigned-partition-id' is missing");
		}
		return result;
	}
public:
	AssertLastAssignedPartitionId() {}
public:
	string type;
	int64_t last_assigned_partition_id;
};

class AssertDefaultSpecId {
public:
	static AssertDefaultSpecId FromJSON(yyjson_val *obj) {
		AssertDefaultSpecId result;
		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = yyjson_get_str(type_val);
		}
		auto default_spec_id_val = yyjson_obj_get(obj, "default-spec-id");
		if (default_spec_id_val) {
			result.default_spec_id = yyjson_get_sint(default_spec_id_val);
		} else {
			throw IOException("AssertDefaultSpecId required property 'default-spec-id' is missing");
		}
		return result;
	}
public:
	AssertDefaultSpecId() {}
public:
	string type;
	int64_t default_spec_id;
};

class AssertDefaultSortOrderId {
public:
	static AssertDefaultSortOrderId FromJSON(yyjson_val *obj) {
		AssertDefaultSortOrderId result;
		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = yyjson_get_str(type_val);
		}
		auto default_sort_order_id_val = yyjson_obj_get(obj, "default-sort-order-id");
		if (default_sort_order_id_val) {
			result.default_sort_order_id = yyjson_get_sint(default_sort_order_id_val);
		} else {
			throw IOException("AssertDefaultSortOrderId required property 'default-sort-order-id' is missing");
		}
		return result;
	}
public:
	AssertDefaultSortOrderId() {}
public:
	string type;
	int64_t default_sort_order_id;
};

class ViewRequirement {
public:
	static ViewRequirement FromJSON(yyjson_val *obj) {
		ViewRequirement result;
		return result;
	}
public:
	ViewRequirement() {}
public:
};

class AssertViewUUID {
public:
	static AssertViewUUID FromJSON(yyjson_val *obj) {
		AssertViewUUID result;
		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = yyjson_get_str(type_val);
		} else {
			throw IOException("AssertViewUUID required property 'type' is missing");
		}
		auto uuid_val = yyjson_obj_get(obj, "uuid");
		if (uuid_val) {
			result.uuid = yyjson_get_str(uuid_val);
		} else {
			throw IOException("AssertViewUUID required property 'uuid' is missing");
		}
		return result;
	}
public:
	AssertViewUUID() {}
public:
	string type;
	string uuid;
};

class StorageCredential {
public:
	static StorageCredential FromJSON(yyjson_val *obj) {
		StorageCredential result;
		auto prefix_val = yyjson_obj_get(obj, "prefix");
		if (prefix_val) {
			result.prefix = yyjson_get_str(prefix_val);
		} else {
			throw IOException("StorageCredential required property 'prefix' is missing");
		}
		auto config_val = yyjson_obj_get(obj, "config");
		if (config_val) {
			result.config = ObjectOfStrings::FromJSON(config_val);
		} else {
			throw IOException("StorageCredential required property 'config' is missing");
		}
		return result;
	}
public:
	StorageCredential() {}
public:
	string prefix;
	ObjectOfStrings config;
};

class LoadCredentialsResponse {
public:
	static LoadCredentialsResponse FromJSON(yyjson_val *obj) {
		LoadCredentialsResponse result;
		auto storage_credentials_val = yyjson_obj_get(obj, "storage-credentials");
		if (storage_credentials_val) {
			result.storage_credentials = parse_obj_array<StorageCredential>(storage_credentials_val);
		} else {
			throw IOException("LoadCredentialsResponse required property 'storage-credentials' is missing");
		}
		return result;
	}
public:
	LoadCredentialsResponse() {}
public:
	vector<StorageCredential> storage_credentials;
};

class LoadTableResult {
public:
	static LoadTableResult FromJSON(yyjson_val *obj) {
		LoadTableResult result;
		auto metadata_location_val = yyjson_obj_get(obj, "metadata-location");
		if (metadata_location_val) {
			result.metadata_location = yyjson_get_str(metadata_location_val);
		}
		auto metadata_val = yyjson_obj_get(obj, "metadata");
		if (metadata_val) {
			result.metadata = TableMetadata::FromJSON(metadata_val);
		} else {
			throw IOException("LoadTableResult required property 'metadata' is missing");
		}
		auto config_val = yyjson_obj_get(obj, "config");
		if (config_val) {
			result.config = ObjectOfStrings::FromJSON(config_val);
		}
		auto storage_credentials_val = yyjson_obj_get(obj, "storage-credentials");
		if (storage_credentials_val) {
			result.storage_credentials = parse_obj_array<StorageCredential>(storage_credentials_val);
		}
		return result;
	}
public:
	LoadTableResult() {}
public:
	string metadata_location;
	TableMetadata metadata;
	ObjectOfStrings config;
	vector<StorageCredential> storage_credentials;
};

class ScanTasks {
public:
	static ScanTasks FromJSON(yyjson_val *obj) {
		ScanTasks result;
		auto delete_files_val = yyjson_obj_get(obj, "delete-files");
		if (delete_files_val) {
			result.delete_files = parse_obj_array<DeleteFile>(delete_files_val);
		}
		auto file_scan_tasks_val = yyjson_obj_get(obj, "file-scan-tasks");
		if (file_scan_tasks_val) {
			result.file_scan_tasks = parse_obj_array<FileScanTask>(file_scan_tasks_val);
		}
		auto plan_tasks_val = yyjson_obj_get(obj, "plan-tasks");
		if (plan_tasks_val) {
			result.plan_tasks = parse_obj_array<PlanTask>(plan_tasks_val);
		}
		return result;
	}
public:
	ScanTasks() {}
public:
	vector<DeleteFile> delete_files;
	vector<FileScanTask> file_scan_tasks;
	vector<PlanTask> plan_tasks;
};

class CompletedPlanningWithIDResult {
public:
	static CompletedPlanningWithIDResult FromJSON(yyjson_val *obj) {
		CompletedPlanningWithIDResult result;
		auto plan_id_val = yyjson_obj_get(obj, "plan-id");
		if (plan_id_val) {
			result.plan_id = yyjson_get_str(plan_id_val);
		}
		return result;
	}
public:
	CompletedPlanningWithIDResult() {}
public:
	string plan_id;
};

class PlanStatus {
public:
	static PlanStatus FromJSON(yyjson_val *obj) {
		PlanStatus result;
		return result;
	}
public:
	PlanStatus() {}
public:
};

class FetchPlanningResult {
public:
	static FetchPlanningResult FromJSON(yyjson_val *obj) {
		FetchPlanningResult result;
		return result;
	}
public:
	FetchPlanningResult() {}
public:
};

class PlanTableScanResult {
public:
	static PlanTableScanResult FromJSON(yyjson_val *obj) {
		PlanTableScanResult result;
		return result;
	}
public:
	PlanTableScanResult() {}
public:
};

class FetchScanTasksResult {
public:
	static FetchScanTasksResult FromJSON(yyjson_val *obj) {
		FetchScanTasksResult result;
		return result;
	}
public:
	FetchScanTasksResult() {}
public:
};

class CommitTableRequest {
public:
	static CommitTableRequest FromJSON(yyjson_val *obj) {
		CommitTableRequest result;
		auto identifier_val = yyjson_obj_get(obj, "identifier");
		if (identifier_val) {
			result.identifier = TableIdentifier::FromJSON(identifier_val);
		}
		auto requirements_val = yyjson_obj_get(obj, "requirements");
		if (requirements_val) {
			result.requirements = parse_obj_array<TableRequirement>(requirements_val);
		} else {
			throw IOException("CommitTableRequest required property 'requirements' is missing");
		}
		auto updates_val = yyjson_obj_get(obj, "updates");
		if (updates_val) {
			result.updates = parse_obj_array<TableUpdate>(updates_val);
		} else {
			throw IOException("CommitTableRequest required property 'updates' is missing");
		}
		return result;
	}
public:
	CommitTableRequest() {}
public:
	TableIdentifier identifier;
	vector<TableRequirement> requirements;
	vector<TableUpdate> updates;
};

class CommitViewRequest {
public:
	static CommitViewRequest FromJSON(yyjson_val *obj) {
		CommitViewRequest result;
		auto identifier_val = yyjson_obj_get(obj, "identifier");
		if (identifier_val) {
			result.identifier = TableIdentifier::FromJSON(identifier_val);
		}
		auto requirements_val = yyjson_obj_get(obj, "requirements");
		if (requirements_val) {
			result.requirements = parse_obj_array<ViewRequirement>(requirements_val);
		}
		auto updates_val = yyjson_obj_get(obj, "updates");
		if (updates_val) {
			result.updates = parse_obj_array<ViewUpdate>(updates_val);
		} else {
			throw IOException("CommitViewRequest required property 'updates' is missing");
		}
		return result;
	}
public:
	CommitViewRequest() {}
public:
	TableIdentifier identifier;
	vector<ViewRequirement> requirements;
	vector<ViewUpdate> updates;
};

class CommitTransactionRequest {
public:
	static CommitTransactionRequest FromJSON(yyjson_val *obj) {
		CommitTransactionRequest result;
		auto table_changes_val = yyjson_obj_get(obj, "table-changes");
		if (table_changes_val) {
			result.table_changes = parse_obj_array<CommitTableRequest>(table_changes_val);
		} else {
			throw IOException("CommitTransactionRequest required property 'table-changes' is missing");
		}
		return result;
	}
public:
	CommitTransactionRequest() {}
public:
	vector<CommitTableRequest> table_changes;
};

class CreateTableRequest {
public:
	static CreateTableRequest FromJSON(yyjson_val *obj) {
		CreateTableRequest result;
		auto name_val = yyjson_obj_get(obj, "name");
		if (name_val) {
			result.name = yyjson_get_str(name_val);
		} else {
			throw IOException("CreateTableRequest required property 'name' is missing");
		}
		auto location_val = yyjson_obj_get(obj, "location");
		if (location_val) {
			result.location = yyjson_get_str(location_val);
		}
		auto schema_val = yyjson_obj_get(obj, "schema");
		if (schema_val) {
			result.schema = Schema::FromJSON(schema_val);
		} else {
			throw IOException("CreateTableRequest required property 'schema' is missing");
		}
		auto partition_spec_val = yyjson_obj_get(obj, "partition-spec");
		if (partition_spec_val) {
			result.partition_spec = PartitionSpec::FromJSON(partition_spec_val);
		}
		auto write_order_val = yyjson_obj_get(obj, "write-order");
		if (write_order_val) {
			result.write_order = SortOrder::FromJSON(write_order_val);
		}
		auto stage_create_val = yyjson_obj_get(obj, "stage-create");
		if (stage_create_val) {
			result.stage_create = yyjson_get_bool(stage_create_val);
		}
		auto properties_val = yyjson_obj_get(obj, "properties");
		if (properties_val) {
			result.properties = ObjectOfStrings::FromJSON(properties_val);
		}
		return result;
	}
public:
	CreateTableRequest() {}
public:
	string name;
	string location;
	Schema schema;
	PartitionSpec partition_spec;
	SortOrder write_order;
	bool stage_create;
	ObjectOfStrings properties;
};

class RegisterTableRequest {
public:
	static RegisterTableRequest FromJSON(yyjson_val *obj) {
		RegisterTableRequest result;
		auto name_val = yyjson_obj_get(obj, "name");
		if (name_val) {
			result.name = yyjson_get_str(name_val);
		} else {
			throw IOException("RegisterTableRequest required property 'name' is missing");
		}
		auto metadata_location_val = yyjson_obj_get(obj, "metadata-location");
		if (metadata_location_val) {
			result.metadata_location = yyjson_get_str(metadata_location_val);
		} else {
			throw IOException("RegisterTableRequest required property 'metadata-location' is missing");
		}
		auto overwrite_val = yyjson_obj_get(obj, "overwrite");
		if (overwrite_val) {
			result.overwrite = yyjson_get_bool(overwrite_val);
		}
		return result;
	}
public:
	RegisterTableRequest() {}
public:
	string name;
	string metadata_location;
	bool overwrite;
};

class CreateViewRequest {
public:
	static CreateViewRequest FromJSON(yyjson_val *obj) {
		CreateViewRequest result;
		auto name_val = yyjson_obj_get(obj, "name");
		if (name_val) {
			result.name = yyjson_get_str(name_val);
		} else {
			throw IOException("CreateViewRequest required property 'name' is missing");
		}
		auto location_val = yyjson_obj_get(obj, "location");
		if (location_val) {
			result.location = yyjson_get_str(location_val);
		}
		auto schema_val = yyjson_obj_get(obj, "schema");
		if (schema_val) {
			result.schema = Schema::FromJSON(schema_val);
		} else {
			throw IOException("CreateViewRequest required property 'schema' is missing");
		}
		auto view_version_val = yyjson_obj_get(obj, "view-version");
		if (view_version_val) {
			result.view_version = ViewVersion::FromJSON(view_version_val);
		} else {
			throw IOException("CreateViewRequest required property 'view-version' is missing");
		}
		auto properties_val = yyjson_obj_get(obj, "properties");
		if (properties_val) {
			result.properties = ObjectOfStrings::FromJSON(properties_val);
		} else {
			throw IOException("CreateViewRequest required property 'properties' is missing");
		}
		return result;
	}
public:
	CreateViewRequest() {}
public:
	string name;
	string location;
	Schema schema;
	ViewVersion view_version;
	ObjectOfStrings properties;
};

class LoadViewResult {
public:
	static LoadViewResult FromJSON(yyjson_val *obj) {
		LoadViewResult result;
		auto metadata_location_val = yyjson_obj_get(obj, "metadata-location");
		if (metadata_location_val) {
			result.metadata_location = yyjson_get_str(metadata_location_val);
		} else {
			throw IOException("LoadViewResult required property 'metadata-location' is missing");
		}
		auto metadata_val = yyjson_obj_get(obj, "metadata");
		if (metadata_val) {
			result.metadata = ViewMetadata::FromJSON(metadata_val);
		} else {
			throw IOException("LoadViewResult required property 'metadata' is missing");
		}
		auto config_val = yyjson_obj_get(obj, "config");
		if (config_val) {
			result.config = ObjectOfStrings::FromJSON(config_val);
		}
		return result;
	}
public:
	LoadViewResult() {}
public:
	string metadata_location;
	ViewMetadata metadata;
	ObjectOfStrings config;
};

class TokenType {
public:
	static TokenType FromJSON(yyjson_val *obj) {
		TokenType result;
		return result;
	}
public:
	TokenType() {}
public:
};

class OAuthClientCredentialsRequest {
public:
	static OAuthClientCredentialsRequest FromJSON(yyjson_val *obj) {
		OAuthClientCredentialsRequest result;
		auto grant_type_val = yyjson_obj_get(obj, "grant_type");
		if (grant_type_val) {
			result.grant_type = yyjson_get_str(grant_type_val);
		} else {
			throw IOException("OAuthClientCredentialsRequest required property 'grant_type' is missing");
		}
		auto scope_val = yyjson_obj_get(obj, "scope");
		if (scope_val) {
			result.scope = yyjson_get_str(scope_val);
		}
		auto client_id_val = yyjson_obj_get(obj, "client_id");
		if (client_id_val) {
			result.client_id = yyjson_get_str(client_id_val);
		} else {
			throw IOException("OAuthClientCredentialsRequest required property 'client_id' is missing");
		}
		auto client_secret_val = yyjson_obj_get(obj, "client_secret");
		if (client_secret_val) {
			result.client_secret = yyjson_get_str(client_secret_val);
		} else {
			throw IOException("OAuthClientCredentialsRequest required property 'client_secret' is missing");
		}
		return result;
	}
public:
	OAuthClientCredentialsRequest() {}
public:
	string grant_type;
	string scope;
	string client_id;
	string client_secret;
};

class OAuthTokenExchangeRequest {
public:
	static OAuthTokenExchangeRequest FromJSON(yyjson_val *obj) {
		OAuthTokenExchangeRequest result;
		auto grant_type_val = yyjson_obj_get(obj, "grant_type");
		if (grant_type_val) {
			result.grant_type = yyjson_get_str(grant_type_val);
		} else {
			throw IOException("OAuthTokenExchangeRequest required property 'grant_type' is missing");
		}
		auto scope_val = yyjson_obj_get(obj, "scope");
		if (scope_val) {
			result.scope = yyjson_get_str(scope_val);
		}
		auto requested_token_type_val = yyjson_obj_get(obj, "requested_token_type");
		if (requested_token_type_val) {
			result.requested_token_type = TokenType::FromJSON(requested_token_type_val);
		}
		auto subject_token_val = yyjson_obj_get(obj, "subject_token");
		if (subject_token_val) {
			result.subject_token = yyjson_get_str(subject_token_val);
		} else {
			throw IOException("OAuthTokenExchangeRequest required property 'subject_token' is missing");
		}
		auto subject_token_type_val = yyjson_obj_get(obj, "subject_token_type");
		if (subject_token_type_val) {
			result.subject_token_type = TokenType::FromJSON(subject_token_type_val);
		} else {
			throw IOException("OAuthTokenExchangeRequest required property 'subject_token_type' is missing");
		}
		auto actor_token_val = yyjson_obj_get(obj, "actor_token");
		if (actor_token_val) {
			result.actor_token = yyjson_get_str(actor_token_val);
		}
		auto actor_token_type_val = yyjson_obj_get(obj, "actor_token_type");
		if (actor_token_type_val) {
			result.actor_token_type = TokenType::FromJSON(actor_token_type_val);
		}
		return result;
	}
public:
	OAuthTokenExchangeRequest() {}
public:
	string grant_type;
	string scope;
	TokenType requested_token_type;
	string subject_token;
	TokenType subject_token_type;
	string actor_token;
	TokenType actor_token_type;
};

class OAuthTokenRequest {
public:
	static OAuthTokenRequest FromJSON(yyjson_val *obj) {
		OAuthTokenRequest result;
		return result;
	}
public:
	OAuthTokenRequest() {}
public:
};

class CounterResult {
public:
	static CounterResult FromJSON(yyjson_val *obj) {
		CounterResult result;
		auto unit_val = yyjson_obj_get(obj, "unit");
		if (unit_val) {
			result.unit = yyjson_get_str(unit_val);
		} else {
			throw IOException("CounterResult required property 'unit' is missing");
		}
		auto value_val = yyjson_obj_get(obj, "value");
		if (value_val) {
			result.value = yyjson_get_sint(value_val);
		} else {
			throw IOException("CounterResult required property 'value' is missing");
		}
		return result;
	}
public:
	CounterResult() {}
public:
	string unit;
	int64_t value;
};

class TimerResult {
public:
	static TimerResult FromJSON(yyjson_val *obj) {
		TimerResult result;
		auto time_unit_val = yyjson_obj_get(obj, "time-unit");
		if (time_unit_val) {
			result.time_unit = yyjson_get_str(time_unit_val);
		} else {
			throw IOException("TimerResult required property 'time-unit' is missing");
		}
		auto count_val = yyjson_obj_get(obj, "count");
		if (count_val) {
			result.count = yyjson_get_sint(count_val);
		} else {
			throw IOException("TimerResult required property 'count' is missing");
		}
		auto total_duration_val = yyjson_obj_get(obj, "total-duration");
		if (total_duration_val) {
			result.total_duration = yyjson_get_sint(total_duration_val);
		} else {
			throw IOException("TimerResult required property 'total-duration' is missing");
		}
		return result;
	}
public:
	TimerResult() {}
public:
	string time_unit;
	int64_t count;
	int64_t total_duration;
};

class MetricResult {
public:
	static MetricResult FromJSON(yyjson_val *obj) {
		MetricResult result;
		return result;
	}
public:
	MetricResult() {}
public:
};

class Metrics {
public:
	static Metrics FromJSON(yyjson_val *obj) {
		Metrics result;
		return result;
	}
public:
	Metrics() {}
public:
};

class ReportMetricsRequest {
public:
	static ReportMetricsRequest FromJSON(yyjson_val *obj) {
		ReportMetricsRequest result;
		auto report_type_val = yyjson_obj_get(obj, "report-type");
		if (report_type_val) {
			result.report_type = yyjson_get_str(report_type_val);
		} else {
			throw IOException("ReportMetricsRequest required property 'report-type' is missing");
		}
		return result;
	}
public:
	ReportMetricsRequest() {}
public:
	string report_type;
};

class ScanReport {
public:
	static ScanReport FromJSON(yyjson_val *obj) {
		ScanReport result;
		auto table_name_val = yyjson_obj_get(obj, "table-name");
		if (table_name_val) {
			result.table_name = yyjson_get_str(table_name_val);
		} else {
			throw IOException("ScanReport required property 'table-name' is missing");
		}
		auto snapshot_id_val = yyjson_obj_get(obj, "snapshot-id");
		if (snapshot_id_val) {
			result.snapshot_id = yyjson_get_sint(snapshot_id_val);
		} else {
			throw IOException("ScanReport required property 'snapshot-id' is missing");
		}
		auto filter_val = yyjson_obj_get(obj, "filter");
		if (filter_val) {
			result.filter = Expression::FromJSON(filter_val);
		} else {
			throw IOException("ScanReport required property 'filter' is missing");
		}
		auto schema_id_val = yyjson_obj_get(obj, "schema-id");
		if (schema_id_val) {
			result.schema_id = yyjson_get_sint(schema_id_val);
		} else {
			throw IOException("ScanReport required property 'schema-id' is missing");
		}
		auto projected_field_ids_val = yyjson_obj_get(obj, "projected-field-ids");
		if (projected_field_ids_val) {
			result.projected_field_ids = parse_obj_array<int64_t>(projected_field_ids_val);
		} else {
			throw IOException("ScanReport required property 'projected-field-ids' is missing");
		}
		auto projected_field_names_val = yyjson_obj_get(obj, "projected-field-names");
		if (projected_field_names_val) {
			result.projected_field_names = parse_str_array(projected_field_names_val);
		} else {
			throw IOException("ScanReport required property 'projected-field-names' is missing");
		}
		auto metrics_val = yyjson_obj_get(obj, "metrics");
		if (metrics_val) {
			result.metrics = Metrics::FromJSON(metrics_val);
		} else {
			throw IOException("ScanReport required property 'metrics' is missing");
		}
		auto metadata_val = yyjson_obj_get(obj, "metadata");
		if (metadata_val) {
			result.metadata = ObjectOfStrings::FromJSON(metadata_val);
		}
		return result;
	}
public:
	ScanReport() {}
public:
	string table_name;
	int64_t snapshot_id;
	Expression filter;
	int64_t schema_id;
	vector<int64_t> projected_field_ids;
	vector<string> projected_field_names;
	Metrics metrics;
	ObjectOfStrings metadata;
};

class CommitReport {
public:
	static CommitReport FromJSON(yyjson_val *obj) {
		CommitReport result;
		auto table_name_val = yyjson_obj_get(obj, "table-name");
		if (table_name_val) {
			result.table_name = yyjson_get_str(table_name_val);
		} else {
			throw IOException("CommitReport required property 'table-name' is missing");
		}
		auto snapshot_id_val = yyjson_obj_get(obj, "snapshot-id");
		if (snapshot_id_val) {
			result.snapshot_id = yyjson_get_sint(snapshot_id_val);
		} else {
			throw IOException("CommitReport required property 'snapshot-id' is missing");
		}
		auto sequence_number_val = yyjson_obj_get(obj, "sequence-number");
		if (sequence_number_val) {
			result.sequence_number = yyjson_get_sint(sequence_number_val);
		} else {
			throw IOException("CommitReport required property 'sequence-number' is missing");
		}
		auto operation_val = yyjson_obj_get(obj, "operation");
		if (operation_val) {
			result.operation = yyjson_get_str(operation_val);
		} else {
			throw IOException("CommitReport required property 'operation' is missing");
		}
		auto metrics_val = yyjson_obj_get(obj, "metrics");
		if (metrics_val) {
			result.metrics = Metrics::FromJSON(metrics_val);
		} else {
			throw IOException("CommitReport required property 'metrics' is missing");
		}
		auto metadata_val = yyjson_obj_get(obj, "metadata");
		if (metadata_val) {
			result.metadata = ObjectOfStrings::FromJSON(metadata_val);
		}
		return result;
	}
public:
	CommitReport() {}
public:
	string table_name;
	int64_t snapshot_id;
	int64_t sequence_number;
	string operation;
	Metrics metrics;
	ObjectOfStrings metadata;
};

class OAuthError {
public:
	static OAuthError FromJSON(yyjson_val *obj) {
		OAuthError result;
		auto error_val = yyjson_obj_get(obj, "error");
		if (error_val) {
			result.error = yyjson_get_str(error_val);
		} else {
			throw IOException("OAuthError required property 'error' is missing");
		}
		auto error_description_val = yyjson_obj_get(obj, "error_description");
		if (error_description_val) {
			result.error_description = yyjson_get_str(error_description_val);
		}
		auto error_uri_val = yyjson_obj_get(obj, "error_uri");
		if (error_uri_val) {
			result.error_uri = yyjson_get_str(error_uri_val);
		}
		return result;
	}
public:
	OAuthError() {}
public:
	string error;
	string error_description;
	string error_uri;
};

class OAuthTokenResponse {
public:
	static OAuthTokenResponse FromJSON(yyjson_val *obj) {
		OAuthTokenResponse result;
		auto access_token_val = yyjson_obj_get(obj, "access_token");
		if (access_token_val) {
			result.access_token = yyjson_get_str(access_token_val);
		} else {
			throw IOException("OAuthTokenResponse required property 'access_token' is missing");
		}
		auto token_type_val = yyjson_obj_get(obj, "token_type");
		if (token_type_val) {
			result.token_type = yyjson_get_str(token_type_val);
		} else {
			throw IOException("OAuthTokenResponse required property 'token_type' is missing");
		}
		auto expires_in_val = yyjson_obj_get(obj, "expires_in");
		if (expires_in_val) {
			result.expires_in = yyjson_get_sint(expires_in_val);
		}
		auto issued_token_type_val = yyjson_obj_get(obj, "issued_token_type");
		if (issued_token_type_val) {
			result.issued_token_type = TokenType::FromJSON(issued_token_type_val);
		}
		auto refresh_token_val = yyjson_obj_get(obj, "refresh_token");
		if (refresh_token_val) {
			result.refresh_token = yyjson_get_str(refresh_token_val);
		}
		auto scope_val = yyjson_obj_get(obj, "scope");
		if (scope_val) {
			result.scope = yyjson_get_str(scope_val);
		}
		return result;
	}
public:
	OAuthTokenResponse() {}
public:
	string access_token;
	string token_type;
	int64_t expires_in;
	TokenType issued_token_type;
	string refresh_token;
	string scope;
};

class IcebergErrorResponse {
public:
	static IcebergErrorResponse FromJSON(yyjson_val *obj) {
		IcebergErrorResponse result;
		auto error_val = yyjson_obj_get(obj, "error");
		if (error_val) {
			result.error = ErrorModel::FromJSON(error_val);
		} else {
			throw IOException("IcebergErrorResponse required property 'error' is missing");
		}
		return result;
	}
public:
	IcebergErrorResponse() {}
public:
	ErrorModel error;
};

class CreateNamespaceResponse {
public:
	static CreateNamespaceResponse FromJSON(yyjson_val *obj) {
		CreateNamespaceResponse result;
		auto _namespace_val = yyjson_obj_get(obj, "namespace");
		if (_namespace_val) {
			result._namespace = Namespace::FromJSON(_namespace_val);
		} else {
			throw IOException("CreateNamespaceResponse required property 'namespace' is missing");
		}
		auto properties_val = yyjson_obj_get(obj, "properties");
		if (properties_val) {
			result.properties = ObjectOfStrings::FromJSON(properties_val);
		}
		return result;
	}
public:
	CreateNamespaceResponse() {}
public:
	Namespace _namespace;
	ObjectOfStrings properties;
};

class GetNamespaceResponse {
public:
	static GetNamespaceResponse FromJSON(yyjson_val *obj) {
		GetNamespaceResponse result;
		auto _namespace_val = yyjson_obj_get(obj, "namespace");
		if (_namespace_val) {
			result._namespace = Namespace::FromJSON(_namespace_val);
		} else {
			throw IOException("GetNamespaceResponse required property 'namespace' is missing");
		}
		auto properties_val = yyjson_obj_get(obj, "properties");
		if (properties_val) {
			result.properties = ObjectOfStrings::FromJSON(properties_val);
		}
		return result;
	}
public:
	GetNamespaceResponse() {}
public:
	Namespace _namespace;
	ObjectOfStrings properties;
};

class ListTablesResponse {
public:
	static ListTablesResponse FromJSON(yyjson_val *obj) {
		ListTablesResponse result;
		auto next_page_token_val = yyjson_obj_get(obj, "next-page-token");
		if (next_page_token_val) {
			result.next_page_token = PageToken::FromJSON(next_page_token_val);
		}
		auto identifiers_val = yyjson_obj_get(obj, "identifiers");
		if (identifiers_val) {
			result.identifiers = parse_obj_array<TableIdentifier>(identifiers_val);
		}
		return result;
	}
public:
	ListTablesResponse() {}
public:
	PageToken next_page_token;
	vector<TableIdentifier> identifiers;
};

class ListNamespacesResponse {
public:
	static ListNamespacesResponse FromJSON(yyjson_val *obj) {
		ListNamespacesResponse result;
		auto next_page_token_val = yyjson_obj_get(obj, "next-page-token");
		if (next_page_token_val) {
			result.next_page_token = PageToken::FromJSON(next_page_token_val);
		}
		auto namespaces_val = yyjson_obj_get(obj, "namespaces");
		if (namespaces_val) {
			result.namespaces = parse_obj_array<Namespace>(namespaces_val);
		}
		return result;
	}
public:
	ListNamespacesResponse() {}
public:
	PageToken next_page_token;
	vector<Namespace> namespaces;
};

class UpdateNamespacePropertiesResponse {
public:
	static UpdateNamespacePropertiesResponse FromJSON(yyjson_val *obj) {
		UpdateNamespacePropertiesResponse result;
		auto updated_val = yyjson_obj_get(obj, "updated");
		if (updated_val) {
			result.updated = parse_str_array(updated_val);
		} else {
			throw IOException("UpdateNamespacePropertiesResponse required property 'updated' is missing");
		}
		auto removed_val = yyjson_obj_get(obj, "removed");
		if (removed_val) {
			result.removed = parse_str_array(removed_val);
		} else {
			throw IOException("UpdateNamespacePropertiesResponse required property 'removed' is missing");
		}
		auto missing_val = yyjson_obj_get(obj, "missing");
		if (missing_val) {
			result.missing = parse_str_array(missing_val);
		}
		return result;
	}
public:
	UpdateNamespacePropertiesResponse() {}
public:
	vector<string> updated;
	vector<string> removed;
	vector<string> missing;
};

class CommitTableResponse {
public:
	static CommitTableResponse FromJSON(yyjson_val *obj) {
		CommitTableResponse result;
		auto metadata_location_val = yyjson_obj_get(obj, "metadata-location");
		if (metadata_location_val) {
			result.metadata_location = yyjson_get_str(metadata_location_val);
		} else {
			throw IOException("CommitTableResponse required property 'metadata-location' is missing");
		}
		auto metadata_val = yyjson_obj_get(obj, "metadata");
		if (metadata_val) {
			result.metadata = TableMetadata::FromJSON(metadata_val);
		} else {
			throw IOException("CommitTableResponse required property 'metadata' is missing");
		}
		return result;
	}
public:
	CommitTableResponse() {}
public:
	string metadata_location;
	TableMetadata metadata;
};

class StatisticsFile {
public:
	static StatisticsFile FromJSON(yyjson_val *obj) {
		StatisticsFile result;
		auto snapshot_id_val = yyjson_obj_get(obj, "snapshot-id");
		if (snapshot_id_val) {
			result.snapshot_id = yyjson_get_sint(snapshot_id_val);
		} else {
			throw IOException("StatisticsFile required property 'snapshot-id' is missing");
		}
		auto statistics_path_val = yyjson_obj_get(obj, "statistics-path");
		if (statistics_path_val) {
			result.statistics_path = yyjson_get_str(statistics_path_val);
		} else {
			throw IOException("StatisticsFile required property 'statistics-path' is missing");
		}
		auto file_size_in_bytes_val = yyjson_obj_get(obj, "file-size-in-bytes");
		if (file_size_in_bytes_val) {
			result.file_size_in_bytes = yyjson_get_sint(file_size_in_bytes_val);
		} else {
			throw IOException("StatisticsFile required property 'file-size-in-bytes' is missing");
		}
		auto file_footer_size_in_bytes_val = yyjson_obj_get(obj, "file-footer-size-in-bytes");
		if (file_footer_size_in_bytes_val) {
			result.file_footer_size_in_bytes = yyjson_get_sint(file_footer_size_in_bytes_val);
		} else {
			throw IOException("StatisticsFile required property 'file-footer-size-in-bytes' is missing");
		}
		auto blob_metadata_val = yyjson_obj_get(obj, "blob-metadata");
		if (blob_metadata_val) {
			result.blob_metadata = parse_obj_array<BlobMetadata>(blob_metadata_val);
		} else {
			throw IOException("StatisticsFile required property 'blob-metadata' is missing");
		}
		return result;
	}
public:
	StatisticsFile() {}
public:
	int64_t snapshot_id;
	string statistics_path;
	int64_t file_size_in_bytes;
	int64_t file_footer_size_in_bytes;
	vector<BlobMetadata> blob_metadata;
};

class BlobMetadata {
public:
	static BlobMetadata FromJSON(yyjson_val *obj) {
		BlobMetadata result;
		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = yyjson_get_str(type_val);
		} else {
			throw IOException("BlobMetadata required property 'type' is missing");
		}
		auto snapshot_id_val = yyjson_obj_get(obj, "snapshot-id");
		if (snapshot_id_val) {
			result.snapshot_id = yyjson_get_sint(snapshot_id_val);
		} else {
			throw IOException("BlobMetadata required property 'snapshot-id' is missing");
		}
		auto sequence_number_val = yyjson_obj_get(obj, "sequence-number");
		if (sequence_number_val) {
			result.sequence_number = yyjson_get_sint(sequence_number_val);
		} else {
			throw IOException("BlobMetadata required property 'sequence-number' is missing");
		}
		auto fields_val = yyjson_obj_get(obj, "fields");
		if (fields_val) {
			result.fields = parse_obj_array<int64_t>(fields_val);
		} else {
			throw IOException("BlobMetadata required property 'fields' is missing");
		}
		auto properties_val = yyjson_obj_get(obj, "properties");
		if (properties_val) {
			result.properties = ObjectOfStrings::FromJSON(properties_val);
		}
		return result;
	}
public:
	BlobMetadata() {}
public:
	string type;
	int64_t snapshot_id;
	int64_t sequence_number;
	vector<int64_t> fields;
	ObjectOfStrings properties;
};

class PartitionStatisticsFile {
public:
	static PartitionStatisticsFile FromJSON(yyjson_val *obj) {
		PartitionStatisticsFile result;
		auto snapshot_id_val = yyjson_obj_get(obj, "snapshot-id");
		if (snapshot_id_val) {
			result.snapshot_id = yyjson_get_sint(snapshot_id_val);
		} else {
			throw IOException("PartitionStatisticsFile required property 'snapshot-id' is missing");
		}
		auto statistics_path_val = yyjson_obj_get(obj, "statistics-path");
		if (statistics_path_val) {
			result.statistics_path = yyjson_get_str(statistics_path_val);
		} else {
			throw IOException("PartitionStatisticsFile required property 'statistics-path' is missing");
		}
		auto file_size_in_bytes_val = yyjson_obj_get(obj, "file-size-in-bytes");
		if (file_size_in_bytes_val) {
			result.file_size_in_bytes = yyjson_get_sint(file_size_in_bytes_val);
		} else {
			throw IOException("PartitionStatisticsFile required property 'file-size-in-bytes' is missing");
		}
		return result;
	}
public:
	PartitionStatisticsFile() {}
public:
	int64_t snapshot_id;
	string statistics_path;
	int64_t file_size_in_bytes;
};

class BooleanTypeValue {
public:
	static BooleanTypeValue FromJSON(yyjson_val *obj) {
		BooleanTypeValue result;
		return result;
	}
public:
	BooleanTypeValue() {}
public:
};

class IntegerTypeValue {
public:
	static IntegerTypeValue FromJSON(yyjson_val *obj) {
		IntegerTypeValue result;
		return result;
	}
public:
	IntegerTypeValue() {}
public:
};

class LongTypeValue {
public:
	static LongTypeValue FromJSON(yyjson_val *obj) {
		LongTypeValue result;
		return result;
	}
public:
	LongTypeValue() {}
public:
};

class FloatTypeValue {
public:
	static FloatTypeValue FromJSON(yyjson_val *obj) {
		FloatTypeValue result;
		return result;
	}
public:
	FloatTypeValue() {}
public:
};

class DoubleTypeValue {
public:
	static DoubleTypeValue FromJSON(yyjson_val *obj) {
		DoubleTypeValue result;
		return result;
	}
public:
	DoubleTypeValue() {}
public:
};

class DecimalTypeValue {
public:
	static DecimalTypeValue FromJSON(yyjson_val *obj) {
		DecimalTypeValue result;
		return result;
	}
public:
	DecimalTypeValue() {}
public:
};

class StringTypeValue {
public:
	static StringTypeValue FromJSON(yyjson_val *obj) {
		StringTypeValue result;
		return result;
	}
public:
	StringTypeValue() {}
public:
};

class UUIDTypeValue {
public:
	static UUIDTypeValue FromJSON(yyjson_val *obj) {
		UUIDTypeValue result;
		return result;
	}
public:
	UUIDTypeValue() {}
public:
};

class DateTypeValue {
public:
	static DateTypeValue FromJSON(yyjson_val *obj) {
		DateTypeValue result;
		return result;
	}
public:
	DateTypeValue() {}
public:
};

class TimeTypeValue {
public:
	static TimeTypeValue FromJSON(yyjson_val *obj) {
		TimeTypeValue result;
		return result;
	}
public:
	TimeTypeValue() {}
public:
};

class TimestampTypeValue {
public:
	static TimestampTypeValue FromJSON(yyjson_val *obj) {
		TimestampTypeValue result;
		return result;
	}
public:
	TimestampTypeValue() {}
public:
};

class TimestampTzTypeValue {
public:
	static TimestampTzTypeValue FromJSON(yyjson_val *obj) {
		TimestampTzTypeValue result;
		return result;
	}
public:
	TimestampTzTypeValue() {}
public:
};

class TimestampNanoTypeValue {
public:
	static TimestampNanoTypeValue FromJSON(yyjson_val *obj) {
		TimestampNanoTypeValue result;
		return result;
	}
public:
	TimestampNanoTypeValue() {}
public:
};

class TimestampTzNanoTypeValue {
public:
	static TimestampTzNanoTypeValue FromJSON(yyjson_val *obj) {
		TimestampTzNanoTypeValue result;
		return result;
	}
public:
	TimestampTzNanoTypeValue() {}
public:
};

class FixedTypeValue {
public:
	static FixedTypeValue FromJSON(yyjson_val *obj) {
		FixedTypeValue result;
		return result;
	}
public:
	FixedTypeValue() {}
public:
};

class BinaryTypeValue {
public:
	static BinaryTypeValue FromJSON(yyjson_val *obj) {
		BinaryTypeValue result;
		return result;
	}
public:
	BinaryTypeValue() {}
public:
};

class CountMap {
public:
	static CountMap FromJSON(yyjson_val *obj) {
		CountMap result;
		auto keys_val = yyjson_obj_get(obj, "keys");
		if (keys_val) {
			result.keys = parse_obj_array<IntegerTypeValue>(keys_val);
		}
		auto values_val = yyjson_obj_get(obj, "values");
		if (values_val) {
			result.values = parse_obj_array<LongTypeValue>(values_val);
		}
		return result;
	}
public:
	CountMap() {}
public:
	vector<IntegerTypeValue> keys;
	vector<LongTypeValue> values;
};

class ValueMap {
public:
	static ValueMap FromJSON(yyjson_val *obj) {
		ValueMap result;
		auto keys_val = yyjson_obj_get(obj, "keys");
		if (keys_val) {
			result.keys = parse_obj_array<IntegerTypeValue>(keys_val);
		}
		auto values_val = yyjson_obj_get(obj, "values");
		if (values_val) {
			result.values = parse_obj_array<PrimitiveTypeValue>(values_val);
		}
		return result;
	}
public:
	ValueMap() {}
public:
	vector<IntegerTypeValue> keys;
	vector<PrimitiveTypeValue> values;
};

class PrimitiveTypeValue {
public:
	static PrimitiveTypeValue FromJSON(yyjson_val *obj) {
		PrimitiveTypeValue result;
		return result;
	}
public:
	PrimitiveTypeValue() {}
public:
};

class FileFormat {
public:
	static FileFormat FromJSON(yyjson_val *obj) {
		FileFormat result;
		return result;
	}
public:
	FileFormat() {}
public:
};

class ContentFile {
public:
	static ContentFile FromJSON(yyjson_val *obj) {
		ContentFile result;
		auto content_val = yyjson_obj_get(obj, "content");
		if (content_val) {
			result.content = yyjson_get_str(content_val);
		} else {
			throw IOException("ContentFile required property 'content' is missing");
		}
		auto file_path_val = yyjson_obj_get(obj, "file-path");
		if (file_path_val) {
			result.file_path = yyjson_get_str(file_path_val);
		} else {
			throw IOException("ContentFile required property 'file-path' is missing");
		}
		auto file_format_val = yyjson_obj_get(obj, "file-format");
		if (file_format_val) {
			result.file_format = FileFormat::FromJSON(file_format_val);
		} else {
			throw IOException("ContentFile required property 'file-format' is missing");
		}
		auto spec_id_val = yyjson_obj_get(obj, "spec-id");
		if (spec_id_val) {
			result.spec_id = yyjson_get_sint(spec_id_val);
		} else {
			throw IOException("ContentFile required property 'spec-id' is missing");
		}
		auto partition_val = yyjson_obj_get(obj, "partition");
		if (partition_val) {
			result.partition = parse_obj_array<PrimitiveTypeValue>(partition_val);
		} else {
			throw IOException("ContentFile required property 'partition' is missing");
		}
		auto file_size_in_bytes_val = yyjson_obj_get(obj, "file-size-in-bytes");
		if (file_size_in_bytes_val) {
			result.file_size_in_bytes = yyjson_get_sint(file_size_in_bytes_val);
		} else {
			throw IOException("ContentFile required property 'file-size-in-bytes' is missing");
		}
		auto record_count_val = yyjson_obj_get(obj, "record-count");
		if (record_count_val) {
			result.record_count = yyjson_get_sint(record_count_val);
		} else {
			throw IOException("ContentFile required property 'record-count' is missing");
		}
		auto key_metadata_val = yyjson_obj_get(obj, "key-metadata");
		if (key_metadata_val) {
			result.key_metadata = BinaryTypeValue::FromJSON(key_metadata_val);
		}
		auto split_offsets_val = yyjson_obj_get(obj, "split-offsets");
		if (split_offsets_val) {
			result.split_offsets = parse_obj_array<int64_t>(split_offsets_val);
		}
		auto sort_order_id_val = yyjson_obj_get(obj, "sort-order-id");
		if (sort_order_id_val) {
			result.sort_order_id = yyjson_get_sint(sort_order_id_val);
		}
		return result;
	}
public:
	ContentFile() {}
public:
	string content;
	string file_path;
	FileFormat file_format;
	int64_t spec_id;
	vector<PrimitiveTypeValue> partition;
	int64_t file_size_in_bytes;
	int64_t record_count;
	BinaryTypeValue key_metadata;
	vector<int64_t> split_offsets;
	int64_t sort_order_id;
};

class DataFile {
public:
	static DataFile FromJSON(yyjson_val *obj) {
		DataFile result;
		auto content_val = yyjson_obj_get(obj, "content");
		if (content_val) {
			result.content = yyjson_get_str(content_val);
		} else {
			throw IOException("DataFile required property 'content' is missing");
		}
		auto column_sizes_val = yyjson_obj_get(obj, "column-sizes");
		if (column_sizes_val) {
			result.column_sizes = CountMap::FromJSON(column_sizes_val);
		}
		auto value_counts_val = yyjson_obj_get(obj, "value-counts");
		if (value_counts_val) {
			result.value_counts = CountMap::FromJSON(value_counts_val);
		}
		auto null_value_counts_val = yyjson_obj_get(obj, "null-value-counts");
		if (null_value_counts_val) {
			result.null_value_counts = CountMap::FromJSON(null_value_counts_val);
		}
		auto nan_value_counts_val = yyjson_obj_get(obj, "nan-value-counts");
		if (nan_value_counts_val) {
			result.nan_value_counts = CountMap::FromJSON(nan_value_counts_val);
		}
		auto lower_bounds_val = yyjson_obj_get(obj, "lower-bounds");
		if (lower_bounds_val) {
			result.lower_bounds = ValueMap::FromJSON(lower_bounds_val);
		}
		auto upper_bounds_val = yyjson_obj_get(obj, "upper-bounds");
		if (upper_bounds_val) {
			result.upper_bounds = ValueMap::FromJSON(upper_bounds_val);
		}
		return result;
	}
public:
	DataFile() {}
public:
	string content;
	CountMap column_sizes;
	CountMap value_counts;
	CountMap null_value_counts;
	CountMap nan_value_counts;
	ValueMap lower_bounds;
	ValueMap upper_bounds;
};

class DeleteFile {
public:
	static DeleteFile FromJSON(yyjson_val *obj) {
		DeleteFile result;
		return result;
	}
public:
	DeleteFile() {}
public:
};

class PositionDeleteFile {
public:
	static PositionDeleteFile FromJSON(yyjson_val *obj) {
		PositionDeleteFile result;
		auto content_val = yyjson_obj_get(obj, "content");
		if (content_val) {
			result.content = yyjson_get_str(content_val);
		} else {
			throw IOException("PositionDeleteFile required property 'content' is missing");
		}
		auto content_offset_val = yyjson_obj_get(obj, "content-offset");
		if (content_offset_val) {
			result.content_offset = yyjson_get_sint(content_offset_val);
		}
		auto content_size_in_bytes_val = yyjson_obj_get(obj, "content-size-in-bytes");
		if (content_size_in_bytes_val) {
			result.content_size_in_bytes = yyjson_get_sint(content_size_in_bytes_val);
		}
		return result;
	}
public:
	PositionDeleteFile() {}
public:
	string content;
	int64_t content_offset;
	int64_t content_size_in_bytes;
};

class EqualityDeleteFile {
public:
	static EqualityDeleteFile FromJSON(yyjson_val *obj) {
		EqualityDeleteFile result;
		auto content_val = yyjson_obj_get(obj, "content");
		if (content_val) {
			result.content = yyjson_get_str(content_val);
		} else {
			throw IOException("EqualityDeleteFile required property 'content' is missing");
		}
		auto equality_ids_val = yyjson_obj_get(obj, "equality-ids");
		if (equality_ids_val) {
			result.equality_ids = parse_obj_array<int64_t>(equality_ids_val);
		}
		return result;
	}
public:
	EqualityDeleteFile() {}
public:
	string content;
	vector<int64_t> equality_ids;
};

class PlanTableScanRequest {
public:
	static PlanTableScanRequest FromJSON(yyjson_val *obj) {
		PlanTableScanRequest result;
		auto snapshot_id_val = yyjson_obj_get(obj, "snapshot-id");
		if (snapshot_id_val) {
			result.snapshot_id = yyjson_get_sint(snapshot_id_val);
		}
		auto select_val = yyjson_obj_get(obj, "select");
		if (select_val) {
			result.select = parse_obj_array<FieldName>(select_val);
		}
		auto filter_val = yyjson_obj_get(obj, "filter");
		if (filter_val) {
			result.filter = Expression::FromJSON(filter_val);
		}
		auto case_sensitive_val = yyjson_obj_get(obj, "case-sensitive");
		if (case_sensitive_val) {
			result.case_sensitive = yyjson_get_bool(case_sensitive_val);
		}
		auto use_snapshot_schema_val = yyjson_obj_get(obj, "use-snapshot-schema");
		if (use_snapshot_schema_val) {
			result.use_snapshot_schema = yyjson_get_bool(use_snapshot_schema_val);
		}
		auto start_snapshot_id_val = yyjson_obj_get(obj, "start-snapshot-id");
		if (start_snapshot_id_val) {
			result.start_snapshot_id = yyjson_get_sint(start_snapshot_id_val);
		}
		auto end_snapshot_id_val = yyjson_obj_get(obj, "end-snapshot-id");
		if (end_snapshot_id_val) {
			result.end_snapshot_id = yyjson_get_sint(end_snapshot_id_val);
		}
		auto stats_fields_val = yyjson_obj_get(obj, "stats-fields");
		if (stats_fields_val) {
			result.stats_fields = parse_obj_array<FieldName>(stats_fields_val);
		}
		return result;
	}
public:
	PlanTableScanRequest() {}
public:
	int64_t snapshot_id;
	vector<FieldName> select;
	Expression filter;
	bool case_sensitive;
	bool use_snapshot_schema;
	int64_t start_snapshot_id;
	int64_t end_snapshot_id;
	vector<FieldName> stats_fields;
};

class FieldName {
public:
	static FieldName FromJSON(yyjson_val *obj) {
		FieldName result;
		return result;
	}
public:
	FieldName() {}
public:
};

class PlanTask {
public:
	static PlanTask FromJSON(yyjson_val *obj) {
		PlanTask result;
		return result;
	}
public:
	PlanTask() {}
public:
};

class FileScanTask {
public:
	static FileScanTask FromJSON(yyjson_val *obj) {
		FileScanTask result;
		auto data_file_val = yyjson_obj_get(obj, "data-file");
		if (data_file_val) {
			result.data_file = DataFile::FromJSON(data_file_val);
		} else {
			throw IOException("FileScanTask required property 'data-file' is missing");
		}
		auto delete_file_references_val = yyjson_obj_get(obj, "delete-file-references");
		if (delete_file_references_val) {
			result.delete_file_references = parse_obj_array<int64_t>(delete_file_references_val);
		}
		auto residual_filter_val = yyjson_obj_get(obj, "residual-filter");
		if (residual_filter_val) {
			result.residual_filter = Expression::FromJSON(residual_filter_val);
		}
		return result;
	}
public:
	FileScanTask() {}
public:
	DataFile data_file;
	vector<int64_t> delete_file_references;
	Expression residual_filter;
};

class CreateNamespaceRequest {
public:
	static CreateNamespaceRequest FromJSON(yyjson_val *obj) {
		CreateNamespaceRequest result;
		auto _namespace_val = yyjson_obj_get(obj, "namespace");
		if (_namespace_val) {
			result._namespace = Namespace::FromJSON(_namespace_val);
		} else {
			throw IOException("CreateNamespaceRequest required property 'namespace' is missing");
		}
		auto properties_val = yyjson_obj_get(obj, "properties");
		if (properties_val) {
			result.properties = ObjectOfStrings::FromJSON(properties_val);
		}
		return result;
	}
public:
	CreateNamespaceRequest() {}
public:
	Namespace _namespace;
	ObjectOfStrings properties;
};

class RenameTableRequest {
public:
	static RenameTableRequest FromJSON(yyjson_val *obj) {
		RenameTableRequest result;
		auto source_val = yyjson_obj_get(obj, "source");
		if (source_val) {
			result.source = TableIdentifier::FromJSON(source_val);
		} else {
			throw IOException("RenameTableRequest required property 'source' is missing");
		}
		auto destination_val = yyjson_obj_get(obj, "destination");
		if (destination_val) {
			result.destination = TableIdentifier::FromJSON(destination_val);
		} else {
			throw IOException("RenameTableRequest required property 'destination' is missing");
		}
		return result;
	}
public:
	RenameTableRequest() {}
public:
	TableIdentifier source;
	TableIdentifier destination;
};

class StructField {
public:
	static StructField FromJSON(yyjson_val *obj) {
		StructField result;
		auto id_val = yyjson_obj_get(obj, "id");
		if (id_val) {
			result.id = yyjson_get_sint(id_val);
		} else {
			throw IOException("StructField required property 'id' is missing");
		}
		auto name_val = yyjson_obj_get(obj, "name");
		if (name_val) {
			result.name = yyjson_get_str(name_val);
		} else {
			throw IOException("StructField required property 'name' is missing");
		}
		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = Type::FromJSON(type_val);
		} else {
			throw IOException("StructField required property 'type' is missing");
		}
		auto required_val = yyjson_obj_get(obj, "required");
		if (required_val) {
			result.required = yyjson_get_bool(required_val);
		} else {
			throw IOException("StructField required property 'required' is missing");
		}
		auto doc_val = yyjson_obj_get(obj, "doc");
		if (doc_val) {
			result.doc = yyjson_get_str(doc_val);
		}
		auto initial_default_val = yyjson_obj_get(obj, "initial-default");
		if (initial_default_val) {
			result.initial_default = PrimitiveTypeValue::FromJSON(initial_default_val);
		}
		auto write_default_val = yyjson_obj_get(obj, "write-default");
		if (write_default_val) {
			result.write_default = PrimitiveTypeValue::FromJSON(write_default_val);
		}
		return result;
	}
public:
	StructField() {}
public:
	int64_t id;
	string name;
	Type type;
	bool required;
	string doc;
	PrimitiveTypeValue initial_default;
	PrimitiveTypeValue write_default;
};

class ListType {
public:
	static ListType FromJSON(yyjson_val *obj) {
		ListType result;
		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = yyjson_get_str(type_val);
		} else {
			throw IOException("ListType required property 'type' is missing");
		}
		auto element_id_val = yyjson_obj_get(obj, "element-id");
		if (element_id_val) {
			result.element_id = yyjson_get_sint(element_id_val);
		} else {
			throw IOException("ListType required property 'element-id' is missing");
		}
		auto element_val = yyjson_obj_get(obj, "element");
		if (element_val) {
			result.element = Type::FromJSON(element_val);
		} else {
			throw IOException("ListType required property 'element' is missing");
		}
		auto element_required_val = yyjson_obj_get(obj, "element-required");
		if (element_required_val) {
			result.element_required = yyjson_get_bool(element_required_val);
		} else {
			throw IOException("ListType required property 'element-required' is missing");
		}
		return result;
	}
public:
	ListType() {}
public:
	string type;
	int64_t element_id;
	Type element;
	bool element_required;
};

class MapType {
public:
	static MapType FromJSON(yyjson_val *obj) {
		MapType result;
		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = yyjson_get_str(type_val);
		} else {
			throw IOException("MapType required property 'type' is missing");
		}
		auto key_id_val = yyjson_obj_get(obj, "key-id");
		if (key_id_val) {
			result.key_id = yyjson_get_sint(key_id_val);
		} else {
			throw IOException("MapType required property 'key-id' is missing");
		}
		auto key_val = yyjson_obj_get(obj, "key");
		if (key_val) {
			result.key = Type::FromJSON(key_val);
		} else {
			throw IOException("MapType required property 'key' is missing");
		}
		auto value_id_val = yyjson_obj_get(obj, "value-id");
		if (value_id_val) {
			result.value_id = yyjson_get_sint(value_id_val);
		} else {
			throw IOException("MapType required property 'value-id' is missing");
		}
		auto value_val = yyjson_obj_get(obj, "value");
		if (value_val) {
			result.value = Type::FromJSON(value_val);
		} else {
			throw IOException("MapType required property 'value' is missing");
		}
		auto value_required_val = yyjson_obj_get(obj, "value-required");
		if (value_required_val) {
			result.value_required = yyjson_get_bool(value_required_val);
		} else {
			throw IOException("MapType required property 'value-required' is missing");
		}
		return result;
	}
public:
	MapType() {}
public:
	string type;
	int64_t key_id;
	Type key;
	int64_t value_id;
	Type value;
	bool value_required;
};

class UnaryExpression {
public:
	static UnaryExpression FromJSON(yyjson_val *obj) {
		UnaryExpression result;
		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = ExpressionType::FromJSON(type_val);
		} else {
			throw IOException("UnaryExpression required property 'type' is missing");
		}
		auto term_val = yyjson_obj_get(obj, "term");
		if (term_val) {
			result.term = Term::FromJSON(term_val);
		} else {
			throw IOException("UnaryExpression required property 'term' is missing");
		}
		auto value_val = yyjson_obj_get(obj, "value");
		if (value_val) {
			result.value = ObjectOfStrings::FromJSON(value_val);
		} else {
			throw IOException("UnaryExpression required property 'value' is missing");
		}
		return result;
	}
public:
	UnaryExpression() {}
public:
	ExpressionType type;
	Term term;
	ObjectOfStrings value;
};

class LiteralExpression {
public:
	static LiteralExpression FromJSON(yyjson_val *obj) {
		LiteralExpression result;
		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = ExpressionType::FromJSON(type_val);
		} else {
			throw IOException("LiteralExpression required property 'type' is missing");
		}
		auto term_val = yyjson_obj_get(obj, "term");
		if (term_val) {
			result.term = Term::FromJSON(term_val);
		} else {
			throw IOException("LiteralExpression required property 'term' is missing");
		}
		auto value_val = yyjson_obj_get(obj, "value");
		if (value_val) {
			result.value = ObjectOfStrings::FromJSON(value_val);
		} else {
			throw IOException("LiteralExpression required property 'value' is missing");
		}
		return result;
	}
public:
	LiteralExpression() {}
public:
	ExpressionType type;
	Term term;
	ObjectOfStrings value;
};

class SetExpression {
public:
	static SetExpression FromJSON(yyjson_val *obj) {
		SetExpression result;
		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = ExpressionType::FromJSON(type_val);
		} else {
			throw IOException("SetExpression required property 'type' is missing");
		}
		auto term_val = yyjson_obj_get(obj, "term");
		if (term_val) {
			result.term = Term::FromJSON(term_val);
		} else {
			throw IOException("SetExpression required property 'term' is missing");
		}
		auto values_val = yyjson_obj_get(obj, "values");
		if (values_val) {
			result.values = parse_obj_array<object>(values_val);
		} else {
			throw IOException("SetExpression required property 'values' is missing");
		}
		return result;
	}
public:
	SetExpression() {}
public:
	ExpressionType type;
	Term term;
	vector<ObjectOfStrings> values;
};

class TransformTerm {
public:
	static TransformTerm FromJSON(yyjson_val *obj) {
		TransformTerm result;
		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = yyjson_get_str(type_val);
		} else {
			throw IOException("TransformTerm required property 'type' is missing");
		}
		auto transform_val = yyjson_obj_get(obj, "transform");
		if (transform_val) {
			result.transform = Transform::FromJSON(transform_val);
		} else {
			throw IOException("TransformTerm required property 'transform' is missing");
		}
		auto term_val = yyjson_obj_get(obj, "term");
		if (term_val) {
			result.term = Reference::FromJSON(term_val);
		} else {
			throw IOException("TransformTerm required property 'term' is missing");
		}
		return result;
	}
public:
	TransformTerm() {}
public:
	string type;
	Transform transform;
	Reference term;
};

class SetStatisticsUpdate {
public:
	static SetStatisticsUpdate FromJSON(yyjson_val *obj) {
		SetStatisticsUpdate result;
		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			result.action = yyjson_get_str(action_val);
		}
		auto snapshot_id_val = yyjson_obj_get(obj, "snapshot-id");
		if (snapshot_id_val) {
			result.snapshot_id = yyjson_get_sint(snapshot_id_val);
		}
		auto statistics_val = yyjson_obj_get(obj, "statistics");
		if (statistics_val) {
			result.statistics = StatisticsFile::FromJSON(statistics_val);
		} else {
			throw IOException("SetStatisticsUpdate required property 'statistics' is missing");
		}
		return result;
	}
public:
	SetStatisticsUpdate() {}
public:
	string action;
	int64_t snapshot_id;
	StatisticsFile statistics;
};

class SetPartitionStatisticsUpdate {
public:
	static SetPartitionStatisticsUpdate FromJSON(yyjson_val *obj) {
		SetPartitionStatisticsUpdate result;
		auto action_val = yyjson_obj_get(obj, "action");
		if (action_val) {
			result.action = yyjson_get_str(action_val);
		}
		auto partition_statistics_val = yyjson_obj_get(obj, "partition-statistics");
		if (partition_statistics_val) {
			result.partition_statistics = PartitionStatisticsFile::FromJSON(partition_statistics_val);
		} else {
			throw IOException("SetPartitionStatisticsUpdate required property 'partition-statistics' is missing");
		}
		return result;
	}
public:
	SetPartitionStatisticsUpdate() {}
public:
	string action;
	PartitionStatisticsFile partition_statistics;
};

class CompletedPlanningResult {
public:
	static CompletedPlanningResult FromJSON(yyjson_val *obj) {
		CompletedPlanningResult result;
		auto status_val = yyjson_obj_get(obj, "status");
		if (status_val) {
			result.status = PlanStatus::FromJSON(status_val);
		} else {
			throw IOException("CompletedPlanningResult required property 'status' is missing");
		}
		return result;
	}
public:
	CompletedPlanningResult() {}
public:
	PlanStatus status;
};

class FailedPlanningResult {
public:
	static FailedPlanningResult FromJSON(yyjson_val *obj) {
		FailedPlanningResult result;
		auto status_val = yyjson_obj_get(obj, "status");
		if (status_val) {
			result.status = PlanStatus::FromJSON(status_val);
		} else {
			throw IOException("FailedPlanningResult required property 'status' is missing");
		}
		return result;
	}
public:
	FailedPlanningResult() {}
public:
	PlanStatus status;
};

class AsyncPlanningResult {
public:
	static AsyncPlanningResult FromJSON(yyjson_val *obj) {
		AsyncPlanningResult result;
		auto status_val = yyjson_obj_get(obj, "status");
		if (status_val) {
			result.status = PlanStatus::FromJSON(status_val);
		} else {
			throw IOException("AsyncPlanningResult required property 'status' is missing");
		}
		auto plan_id_val = yyjson_obj_get(obj, "plan-id");
		if (plan_id_val) {
			result.plan_id = yyjson_get_str(plan_id_val);
		}
		return result;
	}
public:
	AsyncPlanningResult() {}
public:
	PlanStatus status;
	string plan_id;
};

class EmptyPlanningResult {
public:
	static EmptyPlanningResult FromJSON(yyjson_val *obj) {
		EmptyPlanningResult result;
		auto status_val = yyjson_obj_get(obj, "status");
		if (status_val) {
			result.status = PlanStatus::FromJSON(status_val);
		} else {
			throw IOException("EmptyPlanningResult required property 'status' is missing");
		}
		return result;
	}
public:
	EmptyPlanningResult() {}
public:
	PlanStatus status;
};

class FetchScanTasksRequest {
public:
	static FetchScanTasksRequest FromJSON(yyjson_val *obj) {
		FetchScanTasksRequest result;
		auto plan_task_val = yyjson_obj_get(obj, "plan-task");
		if (plan_task_val) {
			result.plan_task = PlanTask::FromJSON(plan_task_val);
		} else {
			throw IOException("FetchScanTasksRequest required property 'plan-task' is missing");
		}
		return result;
	}
public:
	FetchScanTasksRequest() {}
public:
	PlanTask plan_task;
};


} // namespace rest_api_objects

} // namespace duckdb
