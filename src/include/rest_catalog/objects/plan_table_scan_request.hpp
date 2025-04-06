
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/expression.hpp"
#include "rest_catalog/objects/field_name.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class PlanTableScanRequest {
public:
	PlanTableScanRequest() {
	}

public:
	static PlanTableScanRequest FromJSON(yyjson_val *obj) {
		PlanTableScanRequest res;
		auto error = res.TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return res;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;
		auto snapshot_id_val = yyjson_obj_get(obj, "snapshot_id");
		if (snapshot_id_val) {
			snapshot_id = yyjson_get_sint(snapshot_id_val);
		}
		auto select_val = yyjson_obj_get(obj, "select");
		if (select_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(select_val, idx, max, val) {
				FieldName tmp;
				error = tmp.TryFromJSON(val);
				if (!error.empty()) {
					return error;
				}
				select.push_back(tmp);
			}
		}
		auto filter_val = yyjson_obj_get(obj, "filter");
		if (filter_val) {
			filter = make_uniq<Expression>();
			error = filter->TryFromJSON(filter_val);
			if (!error.empty()) {
				return error;
			}
		}
		auto case_sensitive_val = yyjson_obj_get(obj, "case_sensitive");
		if (case_sensitive_val) {
			case_sensitive = yyjson_get_bool(case_sensitive_val);
		}
		auto use_snapshot_schema_val = yyjson_obj_get(obj, "use_snapshot_schema");
		if (use_snapshot_schema_val) {
			use_snapshot_schema = yyjson_get_bool(use_snapshot_schema_val);
		}
		auto start_snapshot_id_val = yyjson_obj_get(obj, "start_snapshot_id");
		if (start_snapshot_id_val) {
			start_snapshot_id = yyjson_get_sint(start_snapshot_id_val);
		}
		auto end_snapshot_id_val = yyjson_obj_get(obj, "end_snapshot_id");
		if (end_snapshot_id_val) {
			end_snapshot_id = yyjson_get_sint(end_snapshot_id_val);
		}
		auto stats_fields_val = yyjson_obj_get(obj, "stats_fields");
		if (stats_fields_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(stats_fields_val, idx, max, val) {
				FieldName tmp;
				error = tmp.TryFromJSON(val);
				if (!error.empty()) {
					return error;
				}
				stats_fields.push_back(tmp);
			}
		}
		return string();
	}

public:
	int64_t snapshot_id;
	vector<FieldName> select;
	unique_ptr<Expression> filter;
	bool case_sensitive;
	bool use_snapshot_schema;
	int64_t start_snapshot_id;
	int64_t end_snapshot_id;
	vector<FieldName> stats_fields;
};

} // namespace rest_api_objects
} // namespace duckdb
