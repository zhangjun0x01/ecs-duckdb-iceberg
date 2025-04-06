
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/field_name.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class Expression;

class PlanTableScanRequest {
public:
	PlanTableScanRequest();

public:
	static PlanTableScanRequest FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

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
