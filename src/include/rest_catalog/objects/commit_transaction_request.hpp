
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/commit_table_request.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class CommitTransactionRequest {
public:
	CommitTransactionRequest::CommitTransactionRequest() {
	}

public:
	static CommitTransactionRequest FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		auto table_changes_val = yyjson_obj_get(obj, "table_changes");
		if (!table_changes_val) {
		return "CommitTransactionRequest required property 'table_changes' is missing");
		}
		size_t idx, max;
		yyjson_val *val;
		yyjson_arr_foreach(table_changes_val, idx, max, val) {
			result.table_changes.push_back(CommitTableRequest::FromJSON(val));
		}

		return string();
	}

public:
public:
	vector<CommitTableRequest> table_changes;
};

} // namespace rest_api_objects
} // namespace duckdb
