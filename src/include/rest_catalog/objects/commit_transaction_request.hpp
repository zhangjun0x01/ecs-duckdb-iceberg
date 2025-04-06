
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
	CommitTransactionRequest() {
	}

public:
	static CommitTransactionRequest FromJSON(yyjson_val *obj) {
		CommitTransactionRequest res;
		auto error = res.TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return res;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;
		auto table_changes_val = yyjson_obj_get(obj, "table_changes");
		if (!table_changes_val) {
			return "CommitTransactionRequest required property 'table_changes' is missing";
		} else {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(table_changes_val, idx, max, val) {
				CommitTableRequest tmp;
				error = tmp.TryFromJSON(val);
				if (!error.empty()) {
					return error;
				}
				table_changes.push_back(tmp);
			}
		}
		return string();
	}

public:
	vector<CommitTableRequest> table_changes;
};

} // namespace rest_api_objects
} // namespace duckdb
