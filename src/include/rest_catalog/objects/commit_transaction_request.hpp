#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/commit_table_request.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class CommitTransactionRequest {
public:
	static CommitTransactionRequest FromJSON(yyjson_val *obj) {
		CommitTransactionRequest result;

		auto table_changes_val = yyjson_obj_get(obj, "table-changes");
		if (table_changes_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(table_changes_val, idx, max, val) {
				result.table_changes.push_back(CommitTableRequest::FromJSON(val));
			}
		}
		else {
			throw IOException("CommitTransactionRequest required property 'table-changes' is missing");
		}

		return result;
	}

public:
	vector<CommitTableRequest> table_changes;
};
} // namespace rest_api_objects
} // namespace duckdb