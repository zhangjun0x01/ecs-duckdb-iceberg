#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class ErrorModel {
public:
	static ErrorModel FromJSON(yyjson_val *obj) {
		ErrorModel result;

		auto code_val = yyjson_obj_get(obj, "code");
		if (code_val) {
			result.code = yyjson_get_sint(code_val);
		}
		else {
			throw IOException("ErrorModel required property 'code' is missing");
		}

		auto message_val = yyjson_obj_get(obj, "message");
		if (message_val) {
			result.message = yyjson_get_str(message_val);
		}
		else {
			throw IOException("ErrorModel required property 'message' is missing");
		}

		auto stack_val = yyjson_obj_get(obj, "stack");
		if (stack_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(stack_val, idx, max, val) {
				result.stack.push_back(yyjson_get_str(val));
			}
		}

		auto type_val = yyjson_obj_get(obj, "type");
		if (type_val) {
			result.type = yyjson_get_str(type_val);
		}
		else {
			throw IOException("ErrorModel required property 'type' is missing");
		}

		return result;
	}

public:
	int64_t code;
	string message;
	vector<string> stack;
	string type;
};
} // namespace rest_api_objects
} // namespace duckdb