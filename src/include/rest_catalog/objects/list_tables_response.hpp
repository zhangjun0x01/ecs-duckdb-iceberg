
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/page_token.hpp"
#include "rest_catalog/objects/table_identifier.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class ListTablesResponse {
public:
	ListTablesResponse::ListTablesResponse() {
	}

public:
	static ListTablesResponse FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;

		auto next_page_token_val = yyjson_obj_get(obj, "next_page_token");
		if (next_page_token_val) {
			error = page_token.TryFromJSON(next_page_token_val);
			if (!error.empty()) {
				return error;
			}
		}

		auto identifiers_val = yyjson_obj_get(obj, "identifiers");
		if (identifiers_val) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(identifiers_val, idx, max, val) {

				TableIdentifier tmp;
				error = tmp.TryFromJSON(val);
				if (!error.empty()) {
					return error;
				}
				identifiers.push_back(tmp);
			}
		}
		return string();
	}

public:
public:
	vector<TableIdentifier> identifiers;
	PageToken next_page_token;
};

} // namespace rest_api_objects
} // namespace duckdb
