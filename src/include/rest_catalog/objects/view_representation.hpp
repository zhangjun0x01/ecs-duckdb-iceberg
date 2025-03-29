#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class ViewRepresentation {
public:
	static ViewRepresentation FromJSON(yyjson_val *obj) {
		ViewRepresentation result;
		result.sqlview_representation = SQLViewRepresentation::FromJSON(obj);
		result.has_sqlview_representation = true;
		return result;
	}

public:
	SQLViewRepresentation sqlview_representation;
	bool has_sqlview_representation = false;
};
} // namespace rest_api_objects
} // namespace duckdb
