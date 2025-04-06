
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class Snapshot {
public:
	Snapshot();
	class Object2 {
	public:
		Object2();

	public:
		static Object2 FromJSON(yyjson_val *obj);

	public:
		string TryFromJSON(yyjson_val *obj);

	public:
		string operation;
		case_insensitive_map_t<string> additional_properties;
	};

public:
	static Snapshot FromJSON(yyjson_val *obj);

public:
	string TryFromJSON(yyjson_val *obj);

public:
	int64_t snapshot_id;
	int64_t timestamp_ms;
	string manifest_list;
	Object2 summary;
	int64_t parent_snapshot_id;
	int64_t sequence_number;
	int64_t schema_id;
};

} // namespace rest_api_objects
} // namespace duckdb
