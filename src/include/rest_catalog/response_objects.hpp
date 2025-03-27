#pragma once

#include "yyjson.hpp"
#include <string>
#include <vector>
#include <unordered_map>

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

using ObjectOfStrings = unordered_map<string, string>;

inline ObjectOfStrings parse_object_of_strings(yyjson_val *obj) {
    ObjectOfStrings result;
    size_t idx, max;
    yyjson_val *key, *val;
    yyjson_obj_foreach(obj, idx, max, key, val) {
        auto key_str = yyjson_get_str(key);
        auto val_str = yyjson_get_str(val);
        result[key_str] = val_str;
    }
    return result;
}

} // namespace rest_api_objects
} // namespace duckdb
