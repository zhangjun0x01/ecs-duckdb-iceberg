
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
#include "rest_catalog/objects/reference.hpp"
#include "rest_catalog/objects/transform_term.hpp"

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

class Term {
public:
	Term::Term() {
	}

public:
	static Term FromJSON(yyjson_val *obj) {
		auto error = TryFromJSON(obj);
		if (!error.empty()) {
			throw InvalidInputException(error);
		}
		return *this;
	}

public:
	string TryFromJSON(yyjson_val *obj) {
		string error;
		do {
			error = reference.TryFromJSON(obj);
			if (error.empty()) {
				has_reference = true;
				break;
			}
			error = transform_term.TryFromJSON(obj);
			if (error.empty()) {
				has_transform_term = true;
				break;
			}
			return "Term failed to parse, none of the oneOf candidates matched";
		} while (false);

		return string();
	}

public:
	TransformTerm transform_term;
	Reference reference;

public:
	bool has_reference = false;
	bool has_transform_term = false;
};

} // namespace rest_api_objects
} // namespace duckdb
