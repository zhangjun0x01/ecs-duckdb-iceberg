#pragma once
#include "duckdb/common/types/value.hpp"

namespace duckdb {

struct IcebergPredicateStats {
public:
	IcebergPredicateStats() {
	}

public:
	Value lower_bound;
	Value upper_bound;
	bool has_null = false;
	bool has_nan = false;
};

} // namespace duckdb
