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
};

} // namespace duckdb
