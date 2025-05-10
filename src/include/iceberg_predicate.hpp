#pragma once
#include "iceberg_transform.hpp"
#include "duckdb/planner/table_filter.hpp"

namespace duckdb {

struct IcebergPredicate {
public:
	IcebergPredicate() = delete;

public:
	static bool MatchBounds(TableFilter &filter, const Value &lower_bound, const Value &upper_bound,
	                        const IcebergTransform &transform);
};

} // namespace duckdb
