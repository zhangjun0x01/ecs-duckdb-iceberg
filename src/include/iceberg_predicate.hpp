#pragma once
#include "iceberg_transform.hpp"
#include "iceberg_predicate_stats.hpp"
#include "duckdb/planner/table_filter.hpp"

namespace duckdb {

struct IcebergPredicate {
public:
	IcebergPredicate() = delete;

public:
	static bool MatchBounds(TableFilter &filter, const IcebergPredicateStats &stats, const IcebergTransform &transform);
};

} // namespace duckdb
