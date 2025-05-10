#include "iceberg_predicate.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"

namespace duckdb {

template <class TRANSFORM>
bool MatchBoundsTemplated(TableFilter &filter, const Value &lower_bound, const Value &upper_bound,
                          const IcebergTransform &transform);

template <class TRANSFORM>
static bool MatchBoundsConstantFilter(ConstantFilter &constant_filter, const Value &lower_bound,
                                      const Value &upper_bound, const IcebergTransform &transform) {
	auto constant_value = TRANSFORM::ApplyTransform(constant_filter.constant, transform);

	switch (constant_filter.comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
		return TRANSFORM::CompareEqual(constant_value, lower_bound, upper_bound);
	case ExpressionType::COMPARE_GREATERTHAN:
		return TRANSFORM::CompareGreaterThan(constant_value, lower_bound, upper_bound);
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return TRANSFORM::CompareGreaterThanOrEqual(constant_value, lower_bound, upper_bound);
	case ExpressionType::COMPARE_LESSTHAN:
		return TRANSFORM::CompareLessThan(constant_value, lower_bound, upper_bound);
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return TRANSFORM::CompareLessThanOrEqual(constant_value, lower_bound, upper_bound);
	default:
		//! Conservative approach: we don't know, so we just say it's not filtered out
		return true;
	}
}

template <class TRANSFORM>
static bool MatchBoundsConjunctionAndFilter(ConjunctionAndFilter &conjunction_and, const Value &lower_bound,
                                            const Value &upper_bound, const IcebergTransform &transform) {
	for (auto &child : conjunction_and.child_filters) {
		if (!MatchBoundsTemplated<TRANSFORM>(*child, lower_bound, upper_bound, transform)) {
			return false;
		}
	}
	return true;
}

template <class TRANSFORM>
bool MatchBoundsTemplated(TableFilter &filter, const Value &lower_bound, const Value &upper_bound,
                          const IcebergTransform &transform) {
	//! TODO: support more filter types
	switch (filter.filter_type) {
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &constant_filter = filter.Cast<ConstantFilter>();
		return MatchBoundsConstantFilter<TRANSFORM>(constant_filter, lower_bound, upper_bound, transform);
	}
	case TableFilterType::CONJUNCTION_AND: {
		auto &conjunction_and_filter = filter.Cast<ConjunctionAndFilter>();
		return MatchBoundsConjunctionAndFilter<TRANSFORM>(conjunction_and_filter, lower_bound, upper_bound, transform);
	}
	default:
		//! Conservative approach: we don't know what this is, just say it doesn't filter anything
		return true;
	}
}

bool IcebergPredicate::MatchBounds(TableFilter &filter, const Value &lower_bound, const Value &upper_bound,
                                   const IcebergTransform &transform) {
	switch (transform.Type()) {
	case IcebergTransformType::IDENTITY:
		return MatchBoundsTemplated<IdentityTransform>(filter, lower_bound, upper_bound, transform);
	case IcebergTransformType::BUCKET:
		throw NotImplementedException("Transform '%s' not implemented", transform.RawType());
	case IcebergTransformType::TRUNCATE:
		throw NotImplementedException("Transform '%s' not implemented", transform.RawType());
	case IcebergTransformType::YEAR:
		throw NotImplementedException("Transform '%s' not implemented", transform.RawType());
	case IcebergTransformType::MONTH:
		throw NotImplementedException("Transform '%s' not implemented", transform.RawType());
	case IcebergTransformType::DAY:
		// return MatchBoundsTemplated<DayTransform>(filter, lower_bound, upper_bound, transform);
		throw NotImplementedException("Transform '%s' not implemented", transform.RawType());
	case IcebergTransformType::HOUR:
		throw NotImplementedException("Transform '%s' not implemented", transform.RawType());
	case IcebergTransformType::VOID:
		throw NotImplementedException("Transform '%s' not implemented", transform.RawType());
	default:
		throw InternalException("Transform '%s' not implemented", transform.RawType());
	}
}

} // namespace duckdb
