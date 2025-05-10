#pragma once

#include "iceberg_predicate_stats.hpp"

namespace duckdb {

enum class IcebergTransformType : uint8_t { IDENTITY, BUCKET, TRUNCATE, YEAR, MONTH, DAY, HOUR, VOID, INVALID };

struct IcebergTransform {
public:
	IcebergTransform();
	IcebergTransform(const string &transform);

public:
	operator IcebergTransformType() const {
		return Type();
	}

public:
	//! Singleton iceberg transform, used as stand-in when there is no partition field
	static const IcebergTransform &Identity() {
		static auto identity = IcebergTransform("identity");
		return identity;
	}
	idx_t GetBucketModulo() const {
		D_ASSERT(type == IcebergTransformType::BUCKET);
		return modulo;
	}
	idx_t GetTruncateWidth() const {
		D_ASSERT(type == IcebergTransformType::TRUNCATE);
		return width;
	}
	IcebergTransformType Type() const {
		D_ASSERT(type != IcebergTransformType::INVALID);
		return type;
	}
	const string &RawType() const {
		return raw_transform;
	}

private:
	//! Preserve the input for debugging
	string raw_transform;
	IcebergTransformType type;
	idx_t modulo;
	idx_t width;
};

struct IdentityTransform {
	static Value ApplyTransform(const Value &constant, const IcebergTransform &transform) {
		return constant;
	}
	static bool CompareEqual(const Value &constant, const IcebergPredicateStats &stats) {
		return constant >= stats.lower_bound && constant <= stats.upper_bound;
	}
	static bool CompareLessThan(const Value &constant, const IcebergPredicateStats &stats) {
		return stats.lower_bound < constant;
	}
	static bool CompareLessThanOrEqual(const Value &constant, const IcebergPredicateStats &stats) {
		return stats.lower_bound <= constant;
	}
	static bool CompareGreaterThan(const Value &constant, const IcebergPredicateStats &stats) {
		return stats.upper_bound > constant;
	}
	static bool CompareGreaterThanOrEqual(const Value &constant, const IcebergPredicateStats &stats) {
		return stats.upper_bound >= constant;
	}
};

// struct DayTransform {
//	static Value ApplyTransform(const Value &constant, const IcebergTransform &transform) {
//		throw NotImplementedException("'day' transform ApplyTransform");
//	}
//	static bool CompareEqual(const Value &constant, const IcebergPredicateStats &stats) {
//		return constant >= stats.lower_bound && constant <= stats.upper_bound;
//	}
//	static bool CompareLessThan(const Value &constant, const IcebergPredicateStats &stats) {
//		return stats.lower_bound <= constant;
//	}
//	static bool CompareLessThanOrEqual(const Value &constant, const IcebergPredicateStats &stats) {
//		return stats.lower_bound <= constant;
//	}
//	static bool CompareGreaterThan(const Value &constant, const IcebergPredicateStats &stats) {
//		return stats.upper_bound >= constant;
//	}
//	static bool CompareGreaterThanOrEqual(const Value &constant, const IcebergPredicateStats &stats) {
//		return stats.upper_bound >= constant;
//	}
//};

} // namespace duckdb
