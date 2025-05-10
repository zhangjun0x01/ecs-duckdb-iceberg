#pragma once

#include "duckdb/common/types/value.hpp"

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
	static Value ApplyTransform(const Value &input, const IcebergTransform &transform) {
		return input;
	}
	static bool CompareEqual(const Value &constant, const Value &lower, const Value &upper) {
		return constant >= lower && constant <= upper;
	}
	static bool CompareLessThan(const Value &constant, const Value &lower, const Value &upper) {
		return lower < constant;
	}
	static bool CompareLessThanOrEqual(const Value &constant, const Value &lower, const Value &upper) {
		return lower <= constant;
	}
	static bool CompareGreaterThan(const Value &constant, const Value &lower, const Value &upper) {
		return upper > constant;
	}
	static bool CompareGreaterThanOrEqual(const Value &constant, const Value &lower, const Value &upper) {
		return upper >= constant;
	}
};

// struct DayTransform {
//	static Value ApplyTransform(const Value &input, const IcebergTransform &transform) {
//		throw NotImplementedException("'day' transform ApplyTransform");
//	}
//	static bool CompareEqual(const Value &constant, const Value &lower, const Value &upper) {
//		return constant >= lower && constant <= upper;
//	}
//	static bool CompareLessThan(const Value &constant, const Value &lower, const Value &upper) {
//		return lower <= constant;
//	}
//	static bool CompareLessThanOrEqual(const Value &constant, const Value &lower, const Value &upper) {
//		return lower <= constant;
//	}
//	static bool CompareGreaterThan(const Value &constant, const Value &lower, const Value &upper) {
//		return upper >= constant;
//	}
//	static bool CompareGreaterThanOrEqual(const Value &constant, const Value &lower, const Value &upper) {
//		return upper >= constant;
//	}
//};

} // namespace duckdb
