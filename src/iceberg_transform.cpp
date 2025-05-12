#include "iceberg_transform.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

IcebergTransform::IcebergTransform() : raw_transform() {
	type = IcebergTransformType::INVALID;
}

IcebergTransform::IcebergTransform(const string &transform) : raw_transform(transform) {
	if (transform == "identity") {
		type = IcebergTransformType::IDENTITY;
	} else if (StringUtil::StartsWith(transform, "bucket")) {
		type = IcebergTransformType::BUCKET;
		D_ASSERT(transform[6] == '[');
		D_ASSERT(transform.back() == ']');
		auto start = transform.find('[');
		auto end = transform.rfind(']');
		auto digits = transform.substr(start + 1, end - start);
		modulo = std::stoi(digits);
	} else if (StringUtil::StartsWith(transform, "truncate")) {
		type = IcebergTransformType::TRUNCATE;
		D_ASSERT(transform[8] == '[');
		D_ASSERT(transform.back() == ']');
		auto start = transform.find('[');
		auto end = transform.rfind(']');
		auto digits = transform.substr(start + 1, end - start);
		width = std::stoi(digits);
	} else if (transform == "year") {
		type = IcebergTransformType::YEAR;
	} else if (transform == "month") {
		type = IcebergTransformType::MONTH;
	} else if (transform == "day") {
		type = IcebergTransformType::DAY;
	} else if (transform == "hour") {
		type = IcebergTransformType::HOUR;
	} else if (transform == "void") {
		type = IcebergTransformType::VOID;
	} else {
		throw NotImplementedException("Unrecognized transform ('%s')", transform);
	}
}

} // namespace duckdb
