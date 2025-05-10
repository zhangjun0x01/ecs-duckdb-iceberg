#include "iceberg_value.hpp"
#include "duckdb/common/helper.hpp"

namespace duckdb {

static DeserializeResult DeserializeError(const string_t &blob, const LogicalType &type) {
	return DeserializeResult(
	    StringUtil::Format("Failed to deserialize blob '%s' of size %d, attempting to produce value of type '%s'",
	                       blob.GetString(), blob.GetSize(), type.ToString()));
}

template <class VALUE_TYPE>
static Value DeserializeDecimalTemplated(const string_t &blob, uint8_t width, uint8_t scale) {
	VALUE_TYPE ret = 0;
	//! The blob has to be smaller or equal to the size of the type
	D_ASSERT(blob.GetSize() <= sizeof(VALUE_TYPE));
	std::memcpy(&ret, blob.GetData(), blob.GetSize());
	return Value::DECIMAL(ret, width, scale);
}

static Value DeserializeHugeintDecimal(const string_t &blob, uint8_t width, uint8_t scale) {
	hugeint_t ret;

	//! The blob has to be smaller or equal to the size of the type
	D_ASSERT(blob.GetSize() <= sizeof(hugeint_t));
	int64_t upper_val = 0;
	uint64_t lower_val = 0;
	// Read upper and lower parts of hugeint

	idx_t upper_val_size = MinValue<idx_t>(blob.GetSize(), sizeof(int64_t));

	std::memcpy(&upper_val, blob.GetData(), upper_val_size);
	if (blob.GetSize() > sizeof(int64_t)) {
		idx_t lower_val_size = MinValue(static_cast<size_t>(blob.GetSize() - sizeof(int64_t)), sizeof(uint64_t));
		std::memcpy(&lower_val, blob.GetData() + sizeof(int64_t), lower_val_size);
	}
	ret = hugeint_t(upper_val, lower_val);
	return Value::DECIMAL(ret, width, scale);
}

static DeserializeResult DeserializeDecimal(const string_t &blob, const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::DECIMAL);

	uint8_t width;
	uint8_t scale;
	if (!type.GetDecimalProperties(width, scale)) {
		return DeserializeError(blob, type);
	}

	auto physical_type = type.InternalType();
	switch (physical_type) {
	case PhysicalType::INT16: {
		return DeserializeDecimalTemplated<int16_t>(blob, width, scale);
	}
	case PhysicalType::INT32: {
		return DeserializeDecimalTemplated<int32_t>(blob, width, scale);
	}
	case PhysicalType::INT64: {
		return DeserializeDecimalTemplated<int64_t>(blob, width, scale);
	}
	case PhysicalType::INT128: {
		return DeserializeHugeintDecimal(blob, width, scale);
	}
	default:
		throw InternalException("DeserializeDecimal not implemented for physical type '%s'",
		                        TypeIdToString(physical_type));
	}
}

//! FIXME: because of schema evolution, there are rules for inferring the correct type that we need to apply:
//! See https://iceberg.apache.org/spec/#schema-evolution
DeserializeResult IcebergValue::DeserializeValue(const string_t &blob, const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::INTEGER: {
		if (blob.GetSize() != sizeof(int32_t)) {
			return DeserializeError(blob, type);
		}
		int32_t val;
		std::memcpy(&val, blob.GetData(), sizeof(int32_t));
		return Value::INTEGER(val);
	}
	case LogicalTypeId::BIGINT: {
		if (blob.GetSize() != sizeof(int64_t)) {
			return DeserializeError(blob, type);
		}
		int64_t val;
		std::memcpy(&val, blob.GetData(), sizeof(int64_t));
		return Value::BIGINT(val);
	}
	case LogicalTypeId::DATE: {
		if (blob.GetSize() != sizeof(int32_t)) { // Dates are typically stored as int32 (days since epoch)
			return DeserializeError(blob, type);
		}
		int32_t days_since_epoch;
		std::memcpy(&days_since_epoch, blob.GetData(), sizeof(int32_t));
		// Convert to DuckDB date
		date_t date = Date::EpochDaysToDate(days_since_epoch);
		return Value::DATE(date);
	}
	case LogicalTypeId::TIMESTAMP: {
		if (blob.GetSize() != sizeof(int64_t)) { // Timestamps are typically stored as int64 (microseconds since epoch)
			return DeserializeError(blob, type);
		}
		int64_t micros_since_epoch;
		std::memcpy(&micros_since_epoch, blob.GetData(), sizeof(int64_t));
		// Convert to DuckDB timestamp using microseconds
		timestamp_t timestamp = Timestamp::FromEpochMicroSeconds(micros_since_epoch);
		return Value::TIMESTAMP(timestamp);
	}
	case LogicalTypeId::TIMESTAMP_TZ: {
		if (blob.GetSize() != sizeof(int64_t)) { // Assuming stored as int64 (microseconds since epoch)
			return DeserializeError(blob, type);
		}
		int64_t micros_since_epoch;
		std::memcpy(&micros_since_epoch, blob.GetData(), sizeof(int64_t));
		// Convert to DuckDB timestamp using microseconds
		timestamp_t timestamp = Timestamp::FromEpochMicroSeconds(micros_since_epoch);
		// Create a TIMESTAMPTZ Value
		return Value::TIMESTAMPTZ(timestamp_tz_t(timestamp));
	}
	case LogicalTypeId::DOUBLE: {
		if (blob.GetSize() != sizeof(double)) {
			return DeserializeError(blob, type);
		}
		double val;
		std::memcpy(&val, blob.GetData(), sizeof(double));
		return Value::DOUBLE(val);
	}
	case LogicalTypeId::VARCHAR: {
		// Assume the bytes represent a UTF-8 string
		return Value(blob);
	}
	case LogicalTypeId::DECIMAL: {
		return DeserializeDecimal(blob, type);
	}
	case LogicalTypeId::BOOLEAN: {
		if (blob.GetSize() != 1) {
			return DeserializeError(blob, type);
		}
		const bool val = blob.GetData()[0] != '\0';
		return Value::BOOLEAN(val);
	}
	case LogicalTypeId::FLOAT: {
		if (blob.GetSize() != sizeof(float)) {
			return DeserializeError(blob, type);
		}
		float val;
		std::memcpy(&val, blob.GetData(), sizeof(float));
		return Value::FLOAT(val);
	}
	case LogicalTypeId::TIME: {
		if (blob.GetSize() != sizeof(int64_t)) {
			return DeserializeError(blob, type);
		}
		//! bound stores microseconds since midnight
		dtime_t val;
		std::memcpy(&val.micros, blob.GetData(), sizeof(int64_t));
		return Value::TIME(val);
	}
	case LogicalTypeId::TIMESTAMP_NS: {
		throw NotImplementedException("timestamp_ns");
	}
	case LogicalTypeId::UUID: {
		if (blob.GetSize() != 16) {
			return DeserializeError(blob, type);
		}
		return Value::UUID(blob.GetString());
	}
	// Add more types as needed
	default:
		break;
	}
	return DeserializeError(blob, type);
}

} // namespace duckdb
