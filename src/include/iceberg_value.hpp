#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

struct DeserializeResult {
public:
	DeserializeResult(Value &&val) : value(std::move(val)) {
	}
	DeserializeResult(const string &error) : error(error) {
	}

public:
	bool HasError() const {
		return !error.empty();
	}
	const string GetError() const {
		D_ASSERT(!error.empty());
		return error;
	}
	const Value &GetValue() const {
		D_ASSERT(error.empty());
		return value;
	}

public:
	Value value;
	string error;
};

struct IcebergValue {
public:
	IcebergValue() = delete;

public:
	static DeserializeResult DeserializeValue(const string_t &blob, const LogicalType &target);
};

} // namespace duckdb
