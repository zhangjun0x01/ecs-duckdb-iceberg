#pragma once

#include "duckdb/function/copy_function.hpp"
#include "duckdb/common/file_system.hpp"

namespace duckdb {

struct IcebergCopyToFunction {
	static CopyFunction Create();
};

struct IcebergCopyToBindData : public FunctionData {
public:
	IcebergCopyToBindData(CopyFunctionBindInput &input, const vector<string> &names, const vector<LogicalType> &types);
	virtual ~IcebergCopyToBindData();

public:
	unique_ptr<FunctionData> Copy() const override {
		throw NotImplementedException("BindData::Copy");
	}

	bool Equals(const FunctionData &other_p) const override {
		throw NotImplementedException("BindData::Equals");
	}
};

struct IcebergCopyToGlobalState : public GlobalFunctionData {
public:
	IcebergCopyToGlobalState(ClientContext &context, FunctionData &bind_data_p, FileSystem &fs,
	                         const string &file_path);
	virtual ~IcebergCopyToGlobalState();
};

struct IcebergCopyToLocalState : public LocalFunctionData {
public:
	IcebergCopyToLocalState(FunctionData &bind_data_p);
	virtual ~IcebergCopyToLocalState();
};

} // namespace duckdb
