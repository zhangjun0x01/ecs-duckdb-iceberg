#include "iceberg_copy.hpp"

namespace duckdb {

IcebergCopyToBindData::IcebergCopyToBindData(CopyFunctionBindInput &input, const vector<string> &names,
                                             const vector<LogicalType> &types) {
	throw NotImplementedException("BindData constructor");
}

IcebergCopyToBindData::~IcebergCopyToBindData() {
}

IcebergCopyToLocalState::IcebergCopyToLocalState(FunctionData &bind_data_p) {
}

IcebergCopyToLocalState::~IcebergCopyToLocalState() {
}

IcebergCopyToGlobalState::~IcebergCopyToGlobalState() {
}

IcebergCopyToGlobalState::IcebergCopyToGlobalState(ClientContext &context, FunctionData &bind_data_p, FileSystem &fs,
                                                   const string &file_path) {
}

static unique_ptr<FunctionData> IcebergCopyToBind(ClientContext &context, CopyFunctionBindInput &input,
                                                  const vector<string> &names, const vector<LogicalType> &sql_types) {
	throw NotImplementedException("Bind");
}

static unique_ptr<LocalFunctionData> IcebergCopyToInitializeLocal(ExecutionContext &context,
                                                                  FunctionData &bind_data_p) {
	throw NotImplementedException("InitializeLocal");
}

static unique_ptr<GlobalFunctionData> IcebergCopyToInitializeGlobal(ClientContext &context, FunctionData &bind_data_p,
                                                                    const string &file_path) {
	throw NotImplementedException("InitializeGlobal");
}

static void IcebergCopyToSink(ExecutionContext &context, FunctionData &bind_data_p, GlobalFunctionData &gstate_p,
                              LocalFunctionData &lstate_p, DataChunk &input) {
	return;
}

static void IcebergCopyToCombine(ExecutionContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
                                 LocalFunctionData &lstate) {
	return;
}

static void IcebergCopyToFinalize(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate) {
	return;
}

CopyFunctionExecutionMode IcebergCopyToExecutionMode(bool preserve_insertion_order, bool supports_batch_index) {
	return CopyFunctionExecutionMode::REGULAR_COPY_TO_FILE;
}

CopyFunction IcebergCopyToFunction::Create() {
	CopyFunction function("iceberg");
	function.extension = "iceberg";

	function.copy_to_bind = IcebergCopyToBind;
	function.copy_to_initialize_local = IcebergCopyToInitializeLocal;
	function.copy_to_initialize_global = IcebergCopyToInitializeGlobal;
	function.copy_to_sink = IcebergCopyToSink;
	function.copy_to_combine = IcebergCopyToCombine;
	function.copy_to_finalize = IcebergCopyToFinalize;
	function.execution_mode = IcebergCopyToExecutionMode;
	return function;
}

} // namespace duckdb
