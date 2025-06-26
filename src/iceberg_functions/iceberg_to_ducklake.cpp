#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/common/enums/joinref_type.hpp"
#include "duckdb/common/enums/tableref_type.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/query_node/recursive_cte_node.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/tableref/emptytableref.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/file_system.hpp"
#include "iceberg_metadata.hpp"
#include "iceberg_functions.hpp"
#include "iceberg_utils.hpp"

#include "metadata/iceberg_table_metadata.hpp"

namespace duckdb {

struct IcebergToDuckLakeBindData : public TableFunctionData {
public:
	IcebergToDuckLakeBindData() {
	}

public:
	//! ducklake_column
	void ConvertColumns() {
		//! ducklake columns are deltas on previous schemas
		//! TODO: parse the iceberg schemas and calculate the diff between each schema
	}

	//! ducklake_data_file
	//! ducklake_file_partition_value
	void CreateDataFileEntry() {
	}

	//! ducklake_delete_file
	void CreateDeleteFileEntry() {
	}

	//! ducklake_file_column_statistics
	//! ducklake_table_column_stats
	//! ducklake_table_stats
	void CreateStatistics() {
		//! NOTE: this is only written once, not once per snapshot
	}

	//! ducklake_snapshot
	//! ducklake_snapshot_changes
	void CreateSnapshot() {
	}

	//! ducklake_partition_column
	void ScanMetadata() {
		//! ConvertColumns()
		//! CreateSnapshot (for each snapshot in the iceberg table)
	}
	//! ducklake_schema
	//! ducklake_table
	void CreateCatalogSchema() {
		//! NOTE: We default to use the 'main' schema in the DuckLake Catalog
	}

public:
	unique_ptr<IcebergTable> iceberg_table;
};

struct IcebergToDuckLakeGlobalTableFunctionState : public GlobalTableFunctionState {
public:
	IcebergToDuckLakeGlobalTableFunctionState() {

	};

public:
	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
		return make_uniq<IcebergToDuckLakeGlobalTableFunctionState>();
	}

public:
	//! TODO: add connection to ducklake catalog, ducklake id values .. (like snapshot id)
};

static unique_ptr<FunctionData> IcebergToDuckLakeBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	auto ret = make_uniq<IcebergToDuckLakeBindData>();

	FileSystem &fs = FileSystem::GetFileSystem(context);
	auto input_string = input.inputs[0].ToString();
	auto filename = IcebergUtils::GetStorageLocation(context, input_string);

	IcebergOptions options;
	auto iceberg_meta_path = IcebergTableMetadata::GetMetaDataPath(context, filename, fs, options);
	auto table_metadata = IcebergTableMetadata::Parse(iceberg_meta_path, fs, options.metadata_compression_codec);
	auto metadata = IcebergTableMetadata::FromTableMetadata(table_metadata);

	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("count");

	return std::move(ret);
}

static void IcebergToDuckLakeFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<IcebergToDuckLakeBindData>();
	auto &global_state = data.global_state->Cast<IcebergToDuckLakeGlobalTableFunctionState>();
	output.SetCardinality(0);
}

TableFunctionSet IcebergFunctions::GetIcebergToDuckLakeFunction() {
	TableFunctionSet function_set("iceberg_to_ducklake");

	auto fun =
	    TableFunction({LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, IcebergToDuckLakeFunction,
	                  IcebergToDuckLakeBind, IcebergToDuckLakeGlobalTableFunctionState::Init);
	function_set.AddFunction(fun);

	return function_set;
}

} // namespace duckdb
