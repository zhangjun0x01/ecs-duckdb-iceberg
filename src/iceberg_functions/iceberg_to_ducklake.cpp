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

struct DuckLakeSnapshot {
public:
	DuckLakeSnapshot(timestamp_t timestamp) : snapshot_time(timestamp) {
	}

public:
	//! The snapshot id is assigned after we've processed all tables
	optional_idx snapshot_id;
	timestamp_t snapshot_time;

	//! schemas/tables/views changed
	idx_t schema_changes = 0;
	//! schemas/tables/views added
	idx_t schema_additions = 0;
	//! data or delete files added
	idx_t files_added = 0;
};

struct DefaultType {
	const char *name;
	LogicalTypeId id;
};

static string ToStringBaseType(const LogicalType &type) {
	static unordered_map<LogicalTypeId, string> type_map {{LogicalTypeId::BOOLEAN, "boolean"},
	                                                      {LogicalTypeId::TINYINT, "int8"},
	                                                      {LogicalTypeId::SMALLINT, "int16"},
	                                                      {LogicalTypeId::INTEGER, "int32"},
	                                                      {LogicalTypeId::BIGINT, "int64"},
	                                                      {LogicalTypeId::HUGEINT, "int128"},
	                                                      {LogicalTypeId::UTINYINT, "uint8"},
	                                                      {LogicalTypeId::USMALLINT, "uint16"},
	                                                      {LogicalTypeId::UINTEGER, "uint32"},
	                                                      {LogicalTypeId::UBIGINT, "uint64"},
	                                                      {LogicalTypeId::UHUGEINT, "uint128"},
	                                                      {LogicalTypeId::FLOAT, "float32"},
	                                                      {LogicalTypeId::DOUBLE, "float64"},
	                                                      {LogicalTypeId::DECIMAL, "decimal"},
	                                                      {LogicalTypeId::TIME, "time"},
	                                                      {LogicalTypeId::DATE, "date"},
	                                                      {LogicalTypeId::TIMESTAMP, "timestamp"},
	                                                      {LogicalTypeId::TIMESTAMP, "timestamp_us"},
	                                                      {LogicalTypeId::TIMESTAMP_MS, "timestamp_ms"},
	                                                      {LogicalTypeId::TIMESTAMP_NS, "timestamp_ns"},
	                                                      {LogicalTypeId::TIMESTAMP_SEC, "timestamp_s"},
	                                                      {LogicalTypeId::TIMESTAMP_TZ, "timestamptz"},
	                                                      {LogicalTypeId::TIME_TZ, "timetz"},
	                                                      {LogicalTypeId::INTERVAL, "interval"},
	                                                      {LogicalTypeId::VARCHAR, "varchar"},
	                                                      {LogicalTypeId::BLOB, "blob"},
	                                                      {LogicalTypeId::UUID, "uuid"}};

	auto it = type_map.find(type.id());
	if (it == type_map.end()) {
		throw InvalidInputException("Failed to convert DuckDB type to DuckLake - unsupported type %s", type);
	}
	return it->second;
}

static string ToDuckLakeColumnType(const LogicalType &type) {
	if (type.HasAlias()) {
		if (type.IsJSONType()) {
			return "json";
		}
		throw InvalidInputException("Unsupported user-defined type");
	}
	switch (type.id()) {
	case LogicalTypeId::STRUCT:
		return "struct";
	case LogicalTypeId::LIST:
		return "list";
	case LogicalTypeId::MAP:
		return "map";
	case LogicalTypeId::DECIMAL:
		return "decimal(" + to_string(DecimalType::GetWidth(type)) + "," + to_string(DecimalType::GetScale(type)) + ")";
	case LogicalTypeId::VARCHAR:
		if (!StringType::GetCollation(type).empty()) {
			throw InvalidInputException("Collations are not supported in DuckLake storage");
		}
		return ToStringBaseType(type);
	default:
		return ToStringBaseType(type);
	}
}

struct DuckLakeColumn {
public:
	DuckLakeColumn(const IcebergColumnDefinition &column, idx_t order,
	               optional_ptr<const IcebergColumnDefinition> parent) {
		column_id = column.id;
		if (parent) {
			parent_column = parent->id;
		}
		column_order = order;
		column_name = column.name;
		column_type = ToDuckLakeColumnType(column.type);
		initial_default = column.initial_default.ToString();
		//! TODO: parse the write-default
		default_value = "NULL";
		nulls_allowed = !column.required;
	}

public:
	string FinalizeEntry(int64_t table_id) {
		D_ASSERT(start_snapshot->snapshot_id.IsValid());
		int64_t begin_snapshot = start_snapshot->snapshot_id.GetIndex();
		string end_snapshot = this->end_snapshot ? to_string(this->end_snapshot) : "NULL";
		string parent_column = this->parent_column.IsValid() ? to_string(this->parent_column.GetIndex()) : "NULL";

		return StringUtil::Format(R"(
			VALUES (
				%d, -- column_id
				%d, -- begin_snapshot
				%s, -- end_snapshot
				%d, -- table_id
				%d, -- column_order
				%s, -- column_name
				%s, -- column_type
				%s, -- initial_default
				%s, -- default_value
				%s, -- nulls_allowed
				%s -- parent_column
			)
		)",
		                          column_id, begin_snapshot, end_snapshot, table_id, column_order, column_name,
		                          column_type, initial_default, default_value, nulls_allowed ? "true" : "false",
		                          parent_column);
	}

public:
	//! This is the 'field_id' of the iceberg schema
	int64_t column_id;
	optional_idx parent_column;

	int64_t column_order;
	string column_name;
	string column_type;
	string initial_default;
	string default_value;
	bool nulls_allowed;

	optional_ptr<DuckLakeSnapshot> start_snapshot;
	optional_ptr<DuckLakeSnapshot> end_snapshot;
};

struct DuckLakeTableSchema {
public:
	DuckLakeTableSchema(const string &table_uuid) : table_uuid(table_uuid) {
	}

public:
	//! Used for both ALTER and ADD
	void AddColumnVersion(DuckLakeColumn &new_column, DuckLakeSnapshot &begin_snapshot) {
		//! First set the end snapshot of the old version of this column (if it exists)
		auto column_id = new_column.column_id;
		DropColumnVersion(column_id, begin_snapshot);
		new_column.start_snapshot = begin_snapshot;
		all_columns.push_back(new_column);
		current_columns.emplace(column_id, all_columns.back());
	}
	void DropColumnVersion(int64_t column_id, DuckLakeSnapshot &end_snapshot) {
		auto it = current_columns.find(column_id);
		if (it == current_columns.end()) {
			return;
		}
		auto &column = it->second.get();
		column.end_snapshot = end_snapshot;
		current_columns.erase(it);
	}

public:
	string table_uuid;

	vector<DuckLakeColumn> all_columns;
	unordered_map<int64_t, reference<DuckLakeColumn>> current_columns;
};

static unordered_map<int64_t, DuckLakeColumn> SchemaToColumns(const IcebergTableSchema &schema) {
	unordered_map<int64_t, DuckLakeColumn> result;

	//! TODO: convert to DuckLakeColumns
	return result;
}

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

	void ConvertMetadata(IcebergTableMetadata &metadata) {
		map<int32_t, reference<IcebergPartitionSpec>> partitions;
		map<int64_t, reference<IcebergSnapshot>> snapshots;

		//! Create ordered maps, to iterate sequentially through
		for (auto &it : metadata.partition_specs) {
			partitions.emplace(it.first, it.second);
		}
		for (auto &it : metadata.snapshots) {
			snapshots.emplace(it.first, it.second);
		}

		//! Convert the iceberg partition specs
		for (auto &it : partitions) {
		}

		auto &table = GetTable(metadata.table_uuid);
		//! Convert the iceberg snapshot
		optional_ptr<IcebergTableSchema> last_schema;
		for (auto &it : snapshots) {
			auto &snapshot = it.second.get();
			auto &ducklake_snapshot = GetSnapshot(snapshot.timestamp_ms);

			auto &current_schema = *metadata.GetSchemaFromId(snapshot.schema_id);
			auto current_columns = SchemaToColumns(current_schema);
			vector<DuckLakeColumn> added_columns;
			vector<int64_t> dropped_columns;
			if (last_schema) {
				if (last_schema->schema_id != current_schema.schema_id) {
					auto existing_columns = SchemaToColumns(*last_schema);
					//! TODO: compare old and new columns to get a diff
				}
			} else {
				for (auto &it : current_columns) {
					added_columns.push_back(it.second);
				}
			}

			for (auto &column : added_columns) {
				table.AddColumnVersion(column, ducklake_snapshot);
			}
			for (auto id : dropped_columns) {
				table.DropColumnVersion(id, ducklake_snapshot);
			}
		}
	}

private:
	DuckLakeSnapshot &GetSnapshot(timestamp_t timestamp) {
		auto it = snapshots.find(timestamp);
		if (it != snapshots.end()) {
			return it->second;
		}
		auto res = snapshots.emplace(timestamp, DuckLakeSnapshot(timestamp));
		return res.first->second;
	}

	DuckLakeTableSchema &GetTable(const string &table_uuid) {
		auto it = table_schemas.find(table_uuid);
		if (it != table_schemas.end()) {
			return it->second;
		}
		auto res = table_schemas.emplace(table_uuid, DuckLakeTableSchema(table_uuid));
		return res.first->second;
	}

public:
	unique_ptr<IcebergTable> iceberg_table;

	map<timestamp_t, DuckLakeSnapshot> snapshots;
	//! table_uuid -> table schema
	unordered_map<string, DuckLakeTableSchema> table_schemas;

	idx_t snapshot_id = 0;
	idx_t schema_id = 0;
	idx_t table_id = 0;
	idx_t column_id = 0;
	idx_t partition_id = 0;
	idx_t data_file_id = 0;
	idx_t delete_file_id = 0;

	//! Only changed when an existing schema, table or view is *changed*
	idx_t schema_version = 0;
	//! Only changed when a schema, table or view is added
	idx_t next_catalog_id = 0;
	//! Only changed when a data_file or delete_file is added
	idx_t next_file_id = 0;
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

	ret->ConvertMetadata(metadata);

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
