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

// struct DuckLakeMetadataSerializer {
// public:
//	DuckLakeMetadataSerializer() {}
// public:
//	int64_t snapshot_id;
//	int64_t schema_id;
//	int64_t table_id;
//	int64_t partition_id;
//	int64_t data_file_id;
//	int64_t delete_file_id;

//};

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

struct DuckLakePartitionColumn {
public:
	DuckLakePartitionColumn(const IcebergPartitionSpecField &field) {
		switch (field.transform.Type()) {
		case IcebergTransformType::IDENTITY:
		case IcebergTransformType::YEAR:
		case IcebergTransformType::MONTH:
		case IcebergTransformType::DAY:
		case IcebergTransformType::HOUR: {
			transform = field.transform.RawType();
			break;
		}
		case IcebergTransformType::BUCKET:
		case IcebergTransformType::TRUNCATE:
		case IcebergTransformType::VOID:
		default:
			throw InvalidInputException("This type of transform (%s) can not be translated to DuckLake",
			                            field.transform.RawType());
		};
		column_id = field.source_id;
	}

public:
	string FinalizeEntry(int64_t table_id, int64_t partition_id, int64_t partition_key_index) {
		return StringUtil::Format("VALUES(%d, %d, %d, %d, %s)", partition_id, table_id, partition_key_index, column_id,
		                          transform);
	}

public:
	int64_t column_id;
	string transform;
};

struct DuckLakePartition {
public:
	DuckLakePartition(const IcebergPartitionSpec &partition) {
		for (auto &field : partition.fields) {
			columns.push_back(DuckLakePartitionColumn(field));
		}
	}

public:
	vector<string> FinalizeEntry(int64_t table_id, int64_t partition_id) {
		this->partition_id = partition_id;
		int64_t begin_snapshot = start_snapshot->snapshot_id.GetIndex();
		string end_snapshot = this->end_snapshot ? to_string(this->end_snapshot) : "NULL";

		vector<string> result;
		result.push_back(
		    StringUtil::Format("VALUES(%d, %d, %d, %s)", partition_id, table_id, begin_snapshot, end_snapshot));
		for (idx_t i = 0; i < columns.size(); i++) {
			auto &column = columns[i];
			result.push_back(column.FinalizeEntry(table_id, partition_id, i));
		}
		return result;
	}

public:
	//! The id is assigned after we've processed all tables
	optional_idx partition_id;
	vector<DuckLakePartitionColumn> columns;

	optional_ptr<DuckLakeSnapshot> start_snapshot;
	optional_ptr<DuckLakeSnapshot> end_snapshot;
};

struct DuckLakeDataFile {
public:
	DuckLakeDataFile(const IcebergManifestEntry &entry, DuckLakePartition &partition) : partition(partition) {
		path = entry.file_path;
		if (!StringUtil::CIEquals(entry.file_format, "parquet")) {
			throw InvalidInputException(
			    "Can't convert Iceberg tables containing data files with file_format '%s' to DuckLake",
			    entry.file_format);
		}
		record_count = entry.record_count;
		file_size_bytes = entry.file_size_in_bytes;
	}

public:
	string FinalizeEntry(int64_t table_id, int64_t data_file_id) {
		//! NOTE: partitions have to be finalized before data files
		D_ASSERT(partition.partition_id.IsValid());
		this->data_file_id = data_file_id;
		int64_t begin_snapshot = start_snapshot->snapshot_id.GetIndex();
		string end_snapshot = this->end_snapshot ? to_string(this->end_snapshot) : "NULL";
		int64_t partition_id = partition.partition_id.GetIndex();

		return StringUtil::Format("VALUES(%d, %d, %d, %s, NULL, '%s', False, 'parquet', %d, %d, NULL, NULL -- "
		                          "row_id_start, %d, NULL -- encryption_key, NULL -- partial_file_info)",
		                          data_file_id, table_id, begin_snapshot, end_snapshot, path, record_count,
		                          file_size_bytes, partition_id);
	}

public:
	DuckLakePartition &partition;

	string path;
	int64_t record_count;
	int64_t file_size_bytes;

	//! The id is assigned after we've processed all tables
	optional_idx data_file_id;

	optional_ptr<DuckLakeSnapshot> start_snapshot;
	optional_ptr<DuckLakeSnapshot> end_snapshot;
};

struct DuckLakeDeleteFile {
public:
	DuckLakeDeleteFile(const IcebergManifestEntry &entry) {
		path = entry.file_path;
		if (!StringUtil::CIEquals(entry.file_format, "parquet")) {
			throw InvalidInputException(
			    "Can't convert Iceberg tables containing delete files with file_format '%s' to DuckLake",
			    entry.file_format);
		}
		record_count = entry.record_count;
		file_size_bytes = entry.file_size_in_bytes;

		//! Find lower and upper bounds for the 'file_path' of the position delete file
		auto lower_bound_it = entry.lower_bounds.find(2147483546);
		auto upper_bound_it = entry.upper_bounds.find(2147483546);
		if (lower_bound_it == entry.lower_bounds.end() || upper_bound_it == entry.upper_bounds.end()) {
			throw InvalidInputException("No lower/upper bounds are available for the Position Delete File, this is "
			                            "required for export to DuckLake");
		}

		auto &lower_bound = lower_bound_it->second;
		auto &upper_bound = upper_bound_it->second;

		if (lower_bound.IsNull() || upper_bound.IsNull()) {
			throw InvalidInputException("Lower and Upper bounds for a Position Delete File can not be NULL!");
		}

		if (lower_bound != upper_bound) {
			throw InvalidInputException("For a Position Delete File to be eligible for conversion to DuckLake, it can "
			                            "only reference a single data file");
		}
		data_file_path = lower_bound.GetValue<string>();
	}

public:
	string FinalizeEntry(int64_t table_id, int64_t delete_file_id) {
		this->delete_file_id = delete_file_id;
		int64_t begin_snapshot = start_snapshot->snapshot_id.GetIndex();
		string end_snapshot = this->end_snapshot ? to_string(this->end_snapshot) : "NULL";
		int64_t data_file_id = referenced_data_file->data_file_id.GetIndex();

		return StringUtil::Format(
		    "VALUE(%d, %d, %d, %s, %d, '%s', false, 'parquet', %d, %d, NULL -- footer_size, NULL -- encryption_key)",
		    delete_file_id, table_id, begin_snapshot, end_snapshot, data_file_id, path, record_count, file_size_bytes);
	}

public:
	string path;
	int64_t record_count;
	int64_t file_size_bytes;
	string data_file_path;

	//! The id is assigned after we've processed all tables
	optional_idx delete_file_id;

	optional_ptr<DuckLakeSnapshot> start_snapshot;
	optional_ptr<DuckLakeSnapshot> end_snapshot;

	optional_ptr<DuckLakeDataFile> referenced_data_file;
};

struct DuckLakeTable {
public:
	DuckLakeTable(const string &table_uuid) : table_uuid(table_uuid) {
	}

public:
	//! ----- Column Updates -----

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
	//! ----- Partition Updates -----

	DuckLakePartition &AddPartition(DuckLakePartition &new_partition, DuckLakeSnapshot &begin_snapshot) {
		if (current_partition) {
			auto &old_partition = *current_partition;
			old_partition.end_snapshot = begin_snapshot;
			current_partition = nullptr;
		}
		new_partition.start_snapshot = begin_snapshot;
		all_partitions.push_back(new_partition);
		current_partition = all_partitions.back();

		return *current_partition;
	}

public:
	//! ----- Data File Updates -----

	void AddDataFile(DuckLakeDataFile &data_file, DuckLakeSnapshot &begin_snapshot) {
		data_file.start_snapshot = begin_snapshot;
		D_ASSERT(!current_data_files.count(data_file.path));
		all_data_files.push_back(data_file);
		current_data_files.emplace(data_file.path, all_data_files.back());
	}
	void DeleteDataFile(const string &data_file_path, DuckLakeSnapshot &end_snapshot) {
		auto it = current_data_files.find(data_file_path);
		if (it == current_data_files.end()) {
			throw InvalidInputException("Iceberg integrity error: Deleting a Data File that doesn't exist?");
		}

		auto &data_file = it->second.get();
		data_file.end_snapshot = end_snapshot;

		current_data_files.erase(it);
	}

public:
	//! ----- Delete File Updates -----

	void AddDeleteFile(DuckLakeDeleteFile &delete_file, DuckLakeSnapshot &begin_snapshot) {
		delete_file.start_snapshot = begin_snapshot;
		D_ASSERT(!current_delete_files.count(delete_file.path));

		//! NOTE: because delete files reference data files by id, the Data Files have to be processed first.
		//! Find the referenced data file, which verifies that the file exists as well
		auto data_file_it = current_data_files.find(delete_file.data_file_path);
		if (data_file_it == current_data_files.end()) {
			throw InvalidInputException("Iceberg integrity error: Referencing a data file that doesn't exist?");
		}
		auto &data_file = data_file_it->second.get();
		delete_file.referenced_data_file = data_file;

		//! Add to the set of referenced data files, verify that there is no other active delete file that references
		//! this data file.
		if (referenced_data_files.count(delete_file.data_file_path)) {
			throw InvalidInputException("Can't convert an Iceberg table that has multiple deletes referencing the same "
			                            "data file to a DuckLake table");
		}
		referenced_data_files.insert(delete_file.data_file_path);

		all_delete_files.push_back(delete_file);
		current_delete_files.emplace(delete_file.path, all_delete_files.back());
	}
	void DeleteDeleteFile(const string &delete_file_path, DuckLakeSnapshot &end_snapshot) {
		auto it = current_delete_files.find(delete_file_path);
		if (it == current_delete_files.end()) {
			throw InvalidInputException("Iceberg integrity error: Deleting a Delete File that doesn't exist?");
		}

		auto &delete_file = it->second.get();
		delete_file.end_snapshot = end_snapshot;
		referenced_data_files.erase(delete_file.data_file_path);

		current_delete_files.erase(it);
	}

public:
	vector<string> FinalizeEntry(int64_t table_id, int64_t schema_id, const string &table_name) {
		vector<string> result;

		//! The first schema marks the start of the table
		auto start_snapshot = all_columns.front().start_snapshot;
		int64_t begin_snapshot = start_snapshot->snapshot_id.GetIndex();

		result.push_back(StringUtil::Format("VALUES(%d, '%s', %d, NULL, %d, '%s')", table_id, table_uuid,
		                                    begin_snapshot, schema_id, table_name));
		return result;
	}

public:
	string table_uuid;

	vector<DuckLakeColumn> all_columns;
	unordered_map<int64_t, reference<DuckLakeColumn>> current_columns;

	vector<DuckLakePartition> all_partitions;
	optional_ptr<DuckLakePartition> current_partition;

	vector<DuckLakeDataFile> all_data_files;
	unordered_map<string, reference<DuckLakeDataFile>> current_data_files;

	vector<DuckLakeDeleteFile> all_delete_files;
	unordered_map<string, reference<DuckLakeDeleteFile>> current_delete_files;

	//! Keep track of which data files are referenced by active delete files
	unordered_set<string> referenced_data_files;
};

static void SchemaToColumnsInternal(const vector<unique_ptr<IcebergColumnDefinition>> &columns,
                                    unordered_map<int64_t, DuckLakeColumn> &result,
                                    optional_ptr<const IcebergColumnDefinition> parent) {
	for (idx_t i = 0; i < columns.size(); i++) {
		auto &column = *columns[i];
		result.emplace(column.id, DuckLakeColumn(column, i, parent));
		if (column.children.empty()) {
			continue;
		}
		auto &children = column.children;
		SchemaToColumnsInternal(children, result, column);
	}
}

static unordered_map<int64_t, DuckLakeColumn> SchemaToColumns(const IcebergTableSchema &schema) {
	unordered_map<int64_t, DuckLakeColumn> result;
	SchemaToColumnsInternal(schema.columns, result, nullptr);
	return result;
}

struct IcebergToDuckLakeBindData : public TableFunctionData {
public:
	IcebergToDuckLakeBindData() {
	}

public:
	void ConvertMetadata(IcebergTableMetadata &metadata, ClientContext &context, const IcebergOptions &options) {
		map<timestamp_t, reference<IcebergSnapshot>> snapshots;
		for (auto &it : metadata.snapshots) {
			snapshots.emplace(it.second.timestamp_ms, it.second);
		}

		auto &table = GetTable(metadata.table_uuid);
		//! Current schema state
		optional_ptr<IcebergTableSchema> last_schema;

		//! Current partition state
		optional_idx current_partition_spec_id;
		optional_ptr<DuckLakePartition> current_partition;

		for (auto &it : snapshots) {
			auto &snapshot = it.second.get();
			auto &ducklake_snapshot = GetSnapshot(it.first);

			//! Process the schema changes
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
			last_schema = current_schema;

			auto iceberg_table = IcebergTable::Load(metadata.location, metadata, snapshot, context, options);

			vector<DuckLakeDataFile> new_data_files;
			vector<string> deleted_data_files;

			vector<DuckLakeDeleteFile> new_delete_files;
			vector<string> deleted_delete_files;
			for (auto &entry : iceberg_table->entries) {
				auto &manifest = entry.manifest;
				auto &manifest_file = entry.manifest_file;

				if (!current_partition_spec_id.IsValid() ||
				    manifest.partition_spec_id > current_partition_spec_id.GetIndex()) {
					auto &partition_spec = *metadata.FindPartitionSpecById(manifest.partition_spec_id);
					auto new_partition = DuckLakePartition(partition_spec);
					current_partition = table.AddPartition(new_partition, ducklake_snapshot);
					current_partition_spec_id = manifest.partition_spec_id;
				}

				switch (manifest.content) {
				case IcebergManifestContentType::DATA: {
					for (auto &manifest_entry : manifest_file.data_files) {
						D_ASSERT(manifest_entry.content == IcebergManifestEntryContentType::DATA);
						if (manifest_entry.status == IcebergManifestEntryStatusType::EXISTING) {
							//! We don't care about existing entries
							continue;
						}
						if (manifest_entry.status == IcebergManifestEntryStatusType::ADDED) {
							new_data_files.push_back(DuckLakeDataFile(manifest_entry, *current_partition));
						} else {
							D_ASSERT(manifest_entry.status == IcebergManifestEntryStatusType::DELETED);
							deleted_data_files.push_back(manifest_entry.file_path);
						}
					}
					break;
				}
				case IcebergManifestContentType::DELETE: {
					for (auto &manifest_entry : manifest_file.data_files) {
						if (manifest_entry.content == IcebergManifestEntryContentType::EQUALITY_DELETES) {
							throw InvalidInputException(
							    "Can't convert a table with equality deletes to a DuckLake table");
						}
						if (manifest_entry.status == IcebergManifestEntryStatusType::EXISTING) {
							//! We don't care about existing entries
							continue;
						}
						if (manifest_entry.status == IcebergManifestEntryStatusType::ADDED) {
							new_delete_files.emplace_back(manifest_entry);
						} else {
							D_ASSERT(manifest_entry.status == IcebergManifestEntryStatusType::DELETED);
							deleted_delete_files.push_back(manifest_entry.file_path);
						}
					}
					break;
				}
				}
			}

			//! Process changes to delete files
			for (auto &delete_file : new_delete_files) {
				table.AddDeleteFile(delete_file, ducklake_snapshot);
			}
			for (auto &path : deleted_delete_files) {
				table.DeleteDeleteFile(path, ducklake_snapshot);
			}

			//! Process changes to data files
			for (auto &data_file : new_data_files) {
				table.AddDataFile(data_file, ducklake_snapshot);
			}
			for (auto &path : deleted_data_files) {
				table.DeleteDataFile(path, ducklake_snapshot);
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

	DuckLakeTable &GetTable(const string &table_uuid) {
		auto it = table_schemas.find(table_uuid);
		if (it != table_schemas.end()) {
			return it->second;
		}
		auto res = table_schemas.emplace(table_uuid, DuckLakeTable(table_uuid));
		return res.first->second;
	}

public:
	map<timestamp_t, DuckLakeSnapshot> snapshots;
	//! table_uuid -> table schema
	unordered_map<string, DuckLakeTable> table_schemas;

	idx_t snapshot_id = 0;
	idx_t schema_id = 0;
	idx_t table_id = 0;
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

	ret->ConvertMetadata(metadata, context, options);

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
