#include "storage/iceberg_transaction_data.hpp"
#include "storage/irc_table_set.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"

#include "duckdb/execution/execution_context.hpp"
#include "duckdb/parallel/thread_context.hpp"

namespace duckdb {

static LogicalType PartitionStructType(IcebergTableInformation &table_info) {
	//! TODO: actually use the partition info
	child_list_t<LogicalType> children;
	children.emplace_back("dummy", LogicalType::INTEGER);
	return LogicalType::STRUCT(children);
}

void IcebergTransactionData::WriteManifestFile(CopyFunction &copy, DatabaseInstance &db, ClientContext &context) {
	auto &allocator = db.GetBufferManager().GetBufferAllocator();

	//! Create the types for the DataChunk

	child_list_t<Value> field_ids;
	vector<string> names;
	vector<LogicalType> types;
	// status: int - 0
	names.push_back("status");
	types.push_back(LogicalType::INTEGER);
	field_ids.emplace_back("status", Value::INTEGER(0));

	// snapshot_id: long - 1
	names.push_back("snapshot_id");
	types.push_back(LogicalType::BIGINT);
	field_ids.emplace_back("snapshot_id", Value::INTEGER(1));

	// sequence_number: long - 3
	names.push_back("sequence_number");
	types.push_back(LogicalType::BIGINT);
	field_ids.emplace_back("sequence_number", Value::INTEGER(3));

	// file_sequence_number: long - 4
	names.push_back("file_sequence_number");
	types.push_back(LogicalType::BIGINT);
	field_ids.emplace_back("file_sequence_number", Value::INTEGER(4));

	//! DataFile struct

	child_list_t<Value> data_file_field_ids;
	child_list_t<LogicalType> children;
	// content: int - 134
	children.emplace_back("content", LogicalType::INTEGER);
	data_file_field_ids.emplace_back("content", Value::INTEGER(134));

	// file_path: string - 100
	children.emplace_back("file_path", LogicalType::VARCHAR);
	data_file_field_ids.emplace_back("file_path", Value::INTEGER(100));

	// file_format: string - 101
	children.emplace_back("file_format", LogicalType::VARCHAR);
	data_file_field_ids.emplace_back("file_format", Value::INTEGER(101));

	// partition: struct(...) - 102
	children.emplace_back("partition", PartitionStructType(table_info));
	data_file_field_ids.emplace_back("partition", Value::INTEGER(102));

	// record_count: long - 103
	children.emplace_back("record_count", LogicalType::BIGINT);
	data_file_field_ids.emplace_back("record_count", Value::INTEGER(103));

	// file_size_in_bytes: long - 104
	children.emplace_back("file_size_in_bytes", LogicalType::BIGINT);
	data_file_field_ids.emplace_back("file_size_in_bytes", Value::INTEGER(104));

	//// column_sizes: map<117: int, 118: long> - 108
	// children.emplace_back("column_sizes", LogicalType::MAP(LogicalType::INTEGER, LogicalType::BIGINT));
	//// value_counts: map<119: int, 120: long> - 109
	// children.emplace_back("value_counts", LogicalType::MAP(LogicalType::INTEGER, LogicalType::BIGINT));
	//// null_value_counts: map<121: int, 122: long> - 110
	// children.emplace_back("null_value_counts", LogicalType::MAP(LogicalType::INTEGER, LogicalType::BIGINT));
	//// nan_value_counts: map<138: int, 139: long> - 137
	// children.emplace_back("nan_value_counts", LogicalType::MAP(LogicalType::INTEGER, LogicalType::BIGINT));
	//// lower_bounds: map<126: int, 127: binary> - 125
	// children.emplace_back("lower_bounds", LogicalType::MAP(LogicalType::INTEGER, LogicalType::BLOB));
	//// upper_bounds: map<129: int, 130: binary> - 128
	// children.emplace_back("upper_bounds", LogicalType::MAP(LogicalType::INTEGER, LogicalType::BLOB));

	// data_file: struct(...) - 2
	names.push_back("data_file");
	types.push_back(LogicalType::STRUCT(std::move(children)));
	data_file_field_ids.emplace_back("__duckdb_field_id", Value::INTEGER(2));
	field_ids.emplace_back("data_file", Value::STRUCT(data_file_field_ids));

	//! Populate the DataChunk with the data files

	DataChunk data;
	data.Initialize(allocator, types, manifest_file.data_files.size());

	for (idx_t i = 0; i < manifest_file.data_files.size(); i++) {
		auto &data_file = manifest_file.data_files[i];
		idx_t col_idx = 0;

		//! We rely on inheriting the snapshot_id, this is only acceptable for ADDED data files
		D_ASSERT(data_file.status == IcebergManifestEntryStatusType::ADDED);

		// status: int - 0
		data.SetValue(col_idx++, i, Value::INTEGER(static_cast<int32_t>(data_file.status)));
		// snapshot_id: long - 1
		data.SetValue(col_idx++, i, Value(LogicalType::BIGINT));
		// sequence_number: long - 3
		data.SetValue(col_idx++, i, Value(LogicalType::BIGINT));
		// file_sequence_number: long - 4
		data.SetValue(col_idx++, i, Value(LogicalType::BIGINT));
		// data_file: struct(...) - 2
		data.SetValue(col_idx, i, data_file.ToDataFileStruct(data.data[col_idx].GetType()));
		col_idx++;
	}
	data.SetCardinality(manifest_file.data_files.size());

	CopyInfo copy_info;
	copy_info.is_from = false;
	copy_info.options["root_name"].push_back(Value("manifest_file"));
	copy_info.options["field_ids"].push_back(Value::STRUCT(field_ids));

	CopyFunctionBindInput input(copy_info);
	input.file_extension = "avro";

	ThreadContext thread_context(context);
	ExecutionContext execution_context(context, thread_context, nullptr);
	auto bind_data = copy.copy_to_bind(context, input, names, types);

	auto global_state = copy.copy_to_initialize_global(context, *bind_data, manifest_file.path);
	auto local_state = copy.copy_to_initialize_local(execution_context, *bind_data);

	copy.copy_to_sink(execution_context, *bind_data, *global_state, *local_state, data);
	copy.copy_to_combine(execution_context, *bind_data, *global_state, *local_state);
	copy.copy_to_finalize(context, *bind_data, *global_state);
}

static LogicalType FieldSummaryType() {
	child_list_t<LogicalType> children;
	children.emplace_back("contains_null", LogicalType::BOOLEAN);
	children.emplace_back("contains_nan", LogicalType::BOOLEAN);
	children.emplace_back("lower_bound", LogicalType::BLOB);
	children.emplace_back("upper_bound", LogicalType::BLOB);
	auto field_summary = LogicalType::STRUCT(children);

	return LogicalType::LIST(field_summary);
}

static Value FieldSummaryFieldIds() {
	child_list_t<Value> children;
	children.emplace_back("contains_null", Value::INTEGER(509));
	children.emplace_back("contains_nan", Value::INTEGER(518));
	children.emplace_back("lower_bound", Value::INTEGER(510));
	children.emplace_back("upper_bound", Value::INTEGER(511));
	children.emplace_back("__duckdb_field_id", Value::INTEGER(508));
	auto field_summary = Value::STRUCT(children);

	child_list_t<Value> list_children;
	list_children.emplace_back("element", field_summary);
	list_children.emplace_back("__duckdb_field_id", Value::INTEGER(507));
	return Value::STRUCT(list_children);
}

void IcebergTransactionData::WriteManifestList(CopyFunction &copy, DatabaseInstance &db, ClientContext &context) {
	auto &allocator = db.GetBufferManager().GetBufferAllocator();

	//! Create the types for the DataChunk

	child_list_t<Value> field_ids;
	vector<string> names;
	vector<LogicalType> types;

	// manifest_path: string - 500
	names.push_back("manifest_path");
	types.push_back(LogicalType::VARCHAR);
	field_ids.emplace_back("manifest_path", Value::INTEGER(500));

	// manifest_length: long - 501
	names.push_back("manifest_length");
	types.push_back(LogicalType::BIGINT);
	field_ids.emplace_back("manifest_length", Value::INTEGER(501));

	// partition_spec_id: long - 502
	names.push_back("partition_spec_id");
	types.push_back(LogicalType::BIGINT);
	field_ids.emplace_back("partition_spec_id", Value::INTEGER(502));

	// content: int - 517
	names.push_back("content");
	types.push_back(LogicalType::INTEGER);
	field_ids.emplace_back("content", Value::INTEGER(517));

	// sequence_number: long - 515
	names.push_back("sequence_number");
	types.push_back(LogicalType::BIGINT);
	field_ids.emplace_back("sequence_number", Value::INTEGER(515));

	// min_sequence_number: long - 516
	names.push_back("min_sequence_number");
	types.push_back(LogicalType::BIGINT);
	field_ids.emplace_back("min_sequence_number", Value::INTEGER(516));

	// added_snapshot_id: long - 503
	names.push_back("added_snapshot_id");
	types.push_back(LogicalType::BIGINT);
	field_ids.emplace_back("added_snapshot_id", Value::INTEGER(503));

	// added_files_count: int - 504
	names.push_back("added_files_count");
	types.push_back(LogicalType::INTEGER);
	field_ids.emplace_back("added_files_count", Value::INTEGER(504));

	// existing_files_count: int - 505
	names.push_back("existing_files_count");
	types.push_back(LogicalType::INTEGER);
	field_ids.emplace_back("existing_files_count", Value::INTEGER(505));

	// deleted_files_count: int - 506
	names.push_back("deleted_files_count");
	types.push_back(LogicalType::INTEGER);
	field_ids.emplace_back("deleted_files_count", Value::INTEGER(506));

	// added_rows_count: long - 512
	names.push_back("added_rows_count");
	types.push_back(LogicalType::BIGINT);
	field_ids.emplace_back("added_rows_count", Value::INTEGER(512));

	// existing_rows_count: long - 513
	names.push_back("existing_rows_count");
	types.push_back(LogicalType::BIGINT);
	field_ids.emplace_back("existing_rows_count", Value::INTEGER(513));

	// deleted_rows_count: long - 514
	names.push_back("deleted_rows_count");
	types.push_back(LogicalType::BIGINT);
	field_ids.emplace_back("deleted_rows_count", Value::INTEGER(514));

	// partitions: list<508: field_summary> - 507
	names.push_back("partitions");
	types.push_back(FieldSummaryType());
	field_ids.emplace_back("partitions", FieldSummaryFieldIds());

	//! Populate the DataChunk with the manifests

	DataChunk data;
	data.Initialize(allocator, types, manifest_list.manifests.size());

	for (idx_t i = 0; i < manifest_list.manifests.size(); i++) {
		auto &manifest = manifest_list.manifests[i];
		idx_t col_idx = 0;

		// manifest_path: string - 500
		data.SetValue(col_idx++, i, Value(manifest.manifest_path));

		// manifest_length: long - 501
		//! TODO: collect the length from the manifest file write.
		data.SetValue(col_idx++, i, Value(42069));

		// partition_spec_id: long - 502
		data.SetValue(col_idx++, i, Value::BIGINT(manifest.partition_spec_id));

		// content: int - 517
		data.SetValue(col_idx++, i, Value::INTEGER(static_cast<int32_t>(manifest.content)));

		// sequence_number: long - 515
		data.SetValue(col_idx++, i, Value::BIGINT(manifest.sequence_number));

		// min_sequence_number: long - 516
		//! TODO: keep track of the min sequence number
		data.SetValue(col_idx++, i, Value::BIGINT(manifest.sequence_number));

		// added_snapshot_id: long - 503
		//! TODO: add the snapshot id to the IcebergManifest
		data.SetValue(col_idx++, i, Value::BIGINT(42069));

		// added_files_count: int - 504
		//! TODO: write this into the IcebergManifest, instead of relying on the manifest_file
		data.SetValue(col_idx++, i, Value::INTEGER(manifest_file.data_files.size()));

		// existing_files_count: int - 505
		data.SetValue(col_idx++, i, Value::INTEGER(0));

		// deleted_files_count: int - 506
		data.SetValue(col_idx++, i, Value::INTEGER(0));

		// added_rows_count: long - 512
		data.SetValue(col_idx++, i, Value::BIGINT(static_cast<int64_t>(manifest.added_rows_count)));

		// existing_rows_count: long - 513
		data.SetValue(col_idx++, i, Value::BIGINT(static_cast<int64_t>(manifest.existing_rows_count)));

		// deleted_rows_count: long - 514
		data.SetValue(col_idx++, i, Value::BIGINT(static_cast<int64_t>(0)));

		// partitions: list<508: field_summary> - 507
		data.SetValue(col_idx++, i, manifest.partitions.ToValue());
	}
	data.SetCardinality(manifest_list.manifests.size());

	CopyInfo copy_info;
	copy_info.is_from = false;
	copy_info.options["root_name"].push_back(Value("manifest_list"));
	copy_info.options["field_ids"].push_back(Value::STRUCT(field_ids));

	CopyFunctionBindInput input(copy_info);
	input.file_extension = "avro";

	ThreadContext thread_context(context);
	ExecutionContext execution_context(context, thread_context, nullptr);
	auto bind_data = copy.copy_to_bind(context, input, names, types);

	auto global_state = copy.copy_to_initialize_global(context, *bind_data, manifest_list.path);
	auto local_state = copy.copy_to_initialize_local(execution_context, *bind_data);

	copy.copy_to_sink(execution_context, *bind_data, *global_state, *local_state, data);
	copy.copy_to_combine(execution_context, *bind_data, *global_state, *local_state);
	copy.copy_to_finalize(context, *bind_data, *global_state);
	// Printer::PrintF("Manifest List path: %s", manifest_list.path);
}

rest_api_objects::AddSnapshotUpdate IcebergTransactionData::CreateSnapshotUpdate(DatabaseInstance &db,
                                                                                 ClientContext &context) {
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	auto &schema = system_catalog.GetSchema(data, DEFAULT_SCHEMA);
	auto avro_copy_p = schema.GetEntry(data, CatalogType::COPY_FUNCTION_ENTRY, "avro");
	D_ASSERT(avro_copy_p);
	auto &avro_copy = avro_copy_p->Cast<CopyFunctionCatalogEntry>().function;

	WriteManifestFile(avro_copy, db, context);
	WriteManifestList(avro_copy, db, context);

	rest_api_objects::AddSnapshotUpdate update;
	update.base_update.action = "add-snapshot";
	update.has_action = true;
	update.action = "add-snapshot";
	update.snapshot = snapshot.ToRESTObject();
	return update;
}

unique_ptr<IcebergTransactionData> IcebergTransactionData::Create(ClientContext &context,
                                                                  IcebergTableInformation &table_info) {
	//! Generate a new snapshot id
	auto &table_metadata = table_info.table_metadata;
	auto snapshot_id = UUID::GenerateRandomUUID().upper;
	auto sequence_number = table_metadata.last_sequence_number + 1;

	//! Construct the manifest file
	auto manifest_file_uuid = UUID::ToString(UUID::GenerateRandomUUID());
	auto manifest_file_path = table_info.BaseFilePath() + "/metadata/" + manifest_file_uuid + "-m0.avro";
	IcebergManifestFile manifest_file(manifest_file_path);

	//! Construct the manifest list
	auto manifest_list_uuid = UUID::ToString(UUID::GenerateRandomUUID());
	auto manifest_list_path = table_info.BaseFilePath() + "/metadata/snap-" + std::to_string(snapshot_id) + "-" +
	                          manifest_list_uuid + ".avro";
	IcebergManifestList manifest_list(manifest_list_path);

	//! Construct the manifest, part of the manifest list
	IcebergManifest manifest;
	manifest.manifest_path = manifest_file_path;
	manifest.sequence_number = sequence_number;
	manifest.content = IcebergManifestContentType::DATA;
	manifest.added_rows_count = 0;
	manifest.existing_rows_count = 0;
	//! TODO: support partitions
	manifest.partition_spec_id = 0;
	//! manifest.partitions = CreateManifestPartition();

	manifest_list.manifests.emplace_back(std::move(manifest));

	//! Construct the snapshot
	IcebergSnapshot snapshot;
	snapshot.snapshot_id = snapshot_id;
	snapshot.sequence_number = sequence_number;
	snapshot.schema_id = table_metadata.current_schema_id;
	snapshot.manifest_list = manifest_list_path;
	snapshot.timestamp_ms = Timestamp::GetEpochMs(MetaTransaction::Get(context).start_timestamp);

	return make_uniq<IcebergTransactionData>(table_info, std::move(manifest_file), std::move(manifest_list),
	                                         std::move(snapshot));
}

} // namespace duckdb
