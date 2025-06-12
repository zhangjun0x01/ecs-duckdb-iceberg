#include "storage/iceberg_transaction_data.hpp"
#include "storage/irc_table_set.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"

namespace duckdb {

static LogicalType PartitionStructType(IcebergTableInformation &table_info) {
	//! TODO: actually use the partition info
	child_list_t<LogicalType> children;
	children.emplace_back("dummy", LogicalType::INTEGER);
	return LogicalType::STRUCT(children);
}

void IcebergTransactionData::WriteManifestFile(CopyFunction &copy, DatabaseInstance &db) {
	auto &fs = FileSystem::GetFileSystem(db);
	auto &allocator = db.GetBufferManager().GetBufferAllocator();

	vector<LogicalType> types;
	// status: int - 0
	types.push_back(LogicalType::INTEGER);
	// snapshot_id: long - 1
	types.push_back(LogicalType::BIGINT);
	// sequence_number: long - 3
	types.push_back(LogicalType::BIGINT);
	// file_sequence_number: long - 4
	types.push_back(LogicalType::BIGINT);

	//! DataFile struct
	child_list_t<LogicalType> children;
	// content: int - 134
	children.emplace_back("content", LogicalType::INTEGER);
	// file_path: string - 100
	children.emplace_back("file_path", LogicalType::VARCHAR);
	// file_format: string - 101
	children.emplace_back("file_format", LogicalType::VARCHAR);
	// partition: struct(...) - 102
	children.emplace_back("partition", PartitionStructType(table_info));
	// record_count: long - 103
	children.emplace_back("record_count", LogicalType::BIGINT);
	// file_size_in_bytes: long - 104
	children.emplace_back("file_size_in_bytes", LogicalType::BIGINT);

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
	types.push_back(LogicalType::STRUCT(std::move(children)));

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

	(void)copy;
	auto test = 1 + 42;
}

void IcebergTransactionData::WriteManifestList(CopyFunction &copy, DatabaseInstance &db) {
	throw InternalException("WriteManifestList");
}

rest_api_objects::AddSnapshotUpdate IcebergTransactionData::CreateSnapshotUpdate(DatabaseInstance &db) {
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	auto &schema = system_catalog.GetSchema(data, DEFAULT_SCHEMA);
	auto avro_copy_p = schema.GetEntry(data, CatalogType::COPY_FUNCTION_ENTRY, "avro");
	D_ASSERT(avro_copy_p);
	auto &avro_copy = avro_copy_p->Cast<CopyFunctionCatalogEntry>().function;

	WriteManifestFile(avro_copy, db);
	WriteManifestList(avro_copy, db);

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
