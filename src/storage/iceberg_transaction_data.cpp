#include "storage/iceberg_transaction_data.hpp"
#include "storage/irc_table_set.hpp"
#include "storage/table_update/iceberg_add_snapshot.hpp"

#include "duckdb/common/types/uuid.hpp"

namespace duckdb {

static int64_t NewSnapshotId() {
	auto random_number = UUID::GenerateRandomUUID().upper;
	if (random_number < 0) {
		// Flip the sign bit using XOR with 1LL shifted left 63 bits
		random_number ^= (1LL << 63);
	}
	return random_number;
}

void IcebergTransactionData::AddSnapshot(IcebergSnapshotOperationType operation,
                                         vector<IcebergManifestEntry> &&data_files) {
	//! Generate a new snapshot id
	auto &table_metadata = table_info.table_metadata;
	auto snapshot_id = NewSnapshotId();
	auto sequence_number = table_metadata.last_sequence_number + 1;

	//! Construct the manifest file
	auto manifest_file_uuid = UUID::ToString(UUID::GenerateRandomUUID());
	auto manifest_file_path = table_info.BaseFilePath() + "/metadata/" + manifest_file_uuid + "-m0.avro";
	IcebergManifestFile new_manifest_file(manifest_file_path);

	//! Construct the manifest list
	auto manifest_list_uuid = UUID::ToString(UUID::GenerateRandomUUID());
	auto manifest_list_path = table_info.BaseFilePath() + "/metadata/snap-" + std::to_string(snapshot_id) + "-" +
	                          manifest_list_uuid + ".avro";
	IcebergManifestList new_manifest_list(manifest_list_path);

	//! Construct the manifest, part of the manifest list
	IcebergManifest new_manifest;
	new_manifest.manifest_path = manifest_file_path;
	new_manifest.sequence_number = sequence_number;
	new_manifest.content = IcebergManifestContentType::DATA;
	new_manifest.added_files_count = data_files.size();
	new_manifest.existing_files_count = 0;
	new_manifest.deleted_files_count = 0;
	new_manifest.added_rows_count = 0;
	new_manifest.existing_rows_count = 0;
	new_manifest.deleted_rows_count = 0;
	//! TODO: support partitions
	new_manifest.partition_spec_id = 0;
	//! new_manifest.partitions = CreateManifestPartition();

	new_manifest_list.manifests.emplace_back(std::move(new_manifest));

	//! Construct the snapshot
	IcebergSnapshot new_snapshot;
	new_snapshot.snapshot_id = snapshot_id;
	new_snapshot.sequence_number = sequence_number;
	new_snapshot.schema_id = table_metadata.current_schema_id;
	new_snapshot.manifest_list = manifest_list_path;
	new_snapshot.operation = operation;
	new_snapshot.timestamp_ms = Timestamp::GetEpochMs(MetaTransaction::Get(context).start_timestamp);

	new_snapshot.has_parent_snapshot = !table_info.table_metadata.snapshots.empty() || !alters.empty();
	if (new_snapshot.has_parent_snapshot) {
		if (!alters.empty()) {
			auto &last_alter = alters.back().get();
			new_snapshot.parent_snapshot_id = last_alter.snapshot.snapshot_id;
		} else {
			D_ASSERT(table_info.table_metadata.has_current_snapshot);
			new_snapshot.parent_snapshot_id = table_info.table_metadata.current_snapshot_id;
		}
	}

	auto add_snapshot = make_uniq<IcebergAddSnapshot>(table_info, std::move(new_manifest_file),
	                                                  std::move(new_manifest_list), std::move(new_snapshot));

	auto &manifest_list = add_snapshot->manifest_list;
	auto &manifest_file = add_snapshot->manifest_file;
	auto &manifest = manifest_list.manifests.back();
	auto &snapshot = add_snapshot->snapshot;

	//! Add the data files
	for (auto &data_file : data_files) {
		manifest.added_rows_count += data_file.record_count;
		data_file.sequence_number = snapshot.sequence_number;
	}
	manifest_file.data_files.insert(manifest_file.data_files.end(), std::make_move_iterator(data_files.begin()),
	                                std::make_move_iterator(data_files.end()));
	alters.push_back(*add_snapshot);
	updates.push_back(std::move(add_snapshot));
}

} // namespace duckdb
