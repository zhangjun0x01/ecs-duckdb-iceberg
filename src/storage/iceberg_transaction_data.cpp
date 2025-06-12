#include "storage/iceberg_transaction_data.hpp"
#include "storage/irc_table_set.hpp"
#include "duckdb/common/types/uuid.hpp"

namespace duckdb {

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

	return make_uniq<IcebergTransactionData>(std::move(manifest_file), std::move(manifest_list), std::move(snapshot));
}

} // namespace duckdb
