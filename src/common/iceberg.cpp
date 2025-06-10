#include "duckdb.hpp"
#include "iceberg_metadata.hpp"
#include "avro_scan.hpp"
#include "iceberg_utils.hpp"
#include "metadata/iceberg_manifest.hpp"
#include "metadata/iceberg_manifest_list.hpp"
#include "manifest_reader.hpp"
#include "catalog_utils.hpp"

namespace duckdb {

IcebergTable::IcebergTable(const IcebergSnapshot &snapshot_p) : snapshot(snapshot_p) {
}

IcebergTable IcebergTable::Load(const string &iceberg_path, const IcebergTableMetadata &metadata,
                                const IcebergSnapshot &snapshot, ClientContext &context,
                                const IcebergOptions &options) {
	IcebergTable ret(snapshot);
	ret.path = iceberg_path;

	//! Set up the manifest + manifest entry readers
	auto manifest_list = make_uniq<ManifestListReader>(metadata.iceberg_version);
	auto manifest_file_reader = make_uniq<ManifestFileReader>(metadata.iceberg_version, false);

	auto &fs = FileSystem::GetFileSystem(context);
	auto manifest_list_full_path = options.allow_moved_paths
	                                   ? IcebergUtils::GetFullPath(iceberg_path, snapshot.manifest_list, fs)
	                                   : snapshot.manifest_list;
	auto scan = make_uniq<AvroScan>("IcebergManifestList", context, manifest_list_full_path);
	manifest_list->Initialize(std::move(scan));

	//! TODO: replace the vector with IcebergManifestList
	vector<IcebergManifest> all_manifests;
	while (!manifest_list->Finished()) {
		manifest_list->Read(STANDARD_VECTOR_SIZE, all_manifests);
	}

	for (auto &manifest : all_manifests) {
		auto manifest_entry_full_path = options.allow_moved_paths
		                                    ? IcebergUtils::GetFullPath(iceberg_path, manifest.manifest_path, fs)
		                                    : manifest.manifest_path;
		auto scan = make_uniq<AvroScan>("IcebergManifest", context, manifest_entry_full_path);
		manifest_file_reader->Initialize(std::move(scan));
		manifest_file_reader->SetSequenceNumber(manifest.sequence_number);
		manifest_file_reader->SetPartitionSpecID(manifest.partition_spec_id);

		//! TODO: replace the vector with IcebergManifestFile
		vector<IcebergManifestEntry> data_files;
		while (!manifest_file_reader->Finished()) {
			manifest_file_reader->Read(STANDARD_VECTOR_SIZE, data_files);
		}
		IcebergTableEntry table_entry;
		table_entry.manifest = std::move(manifest);
		table_entry.manifest_entries = std::move(data_files);
		ret.entries.push_back(table_entry);
	}
	return ret;
}

} // namespace duckdb
