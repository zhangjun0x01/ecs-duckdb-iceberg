#include "duckdb.hpp"
#include "iceberg_metadata.hpp"
#include "iceberg_manifest.hpp"
#include "iceberg_utils.hpp"
#include "iceberg_types.hpp"
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

	vector<IcebergManifest> manifests;
	unique_ptr<ManifestReader> manifest_reader;
	unique_ptr<ManifestReader> manifest_entry_reader;
	manifest_reader_manifest_producer manifest_producer = nullptr;
	manifest_reader_manifest_entry_producer entry_producer = nullptr;

	//! Set up the manifest + manifest entry readers
	manifest_entry_reader =
	    make_uniq<ManifestReader>(IcebergManifestEntryV1::PopulateNameMapping, IcebergManifestEntryV1::VerifySchema);
	manifest_reader =
	    make_uniq<ManifestReader>(IcebergManifestV1::PopulateNameMapping, IcebergManifestV1::VerifySchema);
	manifest_producer = IcebergManifestV1::ProduceEntries;
	entry_producer = IcebergManifestEntryV1::ProduceEntries;

	auto &fs = FileSystem::GetFileSystem(context);
	auto manifest_list_full_path = options.allow_moved_paths
	                                   ? IcebergUtils::GetFullPath(iceberg_path, snapshot.manifest_list, fs)
	                                   : snapshot.manifest_list;
	auto scan = make_uniq<AvroScan>("IcebergManifestList", context, manifest_list_full_path);
	manifest_reader->Initialize(std::move(scan));

	vector<IcebergManifest> all_manifests;
	while (!manifest_reader->Finished()) {
		manifest_reader->ReadEntries(STANDARD_VECTOR_SIZE,
		                             [&all_manifests, manifest_producer](DataChunk &chunk, idx_t offset, idx_t count,
		                                                                 const ManifestReaderInput &input) {
			                             return manifest_producer(chunk, offset, count, input, all_manifests);
		                             });
	}

	for (auto &manifest : all_manifests) {
		auto manifest_entry_full_path = options.allow_moved_paths
		                                    ? IcebergUtils::GetFullPath(iceberg_path, manifest.manifest_path, fs)
		                                    : manifest.manifest_path;
		auto scan = make_uniq<AvroScan>("IcebergManifest", context, manifest_entry_full_path);
		manifest_entry_reader->Initialize(std::move(scan));
		manifest_entry_reader->SetSequenceNumber(manifest.sequence_number);
		manifest_entry_reader->SetPartitionSpecID(manifest.partition_spec_id);

		vector<IcebergManifestEntry> data_files;
		while (!manifest_entry_reader->Finished()) {
			manifest_entry_reader->ReadEntries(
			    STANDARD_VECTOR_SIZE, [&data_files, &entry_producer](DataChunk &chunk, idx_t offset, idx_t count,
			                                                         const ManifestReaderInput &input) {
				    return entry_producer(chunk, offset, count, input, data_files);
			    });
		}
		IcebergTableEntry table_entry;
		table_entry.manifest = std::move(manifest);
		table_entry.manifest_entries = std::move(data_files);
		ret.entries.push_back(table_entry);
	}
	return ret;
}

} // namespace duckdb
