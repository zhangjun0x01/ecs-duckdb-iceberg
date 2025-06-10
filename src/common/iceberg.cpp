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

unique_ptr<IcebergTable> IcebergTable::Load(const string &iceberg_path, const IcebergTableMetadata &metadata,
                                            const IcebergSnapshot &snapshot, ClientContext &context,
                                            const IcebergOptions &options) {
	auto ret = make_uniq<IcebergTable>(snapshot);
	ret->path = iceberg_path;

	//! Set up the manifest + manifest entry readers
	auto manifest_file_reader = make_uniq<ManifestFileReader>(metadata.iceberg_version, false);

	auto &fs = FileSystem::GetFileSystem(context);

	auto &manifest_list_cache = ret->manifest_list_cache;
	auto &manifest_file_cache = ret->manifest_file_cache;

	auto manifest_list_full_path = options.allow_moved_paths
	                                   ? IcebergUtils::GetFullPath(iceberg_path, snapshot.manifest_list, fs)
	                                   : snapshot.manifest_list;
	auto &manifest_list = manifest_list_cache.GetOrCreateFromPath(metadata, context, manifest_list_full_path);

	for (auto &manifest : manifest_list.manifests) {
		auto &manifest_file = manifest_file_cache.GetOrCreateFromManifest(*manifest_file_reader, options, iceberg_path,
		                                                                  manifest, context, fs);
		IcebergTableEntry table_entry(manifest, manifest_file);
		ret->entries.push_back(table_entry);
	}
	return ret;
}

} // namespace duckdb
