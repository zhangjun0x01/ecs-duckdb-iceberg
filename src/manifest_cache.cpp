#include "manifest_cache.hpp"
#include "iceberg_utils.hpp"
#include "avro_scan.hpp"
#include "manifest_reader.hpp"

namespace duckdb {

optional_ptr<const IcebergManifestList> IcebergManifestListCache::GetManifestList(const string &path) const {
	lock_guard<mutex> guard(l);
	auto it = entries.find(path);
	if (it == entries.end()) {
		return nullptr;
	}
	return it->second;
}

const IcebergManifestList &IcebergManifestListCache::AddManifestList(IcebergManifestList &&input) const {
	lock_guard<mutex> guard(l);
	auto path = input.path;
	//! The list could already be added, but that's okay, they're immutable anyways, so they should be the same
	auto res = entries.emplace(path, std::move(input));
	return res.first->second;
}

optional_ptr<const IcebergManifestFile> IcebergManifestFileCache::GetManifestFile(const string &path) const {
	lock_guard<mutex> guard(l);
	auto it = entries.find(path);
	if (it == entries.end()) {
		return nullptr;
	}
	return it->second;
}

const IcebergManifestFile &IcebergManifestFileCache::AddManifestFile(IcebergManifestFile &&input) const {
	lock_guard<mutex> guard(l);
	auto path = input.path;
	//! The file could already be added, but that's okay, they're immutable anyways, so they should be the same
	auto res = entries.emplace(path, std::move(input));
	return res.first->second;
}

const IcebergManifestFile &
IcebergManifestFileCache::GetOrCreateFromManifest(ManifestFileReader &manifest_reader, const IcebergOptions &options,
                                                  const string &iceberg_path, const IcebergManifest &manifest,
                                                  ClientContext &context, FileSystem &fs) const {
	auto full_path = options.allow_moved_paths ? IcebergUtils::GetFullPath(iceberg_path, manifest.manifest_path, fs)
	                                           : manifest.manifest_path;

	auto cached_result = GetManifestFile(full_path);

	if (cached_result) {
		return *cached_result;
	}

	auto scan = make_uniq<AvroScan>("IcebergManifest", context, full_path);

	manifest_reader.Initialize(std::move(scan));
	manifest_reader.SetSequenceNumber(manifest.sequence_number);
	manifest_reader.SetPartitionSpecID(manifest.partition_spec_id);

	IcebergManifestFile result(full_path);
	while (!manifest_reader.Finished()) {
		manifest_reader.Read(STANDARD_VECTOR_SIZE, result.data_files);
	}

	return AddManifestFile(std::move(result));
}

} // namespace duckdb
