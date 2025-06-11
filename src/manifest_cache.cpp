#include "manifest_cache.hpp"
#include "iceberg_utils.hpp"
#include "avro_scan.hpp"
#include "manifest_reader.hpp"
#include "metadata/iceberg_table_metadata.hpp"

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

IcebergManifestList &IcebergManifestListCache::AddManifestListMutable(IcebergManifestList &&input) {
	lock_guard<mutex> guard(l);
	auto path = input.path;
	auto res = entries.emplace(path, std::move(input));
	D_ASSERT(res.second);
	return res.first->second;
}

const IcebergManifestList &IcebergManifestListCache::GetOrCreateFromPath(const IcebergTableMetadata &metadata,
                                                                         ClientContext &context,
                                                                         const string &full_path) const {
	auto cached_result = GetManifestList(full_path);
	//! If the result is cached, return it directly
	if (cached_result) {
		return *cached_result;
	}
	IcebergManifestList result(full_path);

	//! Read the manifest list
	auto manifest_list = make_uniq<ManifestListReader>(metadata.iceberg_version);
	auto scan = make_uniq<AvroScan>("IcebergManifestList", context, full_path);
	manifest_list->Initialize(std::move(scan));
	while (!manifest_list->Finished()) {
		manifest_list->Read(STANDARD_VECTOR_SIZE, result.manifests);
	}

	//! Add it to the cache
	return AddManifestList(std::move(result));
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

IcebergManifestFile &IcebergManifestFileCache::AddManifestFileMutable(IcebergManifestFile &&input) {
	lock_guard<mutex> guard(l);
	auto path = input.path;
	auto res = entries.emplace(path, std::move(input));
	D_ASSERT(res.second);
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
