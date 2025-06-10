#include "manifest_cache.hpp"

namespace duckdb {

optional_ptr<const IcebergManifestList> IcebergManifestListCache::GetManifestList(const string &path) const {
	lock_guard<mutex> guard(lock);
	auto it = manifest_list.find(path);
	if (it == manifest_list.end()) {
		return nullptr;
	}
	return it->second;
}

const IcebergManifestList &IcebergManifestListCache::AddManifestList(IcebergManifestList &&input) {
	lock_guard<mutex> guard(lock);
	auto path = input.path;
	//! The list could already be added, but that's okay, they're immutable anyways, so they should be the same
	auto res = manifest_list.emplace(path, std::move(input));
	return *res.first;
}

optional_ptr<const IcebergManifestFile> IcebergManifestFileCache::GetManifestFile(const string &path) const {
	lock_guard<mutex> guard(lock);
	auto it = manifest_file.find(path);
	if (it == manifest_file.end()) {
		return nullptr;
	}
	return it->second;
}

const IcebergManifestFile &IcebergManifestFileCache::AddManifestFile(IcebergManifestFile &&input) {
	lock_guard<mutex> guard(lock);
	auto path = input.path;
	//! The file could already be added, but that's okay, they're immutable anyways, so they should be the same
	auto res = manifest_file.emplace(path, std::move(input));
	return *res.first;
}

} // namespace duckdb
