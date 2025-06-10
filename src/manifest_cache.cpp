#include "manifest_cache.hpp"

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

} // namespace duckdb
