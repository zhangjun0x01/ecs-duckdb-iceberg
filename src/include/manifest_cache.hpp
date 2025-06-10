#pragma once

#include "metadata/iceberg_manifest_list.hpp"
#include "metadata/iceberg_manifest.hpp"

namespace duckdb {

class IcebergManifestListCache {
public:
	IcebergManifestListCache() {
	}

public:
	optional_ptr<const IcebergManifestList> GetManifestList(const string &path) const;
	const IcebergManifestList &AddManifestList(IcebergManifestList &&manifest_list) const;

private:
	//! Map from path to manifest list
	mutable case_insensitive_map_t<IcebergManifestList> manifest_list;
	mutable std::mutex lock;
};

class IcebergManifestFileCache {
public:
	IcebergManifestFileCache() {
	}

public:
	optional_ptr<const IcebergManifestFile> GetManifestFile(const string &path) const;
	const IcebergManifestFile &AddManifestFile(IcebergManifestFile &&manifest_file) const;

private:
	//! Map from path to manifest file
	case_insensitive_map_t<IcebergManifestFile> manifest_list;
	std::mutex lock;
};

} // namespace duckdb
