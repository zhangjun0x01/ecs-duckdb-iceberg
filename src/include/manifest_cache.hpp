#pragma once

#include "metadata/iceberg_manifest_list.hpp"
#include "metadata/iceberg_manifest.hpp"
#include "duckdb/common/mutex.hpp"

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
	mutable case_insensitive_map_t<IcebergManifestList> entries;
	mutable std::mutex l;
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
	mutable case_insensitive_map_t<IcebergManifestFile> entries;
	mutable std::mutex l;
};

} // namespace duckdb
