#pragma once

#include "metadata/iceberg_manifest_list.hpp"
#include "metadata/iceberg_manifest.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/file_system.hpp"
#include "iceberg_options.hpp"

namespace duckdb {

class ManifestFileReader;
struct IcebergTableMetadata;

class IcebergManifestListCache {
public:
	optional_ptr<const IcebergManifestList> GetManifestList(const string &path) const;
	const IcebergManifestList &AddManifestList(IcebergManifestList &&manifest_list) const;
	const IcebergManifestList &GetOrCreateFromPath(const IcebergTableMetadata &metadata, ClientContext &context,
	                                               const string &path) const;

private:
	//! Map from path to manifest list
	mutable case_insensitive_map_t<IcebergManifestList> entries;
	mutable std::mutex l;
};

class IcebergManifestFileCache {
public:
	optional_ptr<const IcebergManifestFile> GetManifestFile(const string &path) const;
	const IcebergManifestFile &AddManifestFile(IcebergManifestFile &&manifest_file) const;
	const IcebergManifestFile &GetOrCreateFromManifest(ManifestFileReader &manifest_reader,
	                                                   const IcebergOptions &options, const string &iceberg_path,
	                                                   const IcebergManifest &manifest, ClientContext &context,
	                                                   FileSystem &fs) const;

private:
	//! Map from path to manifest file
	mutable case_insensitive_map_t<IcebergManifestFile> entries;
	mutable std::mutex l;
};

} // namespace duckdb
