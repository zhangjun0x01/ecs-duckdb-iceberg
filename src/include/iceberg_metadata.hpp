//===----------------------------------------------------------------------===//
//                         DuckDB
//
// iceberg_metadata.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "yyjson.hpp"
#include "iceberg_types.hpp"
#include "iceberg_options.hpp"
#include "duckdb/common/open_file_info.hpp"
#include "duckdb/function/table_function.hpp"

#include "rest_catalog/objects/table_metadata.hpp"

#include "metadata/iceberg_snapshot.hpp"
#include "metadata/iceberg_table_metadata.hpp"
#include "metadata/iceberg_partition_spec.hpp"
#include "metadata/iceberg_table_schema.hpp"
#include "metadata/iceberg_field_mapping.hpp"

using namespace duckdb_yyjson;

namespace duckdb {

// struct IcebergScanData {
// public:
//	IcebergScanData(
//		optional_ptr<const IcebergSnapshot> snapshot,
//		const &IcebergTableSchema &schema,
//		const &IcebergTableMetadata &metadata
//	);
// public:
//	//! nullptr if no snapshot exists (empty table)
//	optional_ptr<const IcebergSnapshot> snapshot;
//	const IcebergTableSchema &schema;
//	const IcebergTableMetadata &metadata;
//};

struct IcebergScanInfo : public TableFunctionInfo {
public:
	IcebergScanInfo(const string &metadata_path, IcebergTableMetadata &metadata, optional_ptr<IcebergSnapshot> snapshot,
	                IcebergTableSchema &schema)
	    : metadata_path(metadata_path), metadata(metadata), snapshot(snapshot), schema(schema) {
	}
	IcebergScanInfo(const string &metadata_path, unique_ptr<IcebergTableMetadata> owned_metadata_p,
	                optional_ptr<IcebergSnapshot> snapshot, IcebergTableSchema &schema)
	    : metadata_path(metadata_path), owned_metadata(std::move(owned_metadata_p)), metadata(*owned_metadata),
	      snapshot(snapshot), schema(schema) {
	}

public:
	string metadata_path;
	unique_ptr<IcebergTableMetadata> owned_metadata;
	IcebergTableMetadata &metadata;
	optional_ptr<IcebergSnapshot> snapshot;
	IcebergTableSchema &schema;
};

//! ------------- ICEBERG_METADATA TABLE FUNCTION -------------

struct IcebergTable {
public:
	IcebergTable(const IcebergSnapshot &snapshot);

public:
	//! Loads all(!) metadata of into IcebergTable object
	static IcebergTable Load(const string &iceberg_path, const IcebergTableMetadata &metadata,
	                         const IcebergSnapshot &snapshot, ClientContext &context, const IcebergOptions &options);

public:
	//! Returns all paths to be scanned for the IcebergManifestContentType
	template <IcebergManifestContentType TYPE>
	vector<string> GetPaths() {
		vector<string> ret;
		for (auto &entry : entries) {
			if (entry.manifest.content != TYPE) {
				continue;
			}
			for (auto &manifest_entry : entry.manifest_entries) {
				if (manifest_entry.status == IcebergManifestEntryStatusType::DELETED) {
					continue;
				}
				ret.push_back(manifest_entry.file_path);
			}
		}
		return ret;
	}
	vector<IcebergManifestEntry> GetAllPaths() {
		vector<IcebergManifestEntry> ret;
		for (auto &entry : entries) {
			for (auto &manifest_entry : entry.manifest_entries) {
				if (manifest_entry.status == IcebergManifestEntryStatusType::DELETED) {
					continue;
				}
				ret.push_back(manifest_entry);
			}
		}
		return ret;
	}

	//! The snapshot of this table
	const IcebergSnapshot &snapshot;
	//! The entries (manifests) of this table
	vector<IcebergTableEntry> entries;

protected:
	string path;
};

} // namespace duckdb
