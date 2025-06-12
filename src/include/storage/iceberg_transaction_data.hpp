#pragma once

#include "metadata/iceberg_manifest_list.hpp"
#include "metadata/iceberg_manifest.hpp"
#include "metadata/iceberg_snapshot.hpp"
#include "rest_catalog/objects/add_snapshot_update.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/function/copy_function.hpp"

namespace duckdb {

struct IcebergTableInformation;

struct IcebergTransactionData {
public:
	IcebergTransactionData(IcebergTableInformation &table_info, IcebergManifestFile &&manifest_file,
	                       IcebergManifestList &&manifest_list, IcebergSnapshot &&snapshot)
	    : table_info(table_info), manifest_file(std::move(manifest_file)), manifest_list(std::move(manifest_list)),
	      snapshot(std::move(snapshot)) {
	}

public:
	static unique_ptr<IcebergTransactionData> Create(ClientContext &context, IcebergTableInformation &table_info);

public:
	rest_api_objects::AddSnapshotUpdate CreateSnapshotUpdate(DatabaseInstance &db);

private:
	void WriteManifestFile(CopyFunction &copy_function, DatabaseInstance &db);
	void WriteManifestList(CopyFunction &copy_function, DatabaseInstance &db);

public:
	IcebergTableInformation &table_info;

	IcebergManifestFile manifest_file;
	IcebergManifestList manifest_list;
	IcebergSnapshot snapshot;
};

} // namespace duckdb
