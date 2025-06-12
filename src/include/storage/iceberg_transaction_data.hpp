#pragma once

#include "metadata/iceberg_manifest_list.hpp"
#include "metadata/iceberg_manifest.hpp"
#include "metadata/iceberg_snapshot.hpp"

#include "duckdb/main/client_context.hpp"

namespace duckdb {

struct IcebergTableInformation;

struct IcebergTransactionData {
public:
	IcebergTransactionData(IcebergManifestFile &&manifest_file, IcebergManifestList &&manifest_list,
	                       IcebergSnapshot &&snapshot)
	    : manifest_file(std::move(manifest_file)), manifest_list(std::move(manifest_list)),
	      snapshot(std::move(snapshot)) {
	}
	static unique_ptr<IcebergTransactionData> Create(ClientContext &context, IcebergTableInformation &table_info);

public:
	IcebergManifestFile manifest_file;
	IcebergManifestList manifest_list;
	IcebergSnapshot snapshot;
};

} // namespace duckdb
