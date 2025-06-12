#pragma once

#include "storage/iceberg_table_update.hpp"

#include "metadata/iceberg_manifest.hpp"
#include "metadata/iceberg_manifest_list.hpp"
#include "metadata/iceberg_snapshot.hpp"

namespace duckdb {

struct IcebergTableInformation;

struct IcebergAddSnapshot : public IcebergTableUpdate {
	static constexpr const IcebergTableUpdateType TYPE = IcebergTableUpdateType::ADD_SNAPSHOT;

public:
	IcebergAddSnapshot(IcebergTableInformation &table_info, IcebergManifestFile &&manifest_file,
	                   IcebergManifestList &&manifest_list, IcebergSnapshot &&snapshot);

public:
	rest_api_objects::TableUpdate CreateUpdate(DatabaseInstance &db, ClientContext &context) override;

private:
	void WriteManifestFile(CopyFunction &copy_function, DatabaseInstance &db, ClientContext &context);
	void WriteManifestList(CopyFunction &copy_function, DatabaseInstance &db, ClientContext &context);

public:
	IcebergManifestFile manifest_file;
	IcebergManifestList manifest_list;
	IcebergSnapshot snapshot;
};

} // namespace duckdb
