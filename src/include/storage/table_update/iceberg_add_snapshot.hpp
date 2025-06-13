#pragma once

#include "storage/iceberg_table_update.hpp"

#include "metadata/iceberg_manifest.hpp"
#include "metadata/iceberg_manifest_list.hpp"
#include "metadata/iceberg_snapshot.hpp"

#include "duckdb/common/vector.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

struct IcebergTableInformation;

struct IcebergAddSnapshot : public IcebergTableUpdate {
	static constexpr const IcebergTableUpdateType TYPE = IcebergTableUpdateType::ADD_SNAPSHOT;

public:
	IcebergAddSnapshot(IcebergTableInformation &table_info, IcebergManifestFile &&manifest_file,
	                   IcebergManifestList &&manifest_list, IcebergSnapshot &&snapshot);

public:
	rest_api_objects::TableUpdate CreateUpdate(DatabaseInstance &db, ClientContext &context) override;

public:
	IcebergManifestFile manifest_file;
	IcebergManifestList manifest_list;
	IcebergSnapshot snapshot;
};

} // namespace duckdb
