#include "storage/table_update/iceberg_add_snapshot.hpp"
#include "storage/irc_table_set.hpp"

#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/caching_file_system.hpp"

namespace duckdb {

IcebergAddSnapshot::IcebergAddSnapshot(IcebergTableInformation &table_info, IcebergManifestFile &&manifest_file,
                                       IcebergManifestList &&manifest_list, IcebergSnapshot &&snapshot)
    : IcebergTableUpdate(IcebergTableUpdateType::ADD_SNAPSHOT, table_info), manifest_file(std::move(manifest_file)),
      manifest_list(std::move(manifest_list)), snapshot(std::move(snapshot)) {
}

rest_api_objects::TableUpdate IcebergAddSnapshot::CreateUpdate(DatabaseInstance &db, ClientContext &context) {
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	auto &schema = system_catalog.GetSchema(data, DEFAULT_SCHEMA);
	auto avro_copy_p = schema.GetEntry(data, CatalogType::COPY_FUNCTION_ENTRY, "avro");
	D_ASSERT(avro_copy_p);
	auto &avro_copy = avro_copy_p->Cast<CopyFunctionCatalogEntry>().function;

	auto manifest_length = manifest_file::WriteToFile(table_info, manifest_file, avro_copy, db, context);
	D_ASSERT(manifest_list.manifests.size() == 1);
	auto &manifest = manifest_list.manifests[0];
	manifest.manifest_length = manifest_length;

	manifest_list::WriteToFile(manifest_list, avro_copy, db, context);

	rest_api_objects::TableUpdate table_update;
	table_update.has_add_snapshot_update = true;

	auto &update = table_update.add_snapshot_update;
	update.base_update.action = "add-snapshot";
	update.has_action = true;
	update.action = "add-snapshot";
	update.snapshot = snapshot.ToRESTObject();
	return table_update;
}

} // namespace duckdb
