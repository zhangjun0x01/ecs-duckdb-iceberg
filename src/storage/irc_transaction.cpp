#include "storage/irc_transaction.hpp"
#include "storage/irc_catalog.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"

namespace duckdb {

IRCTransaction::IRCTransaction(IRCatalog &ic_catalog, TransactionManager &manager, ClientContext &context)
    : Transaction(manager, context), db(*context.db), access_mode(ic_catalog.access_mode), schemas(ic_catalog) {
}

IRCTransaction::~IRCTransaction() = default;

void IRCTransaction::MarkTableAsDirty(const ICTableEntry &table) {
	dirty_tables.insert(&table);
}

void IRCTransaction::Start() {
}

void IRCTransaction::Commit() {
	//! TODO: for every dirty table:
	// - create the manifest list
	// - create the manifest file
	// - create a rest_api_objects::Snapshot from the IcebergSnapshot
	// - create a rest_api_objects::AddSnapshotUpdate, containing the snapshot ^
	// - serialize this to JSON
	// - hit the REST API (POST to '/v1/{prefix}/transactions/commit'), sending along this payload
	// - profit ???
}

void IRCTransaction::CleanupFiles() {
	// remove any files that were written
	auto &fs = FileSystem::GetFileSystem(db);
	for (auto &table : dirty_tables) {
		auto &table_info = table->table_info;
		auto &transaction_data = *table_info.transaction_data;

		auto &data_files = transaction_data.manifest_file.data_files;
		for (auto &data_file : data_files) {
			fs.TryRemoveFile(data_file.file_path);
		}
	}
}

void IRCTransaction::Rollback() {
	CleanupFiles();
}

IRCTransaction &IRCTransaction::Get(ClientContext &context, Catalog &catalog) {
	return Transaction::Get(context, catalog).Cast<IRCTransaction>();
}

} // namespace duckdb
