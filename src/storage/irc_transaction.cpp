#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"

#include "storage/irc_transaction.hpp"
#include "storage/irc_catalog.hpp"
#include "storage/table_update/iceberg_add_snapshot.hpp"

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
	Connection temp_con(db);
	auto &context = temp_con.context;
	context->transaction.BeginTransaction();

	for (auto &table : dirty_tables) {
		auto &transaction_data = *table->table_info.transaction_data;
		for (auto &update : transaction_data.updates) {
			auto table_update = update->CreateUpdate(db, *context);
			// - serialize this to JSON
			// - hit the REST API (POST to '/v1/{prefix}/transactions/commit'), sending along this payload
			// - profit ???
		}
	}
	context->transaction.ClearTransaction();
}

void IRCTransaction::CleanupFiles() {
	// remove any files that were written
	auto &fs = FileSystem::GetFileSystem(db);
	for (auto &table : dirty_tables) {
		auto &transaction_data = *table->table_info.transaction_data;
		for (auto &update : transaction_data.updates) {
			if (update->type != IcebergTableUpdateType::ADD_SNAPSHOT) {
				continue;
			}
			auto &add_snapshot = update->Cast<IcebergAddSnapshot>();
			auto &data_files = add_snapshot.manifest_file.data_files;
			for (auto &data_file : data_files) {
				fs.TryRemoveFile(data_file.file_path);
			}
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
