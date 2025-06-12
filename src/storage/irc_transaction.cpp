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

	rest_api_objects::CommitTransactionRequest transaction;
	for (auto &table : dirty_tables) {
		rest_api_objects::CommitTableRequest table_change;
		table_change.identifier._namespace.value.push_back(table->ParentSchema().name);
		table_change.identifier.name = table->name;
		table_change.has_identifier = true;

		auto &transaction_data = *table->table_info.transaction_data;
		for (auto &update : transaction_data.updates) {
			auto table_update = update->CreateUpdate(db, *context);
			table_change.updates.push_back(std::move(table_update));
		}
		transaction.table_changes.push_back(std::move(table_change));
	}

	// - serialize this to JSON
	// - hit the REST API (POST to '/v1/{prefix}/transactions/commit'), sending along this payload
	// - profit ???
	throw NotImplementedException("Serialize CommitTransactionRequest to JSON");
	context->transaction.Commit();
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
