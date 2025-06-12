#include "storage/irc_transaction.hpp"
#include "storage/irc_catalog.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"

namespace duckdb {

IRCTransaction::IRCTransaction(IRCatalog &ic_catalog, TransactionManager &manager, ClientContext &context)
    : Transaction(manager, context), schemas(ic_catalog), access_mode(ic_catalog.access_mode) {
}

IRCTransaction::~IRCTransaction() = default;

void IRCTransaction::Start() {
}

void IRCTransaction::Commit() {
}

void IRCTransaction::Rollback() {
	throw InternalException("ROLLBACK");
}

IRCTransaction &IRCTransaction::Get(ClientContext &context, Catalog &catalog) {
	return Transaction::Get(context, catalog).Cast<IRCTransaction>();
}

} // namespace duckdb
