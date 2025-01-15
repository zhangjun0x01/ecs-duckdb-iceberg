#include "storage/ic_transaction.hpp"
#include "storage/ic_catalog.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"

namespace duckdb {

IBTransaction::IBTransaction(UCCatalog &ic_catalog, TransactionManager &manager, ClientContext &context)
    : Transaction(manager, context), access_mode(ic_catalog.access_mode) {
	//	connection = UCConnection::Open(ic_catalog.path);
}

IBTransaction::~IBTransaction() = default;

void IBTransaction::Start() {
	transaction_state = UCTransactionState::TRANSACTION_NOT_YET_STARTED;
}
void IBTransaction::Commit() {
	if (transaction_state == UCTransactionState::TRANSACTION_STARTED) {
		transaction_state = UCTransactionState::TRANSACTION_FINISHED;
		//		connection.Execute("COMMIT");
	}
}
void IBTransaction::Rollback() {
	if (transaction_state == UCTransactionState::TRANSACTION_STARTED) {
		transaction_state = UCTransactionState::TRANSACTION_FINISHED;
		//		connection.Execute("ROLLBACK");
	}
}

// UCConnection &IBTransaction::GetConnection() {
//	if (transaction_state == UCTransactionState::TRANSACTION_NOT_YET_STARTED) {
//		transaction_state = UCTransactionState::TRANSACTION_STARTED;
//		string query = "START TRANSACTION";
//		if (access_mode == AccessMode::READ_ONLY) {
//			query += " READ ONLY";
//		}
////		conne/**/ction.Execute(query);
//	}
//	return connection;
//}

// unique_ptr<UCResult> IBTransaction::Query(const string &query) {
//	if (transaction_state == UCTransactionState::TRANSACTION_NOT_YET_STARTED) {
//		transaction_state = UCTransactionState::TRANSACTION_STARTED;
//		string transaction_start = "START TRANSACTION";
//		if (access_mode == AccessMode::READ_ONLY) {
//			transaction_start += " READ ONLY";
//		}
//		connection.Query(transaction_start);
//		return connection.Query(query);
//	}
//	return connection.Query(query);
//}

IBTransaction &IBTransaction::Get(ClientContext &context, Catalog &catalog) {
	return Transaction::Get(context, catalog).Cast<IBTransaction>();
}

} // namespace duckdb
