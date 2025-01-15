
#pragma once

#include "duckdb/transaction/transaction_manager.hpp"
#include "storage/ic_catalog.hpp"
#include "storage/ic_transaction.hpp"

namespace duckdb {

class UCTransactionManager : public TransactionManager {
public:
	UCTransactionManager(AttachedDatabase &db_p, UCCatalog &ic_catalog);

	Transaction &StartTransaction(ClientContext &context) override;
	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
	void RollbackTransaction(Transaction &transaction) override;

	void Checkpoint(ClientContext &context, bool force = false) override;

private:
	UCCatalog &ic_catalog;
	mutex transaction_lock;
	reference_map_t<Transaction, unique_ptr<UCTransaction>> transactions;
};

} // namespace duckdb
