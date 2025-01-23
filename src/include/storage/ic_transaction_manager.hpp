
#pragma once

#include "duckdb/transaction/transaction_manager.hpp"
#include "storage/ic_catalog.hpp"
#include "storage/ic_transaction.hpp"

namespace duckdb {

class ICTransactionManager : public TransactionManager {
public:
	ICTransactionManager(AttachedDatabase &db_p, ICCatalog &ic_catalog);

	Transaction &StartTransaction(ClientContext &context) override;
	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
	void RollbackTransaction(Transaction &transaction) override;

	void Checkpoint(ClientContext &context, bool force = false) override;

private:
	ICCatalog &ic_catalog;
	mutex transaction_lock;
	reference_map_t<Transaction, unique_ptr<ICTransaction>> transactions;
};

} // namespace duckdb
