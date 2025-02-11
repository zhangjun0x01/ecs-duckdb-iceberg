
#pragma once

#include "duckdb/transaction/transaction_manager.hpp"
#include "storage/irc_catalog.hpp"
#include "storage/irc_transaction.hpp"

namespace duckdb {

class ICTransactionManager : public TransactionManager {
public:
	ICTransactionManager(AttachedDatabase &db_p, IRCatalog &ic_catalog);

	Transaction &StartTransaction(ClientContext &context) override;
	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
	void RollbackTransaction(Transaction &transaction) override;

	void Checkpoint(ClientContext &context, bool force = false) override;

private:
	IRCatalog &ic_catalog;
	mutex transaction_lock;
	reference_map_t<Transaction, unique_ptr<IRCTransaction>> transactions;
};

} // namespace duckdb
