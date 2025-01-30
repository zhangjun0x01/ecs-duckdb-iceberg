
#pragma once

#include "duckdb/transaction/transaction.hpp"

namespace duckdb {
class ICRCatalog;
class ICSchemaEntry;
class ICTableEntry;

enum class ICTransactionState { TRANSACTION_NOT_YET_STARTED, TRANSACTION_STARTED, TRANSACTION_FINISHED };

class ICTransaction : public Transaction {
public:
	ICTransaction(ICRCatalog &ic_catalog, TransactionManager &manager, ClientContext &context);
	~ICTransaction() override;

	void Start();
	void Commit();
	void Rollback();

	static ICTransaction &Get(ClientContext &context, Catalog &catalog);
	AccessMode GetAccessMode() const {
		return access_mode;
	}

private:
	ICTransactionState transaction_state;
	AccessMode access_mode;
};

} // namespace duckdb
