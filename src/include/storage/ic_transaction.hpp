
#pragma once

#include "duckdb/transaction/transaction.hpp"

namespace duckdb {
class UCCatalog;
class IBSchemaEntry;
class UCTableEntry;

enum class UCTransactionState { TRANSACTION_NOT_YET_STARTED, TRANSACTION_STARTED, TRANSACTION_FINISHED };

class IBTransaction : public Transaction {
public:
	IBTransaction(UCCatalog &ic_catalog, TransactionManager &manager, ClientContext &context);
	~IBTransaction() override;

	void Start();
	void Commit();
	void Rollback();

	//	UCConnection &GetConnection();
	//	unique_ptr<UCResult> Query(const string &query);
	static IBTransaction &Get(ClientContext &context, Catalog &catalog);
	AccessMode GetAccessMode() const {
		return access_mode;
	}

private:
	//	UCConnection connection;
	UCTransactionState transaction_state;
	AccessMode access_mode;
};

} // namespace duckdb
