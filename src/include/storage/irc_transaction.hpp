
#pragma once

#include "duckdb/transaction/transaction.hpp"
#include "storage/irc_schema_set.hpp"
#include "manifest_cache.hpp"

namespace duckdb {
class IRCatalog;
class IRCSchemaEntry;
class ICTableEntry;

enum class IRCTransactionState { TRANSACTION_NOT_YET_STARTED, TRANSACTION_STARTED, TRANSACTION_FINISHED };

class IRCTransaction : public Transaction {
public:
	IRCTransaction(IRCatalog &ic_catalog, TransactionManager &manager, ClientContext &context);
	~IRCTransaction() override;

public:
	void Start();
	void Commit();
	void Rollback();
	static IRCTransaction &Get(ClientContext &context, Catalog &catalog);
	AccessMode GetAccessMode() const {
		return access_mode;
	}
	IRCSchemaSet &GetSchemas() {
		return schemas;
	}

public:
	IRCSchemaSet schemas;

	IcebergManifestListCache manifest_list_cache;
	IcebergManifestFileCache manifest_file_cache;

private:
	IRCTransactionState transaction_state;
	AccessMode access_mode;
};

} // namespace duckdb
