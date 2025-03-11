#include "storage/irc_catalog.hpp"
#include "storage/irc_schema_entry.hpp"
#include "storage/irc_table_entry.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "catalog_api.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"


namespace duckdb {

ICTableEntry::ICTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info)
    : TableCatalogEntry(catalog, schema, info) {
	this->internal = false;
}

ICTableEntry::ICTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, ICTableInfo &info)
    : TableCatalogEntry(catalog, schema, *info.create_info) {
	this->internal = false;
}

unique_ptr<BaseStatistics> ICTableEntry::GetStatistics(ClientContext &context, column_t column_id) {
	return nullptr;
}

void ICTableEntry::BindUpdateConstraints(Binder &binder, LogicalGet &, LogicalProjection &, LogicalUpdate &,
                                         ClientContext &) {
	throw NotImplementedException("BindUpdateConstraints");
}

TableFunction ICTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	auto &db = DatabaseInstance::GetDatabase(context);
	auto &ic_catalog = catalog.Cast<IRCatalog>();

	auto &parquet_function_set = ExtensionUtil::GetTableFunction(db, "parquet_scan");
	auto parquet_scan_function = parquet_function_set.functions.GetFunctionByArguments(context, {LogicalType::VARCHAR});

	auto &iceberg_function_set = ExtensionUtil::GetTableFunction(db, "iceberg_scan");
	auto iceberg_scan_function = iceberg_function_set.functions.GetFunctionByArguments(context, {LogicalType::VARCHAR});

	D_ASSERT(table_data);

	if (table_data->data_source_format != "ICEBERG") {
		throw NotImplementedException("Table '%s' is of unsupported format '%s', ", table_data->name,
		                              table_data->data_source_format);
	}

	auto &secret_manager = SecretManager::Get(context);
	
	// Get Credentials from IRC API
	auto table_credentials = IRCAPI::GetTableCredentials(context, ic_catalog, table_data->schema_name, table_data->name, ic_catalog.credentials);
	CreateSecretInfo info(OnCreateConflict::REPLACE_ON_CONFLICT, SecretPersistType::TEMPORARY);
	// First check if table credentials are set (possible the IC catalog does not return credentials)
	if (!table_credentials.key_id.empty()) {
		// Inject secret into secret manager scoped to this path
		info.name = "__internal_ic_" + table_data->table_id;
		info.type = "s3";
		info.provider = "config";
		info.storage_type = "memory";
		info.options = {
			{"key_id", table_credentials.key_id},
			{"secret", table_credentials.secret},
			{"session_token", table_credentials.session_token},
			{"region", table_credentials.region},
		};

		if (StringUtil::StartsWith(ic_catalog.host, "glue")) {
			auto secret_entry = IRCatalog::GetSecret(context, ic_catalog.secret_name);
			auto kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_entry->secret);
			auto region = kv_secret.TryGetValue("region").ToString();
			auto endpoint = "s3." + region + ".amazonaws.com";
			info.options["endpoint"] = endpoint;
		}

		std::string lc_storage_location;
		lc_storage_location.resize(table_data->storage_location.size());
		std::transform(table_data->storage_location.begin(), table_data->storage_location.end(), lc_storage_location.begin(), ::tolower);
		size_t metadata_pos = lc_storage_location.find("metadata");
		if (metadata_pos != std::string::npos) {
			info.scope = {lc_storage_location.substr(0, metadata_pos)};
		} else {
			throw std::runtime_error("Substring not found");
		}
		auto my_secret = secret_manager.CreateSecret(context, info);
	}

	named_parameter_map_t param_map;
	vector<LogicalType> return_types;
	vector<string> names;
	TableFunctionRef empty_ref;

	// Set the S3 path as input to table function
	vector<Value> inputs = {table_data->storage_location};

	TableFunctionBindInput bind_input(inputs, param_map, return_types, names, nullptr, nullptr, 
									  iceberg_scan_function, empty_ref);

	auto start = std::chrono::high_resolution_clock::now();
	auto table_ref = iceberg_scan_function.bind_replace(context, bind_input);
	auto stop = std::chrono::high_resolution_clock::now();
	std::chrono::duration<double> elapsed = stop - start;
	std::cout << "Elapsed time bind replace: " << elapsed.count() << " seconds\n";
	// 1) Create a Binder and bind the parser-level TableRef -> BoundTableRef
	auto binder = Binder::CreateBinder(context);
	auto bound_ref = binder->Bind(*table_ref);

	// 2) Create a logical plan from the bound reference
	unique_ptr<LogicalOperator> logical_plan = binder->CreatePlan(*bound_ref);

	// 3) Recursively search the logical plan for a LogicalGet node
	//    For a single table function, you often have just one operator: LogicalGet
	LogicalOperator *op = logical_plan.get();
	switch (op->type) {
	case LogicalOperatorType::LOGICAL_PROJECTION:
		throw NotImplementedException("Iceberg scans with point deletes not supported");
	case LogicalOperatorType::LOGICAL_GET:
		break;
	default:
		throw InternalException("Unsupported logical operator");
	}

	// 4) Access the bind_data inside LogicalGet
	auto &get = op->Cast<LogicalGet>();
	bind_data = std::move(get.bind_data);

	Printer::Print("returning parquet scan function");
	return parquet_scan_function;
}

TableStorageInfo ICTableEntry::GetStorageInfo(ClientContext &context) {
	TableStorageInfo result;
	// TODO fill info
	return result;
}

} // namespace duckdb
