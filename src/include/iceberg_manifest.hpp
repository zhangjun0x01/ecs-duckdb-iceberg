#pragma once

#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/typedefs.hpp"
#include "iceberg_types.hpp"
#include "duckdb/common/column_index.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"

namespace duckdb {

//! Iceberg Manifest scan routines

struct IcebergManifestV1 {
	static constexpr const char *NAME = "IcebergManifestList";
	static constexpr idx_t FORMAT_VERSION = 1;
	using entry_type = IcebergManifest;
	static void ProduceEntry(DataChunk &input, idx_t result_offset, const case_insensitive_map_t<ColumnIndex> &name_to_vec, entry_type &result);
	static bool VerifySchema(const case_insensitive_map_t<ColumnIndex> &name_to_vec);
	static void PopulateNameMapping(idx_t column_id, LogicalType &type, const string &name, case_insensitive_map_t<ColumnIndex> &name_to_vec);
};

struct IcebergManifestV2 {
	static constexpr const char *NAME = "IcebergManifestList";
	static constexpr idx_t FORMAT_VERSION = 2;
	using entry_type = IcebergManifest;
	static void ProduceEntry(DataChunk &input, idx_t result_offset, const case_insensitive_map_t<ColumnIndex> &name_to_vec, entry_type &result);
	static bool VerifySchema(const case_insensitive_map_t<ColumnIndex> &name_to_vec);
	static void PopulateNameMapping(idx_t column_id, LogicalType &type, const string &name, case_insensitive_map_t<ColumnIndex> &name_to_vec);
};

//! Iceberg Manifest Entry scan routines

struct IcebergManifestEntryV1 {
	static constexpr const char *NAME = "IcebergManifest";
	static constexpr idx_t FORMAT_VERSION = 1;
	using entry_type = IcebergManifestEntry;
	static void ProduceEntry(DataChunk &input, idx_t result_offset, const case_insensitive_map_t<ColumnIndex> &name_to_vec, entry_type &result);
	static bool VerifySchema(const case_insensitive_map_t<ColumnIndex> &name_to_vec);
	static void PopulateNameMapping(idx_t column_id, LogicalType &type, const string &name, case_insensitive_map_t<ColumnIndex> &name_to_vec);
};

struct IcebergManifestEntryV2 {
	static constexpr const char *NAME = "IcebergManifest";
	static constexpr idx_t FORMAT_VERSION = 2;
	using entry_type = IcebergManifestEntry;
	static void ProduceEntry(DataChunk &input, idx_t result_offset, const case_insensitive_map_t<ColumnIndex> &name_to_vec, entry_type &result);
	static bool VerifySchema(const case_insensitive_map_t<ColumnIndex> &name_to_vec);
	static void PopulateNameMapping(idx_t column_id, LogicalType &type, const string &name, case_insensitive_map_t<ColumnIndex> &name_to_vec);
};

class AvroScan {
public:
	AvroScan(const string &scan_name, ClientContext &context, const string &path);
public:
	bool GetNext(DataChunk &chunk);
	void InitializeChunk(DataChunk &chunk);
public:
	optional_ptr<TableFunction> avro_scan;
	ClientContext &context;
	unique_ptr<FunctionData> bind_data;
	unique_ptr<GlobalTableFunctionState> global_state;
	vector<LogicalType> return_types;
	vector<string> return_names;
};

template <class OP>
vector<typename OP::entry_type> ScanAvroMetadata(ClientContext &context, const string &path) {
	AvroScan scanner(OP::NAME, context, path);

	case_insensitive_map_t<ColumnIndex> name_to_vec;
	for (idx_t i = 0; i < return_types.size(); i++) {
		OP::PopulateNameMapping(i, return_types[i], return_names[i], name_to_vec);
	}

	if (!OP::VerifySchema(name_to_vec)) {
		throw InvalidInputException("%s schema invalid for Iceberg version %d", OP::NAME, OP::FORMAT_VERSION);
	}

	DataChunk result;
	scanner.InitializeChunk(result);

	vector<typename OP::entry_type> ret;
	scanner.GetNext(result);
	while (chunk->size != 0) {
		for (idx_t i = 0; i < chunk->size(); i++) {
			OP::entry_type entry;
			OP::ProduceEntry(result, i, name_to_vec, entry);
			ret.push_back(entry);
		}
	}
	return ret;
}

} // namespace duckdb
