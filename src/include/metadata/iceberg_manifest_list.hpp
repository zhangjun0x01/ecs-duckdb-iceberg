#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

using sequence_number_t = int64_t;

struct FieldSummary {
public:
	Value ToValue() const {
		child_list_t<Value> children;
		children.emplace_back("contains_null", Value::BOOLEAN(contains_null));
		children.emplace_back("contains_nan", Value::BOOLEAN(contains_nan));
		D_ASSERT(lower_bound.type().id() == LogicalType::BLOB);
		D_ASSERT(upper_bound.type().id() == LogicalType::BLOB);
		children.emplace_back("lower_bound", lower_bound);
		children.emplace_back("upper_bound", upper_bound);
		return Value::STRUCT(children);
	}

public:
	bool contains_null = false;
	//! Optional
	bool contains_nan = false;
	//! Optional
	Value lower_bound;
	//! Optional
	Value upper_bound;
};

struct ManifestPartitions {
public:
	Value ToValue() const {
		child_list_t<LogicalType> children;
		children.emplace_back("contains_null", LogicalType::BOOLEAN);
		children.emplace_back("contains_nan", LogicalType::BOOLEAN);
		children.emplace_back("lower_bound", LogicalType::BLOB);
		children.emplace_back("upper_bound", LogicalType::BLOB);
		auto field_summary_struct = LogicalType::STRUCT(children);

		if (!has_partitions) {
			return Value(LogicalType::LIST(field_summary_struct));
		}
		vector<Value> fields;
		for (auto &field : field_summary) {
			fields.push_back(field.ToValue());
		}
		return Value::LIST(field_summary_struct, fields);
	}

public:
	bool has_partitions = false;
	vector<FieldSummary> field_summary;
};

enum class IcebergManifestContentType : uint8_t {
	DATA = 0,
	DELETE = 1,
};

//! An entry in the manifest list file (top level AVRO file)
struct IcebergManifest {
public:
	//! Path to the manifest AVRO file
	string manifest_path;
	//! sequence_number when manifest was added to table (0 for Iceberg v1)
	sequence_number_t sequence_number = 0;
	bool has_min_sequence_number = false;
	sequence_number_t min_sequence_number = 0;
	//! either data or deletes
	IcebergManifestContentType content;
	//! added files count
	idx_t added_files_count = 0;
	//! existing files count
	idx_t existing_files_count = 0;
	//! deleted files count
	idx_t deleted_files_count = 0;
	//! added rows in the manifest
	idx_t added_rows_count = 0;
	//! existing rows in the manifest
	idx_t existing_rows_count = 0;
	//! deleted rows in the manifest
	idx_t deleted_rows_count = 0;
	//! The id of the partition spec referenced by this manifest (and the data files that are part of it)
	int32_t partition_spec_id;
	//! The field summaries of the partition (if present)
	ManifestPartitions partitions;

public:
	static vector<LogicalType> Types() {
		return {
		    LogicalType::VARCHAR,
		    LogicalType::BIGINT,
		    LogicalType::VARCHAR,
		};
	}

	static string ContentTypeToString(IcebergManifestContentType type) {
		switch (type) {
		case IcebergManifestContentType::DATA:
			return "DATA";
		case IcebergManifestContentType::DELETE:
			return "DELETE";
		default:
			throw InvalidConfigurationException("Invalid Manifest Content Type");
		}
	}

	static vector<string> Names() {
		return {"manifest_path", "manifest_sequence_number", "manifest_content"};
	}
};

struct IcebergManifestList {
public:
	IcebergManifestList(const string &path) : path(path) {
	}

public:
	string path;
	vector<IcebergManifest> manifests;
};

} // namespace duckdb
