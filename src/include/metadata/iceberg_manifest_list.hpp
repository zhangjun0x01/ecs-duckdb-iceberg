#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

using sequence_number_t = int64_t;

struct FieldSummary {
	bool contains_null = false;
	//! Optional
	bool contains_nan = false;
	//! Optional
	Value lower_bound;
	//! Optional
	Value upper_bound;
};

struct ManifestPartitions {
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
	sequence_number_t sequence_number;
	//! either data or deletes
	IcebergManifestContentType content;
	//! added rows in the manifest
	idx_t added_rows_count = 0;
	//! existing rows in the manifest
	idx_t existing_rows_count = 0;
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
