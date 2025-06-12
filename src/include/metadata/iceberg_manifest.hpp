#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

using sequence_number_t = int64_t;

enum class IcebergManifestEntryContentType : uint8_t { DATA = 0, POSITION_DELETES = 1, EQUALITY_DELETES = 2 };

enum class IcebergManifestEntryStatusType : uint8_t { EXISTING = 0, ADDED = 1, DELETED = 2 };

//! An entry in a manifest file
struct IcebergManifestEntry {
public:
	IcebergManifestEntryStatusType status;
	//! ----- Data File Struct ------
	IcebergManifestEntryContentType content;
	string file_path;
	string file_format;
	vector<int32_t> equality_ids;
	int64_t record_count;
	//! source_id -> blob
	unordered_map<int32_t, Value> lower_bounds;
	unordered_map<int32_t, Value> upper_bounds;
	unordered_map<int32_t, int64_t> value_counts;
	unordered_map<int32_t, int64_t> null_value_counts;
	unordered_map<int32_t, int64_t> nan_value_counts;
	Value partition;
	//! Inherited from the 'manifest_file' if NULL and 'status == EXISTING'
	sequence_number_t sequence_number;
	//! Inherited from the 'manifest_file'
	int32_t partition_spec_id;
	int64_t file_size_in_bytes;

public:
	Value ToDataFileStruct(const LogicalType &type) const;

public:
	static vector<LogicalType> Types() {
		return {
		    LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
		};
	}

	static string ContentTypeToString(IcebergManifestEntryContentType type) {
		switch (type) {
		case IcebergManifestEntryContentType::DATA:
			return "EXISTING";
		case IcebergManifestEntryContentType::POSITION_DELETES:
			return "POSITION_DELETES";
		case IcebergManifestEntryContentType::EQUALITY_DELETES:
			return "EQUALITY_DELETES";
		default:
			throw InvalidConfigurationException("Invalid Manifest Entry Content Type");
		}
	}

	static string StatusTypeToString(IcebergManifestEntryStatusType type) {
		switch (type) {
		case IcebergManifestEntryStatusType::EXISTING:
			return "EXISTING";
		case IcebergManifestEntryStatusType::ADDED:
			return "ADDED";
		case IcebergManifestEntryStatusType::DELETED:
			return "DELETED";
		default:
			throw InvalidConfigurationException("Invalid matifest entry type");
		}
	}

	static vector<string> Names() {
		return {"status", "content", "file_path", "file_format", "record_count"};
	}
};

struct IcebergManifestFile {
	IcebergManifestFile(const string &path) : path(path) {
	}

public:
	string path;
	vector<IcebergManifestEntry> data_files;
};

} // namespace duckdb
