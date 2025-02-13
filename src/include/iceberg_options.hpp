#pragma once

#include "duckdb/common/string.hpp"

namespace duckdb {

// First arg is version string, arg is either empty or ".gz" if gzip
// Allows for both "v###.gz.metadata.json" and "###.metadata.json" styles
static string DEFAULT_TABLE_VERSION_FORMAT = "v%s%s.metadata.json,%s%s.metadata.json";
static string DEFAULT_VERSION_HINT_FILE = "version-hint.text";

enum class SnapshotSource : uint8_t {
	LATEST,
	FROM_TIMESTAMP,
	FROM_ID
};

struct IcebergOptions {
	bool allow_moved_paths = false;
	string metadata_compression_codec = "none";
	bool skip_schema_inference = false;
	string table_version = DEFAULT_VERSION_HINT_FILE;
	string version_name_format = DEFAULT_TABLE_VERSION_FORMAT;

	SnapshotSource snapshot_source = SnapshotSource::LATEST;
	uint64_t snapshot_id;
	timestamp_t snapshot_timestamp;
};

} // namespace duckdb
