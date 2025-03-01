#pragma once

#include "iceberg_options.hpp"
#include "iceberg_types.hpp"
#include "iceberg_manifest.hpp"

namespace duckdb {

// Manifest Reader

typedef void (*manifest_reader_name_mapping)(idx_t column_id, const LogicalType &type, const string &name, case_insensitive_map_t<ColumnIndex> &mapping);
typedef bool (*manifest_reader_schema_validation)(const case_insensitive_map_t<ColumnIndex> &mapping);

typedef idx_t (*manifest_reader_manifest_producer)(DataChunk &input, idx_t offset, idx_t count, const case_insensitive_map_t<ColumnIndex> &mapping, vector<IcebergManifest> &result);
typedef idx_t (*manifest_reader_manifest_entry_producer)(DataChunk &input, idx_t offset, idx_t count, const case_insensitive_map_t<ColumnIndex> &mapping, vector<IcebergManifestEntry> &result);

using manifest_reader_read = std::function<idx_t(DataChunk &input, idx_t offset, idx_t count, const case_insensitive_map_t<ColumnIndex> &mapping)>;

class ManifestReader {
public:
	ManifestReader(manifest_reader_name_mapping name_mapping, manifest_reader_schema_validation schema_validator);
public:
	void Initialize(unique_ptr<AvroScan> scan_p);
public:
	bool Finished() const;
	idx_t ReadEntries(idx_t count, manifest_reader_read callback);
private:
	unique_ptr<AvroScan> scan;
	DataChunk input;
	idx_t offset = 0;
	bool finished = true;
	case_insensitive_map_t<ColumnIndex> name_to_vec;

	manifest_reader_name_mapping name_mapping = nullptr;
	manifest_reader_schema_validation schema_validation = nullptr;
};

} // namespace duckdb
