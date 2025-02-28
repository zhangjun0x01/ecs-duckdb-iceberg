#pragma once

#include "iceberg_options.hpp"
#include "iceberg_types.hpp"
#include "iceberg_manifest.hpp"

namespace duckdb {

// Manifest Reader

typedef void (*avro_reader_name_mapping)(idx_t column_id, const LogicalType &type, const string &name, case_insensitive_map_t<ColumnIndex> &mapping);
typedef bool (*avro_reader_schema_validation)(const case_insensitive_map_t<ColumnIndex> &mapping);

typedef idx_t (*avro_reader_manifest_producer)(DataChunk &input, idx_t offset, idx_t count, const case_insensitive_map_t<ColumnIndex> &mapping, vector<IcebergManifest> &result);
typedef idx_t (*avro_reader_manifest_entry_producer)(DataChunk &input, idx_t offset, idx_t count, const case_insensitive_map_t<ColumnIndex> &mapping, vector<IcebergManifestEntry> &result);

using avro_reader_read = std::function<idx_t(DataChunk &input, idx_t offset, idx_t count, const case_insensitive_map_t<ColumnIndex> &mapping)>;

class AvroReader {
public:
	AvroReader(avro_reader_name_mapping name_mapping, avro_reader_schema_validation schema_validator);
public:
	void Initialize(unique_ptr<AvroScan> scan_p);
public:
	bool Finished() const;
	idx_t ReadEntries(idx_t count, avro_reader_read callback);
private:
	unique_ptr<AvroScan> scan;
	DataChunk input;
	idx_t offset = 0;
	bool finished = true;
	case_insensitive_map_t<ColumnIndex> name_to_vec;

	avro_reader_name_mapping name_mapping = nullptr;
	avro_reader_schema_validation schema_validation = nullptr;
};

} // namespace duckdb
