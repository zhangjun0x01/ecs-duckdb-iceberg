#pragma once

#include "iceberg_options.hpp"
#include "iceberg_types.hpp"
#include "iceberg_manifest.hpp"

namespace duckdb {

// Manifest Reader

typedef void (*avro_reader_name_mapping)(idx_t column_id, const LogicalType &type, const string &name, case_insensitive_map_t<ColumnIndex> &mapping);
typedef bool (*avro_reader_schema_validation)(const case_insensitive_map_t<ColumnIndex> &mapping);
typedef void (*avro_reader_manifest_producer)(DataChunk &input, idx_t result_offset, const case_insensitive_map_t<ColumnIndex> &mapping, IcebergManifest &result);
typedef void (*avro_reader_manifest_entry_producer)(DataChunk &input, idx_t result_offset, const case_insensitive_map_t<ColumnIndex> &mapping, IcebergManifestEntry &result);

class AvroReader {
public:
	AvroReader(avro_reader_name_mapping_t name_mapping, avro_reader_schema_validation schema_validator, avro_reader_manifest_producer manifest_producer);
	AvroReader(avro_reader_name_mapping_t name_mapping, avro_reader_schema_validation schema_validator, avro_reader_manifest_entry_producer entry_producer);
public:
	void Initialize(unique_ptr<AvroScan> scan_p);

public:
	bool Finished() const;

	template <class TYPE>
	bool GetNext(TYPE &entry) {
		if (!scan || finished) {
			return false;
		}
		if (offset >= result.size()) {
			scan->GetNext(result);
			if (result.size() == 0) {
				finished = true;
				return false;
			}
		}

		if (manifest_producer) {
			manifest_producer(result, offset, name_to_vec, entry);
		} else {
			manifest_entry_producer(result, offset, name_to_vec, entry);
		}
		offset++;
		return true;
	}
private:
	unique_ptr<AvroScan> scan;
	DataChunk result;
	idx_t offset = 0;
	bool finished = true;
	case_insensitive_map_t<ColumnIndex> name_to_vec;

	avro_reader_name_mapping name_mapping = nullptr;
	avro_reader_schema_validation schema_validation = nullptr;
	avro_reader_manifest_producer manifest_producer = nullptr;
	avro_reader_manifest_entry_producer manifest_entry_producer = nullptr;
};

} // namespace duckdb
