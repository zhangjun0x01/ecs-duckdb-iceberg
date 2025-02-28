#include "manifest_reader.hpp"

namespace duckdb {

AvroReader::AvroReader(avro_reader_name_mapping_t name_mapping, avro_reader_schema_validation schema_validator, avro_reader_manifest_producer manifest_producer) : name_mapping(name_mapping), schema_validator(schema_validator), manifest_producer(manifest_producer) {}
AvroReader::AvroReader(avro_reader_name_mapping_t name_mapping, avro_reader_schema_validation schema_validator, avro_reader_manifest_entry_producer entry_producer) : name_mapping(name_mapping), schema_validator(schema_validator), manifest_entry_producer(manifest_entry_producer) {}

void AvroReader::Initialize(unique_ptr<AvroScan> scan_p) {
	scan = std::move(scan_p);
	scan->InitializeChunk(result);
	finished = false;
	offset = 0;
	name_to_vec.clear();

	for (idx_t i = 0; i < scan->return_types.size(); i++) {
		auto &type = scan->return_types[i];
		auto &name = scan->return_names[i];
		name_mapping(i, type, name, name_to_vec);
	}

	if (!schema_validator(name_to_vec)) {
		throw InvalidInputException("Invalid schema detected in a manifest/manifest entry");
	}
}

bool AvroReader::Finished() const {
	if (!scan) {
		return true;
	}
	return scan->Finished();
}

} // namespace duckdb
