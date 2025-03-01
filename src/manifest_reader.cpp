#include "manifest_reader.hpp"

namespace duckdb {

ManifestReader::ManifestReader(manifest_reader_name_mapping name_mapping, manifest_reader_schema_validation schema_validation) : name_mapping(name_mapping), schema_validation(schema_validation) {}

void ManifestReader::Initialize(unique_ptr<AvroScan> scan_p) {
	const bool first_init = scan == nullptr;
	scan = std::move(scan_p);
	if (first_init) {
		scan->InitializeChunk(input);
	} else {
		input.Reset();
	}
	finished = false;
	offset = 0;
	name_to_vec.clear();

	for (idx_t i = 0; i < scan->return_types.size(); i++) {
		auto &type = scan->return_types[i];
		auto &name = scan->return_names[i];
		name_mapping(i, type, name, name_to_vec);
	}

	if (!schema_validation(name_to_vec)) {
		throw InvalidInputException("Invalid schema detected in a manifest/manifest entry");
	}
}

idx_t ManifestReader::ReadEntries(idx_t count, manifest_reader_read callback) {
	if (!scan || finished) {
		return 0;
	}

	idx_t scanned = 0;
	while (scanned < count) {
		if (offset >= input.size()) {
			scan->GetNext(input);
			if (input.size() == 0) {
				finished = true;
				return scanned;
			}
			offset = 0;
		}

		idx_t remaining = count - scanned;
		idx_t to_scan = MinValue(input.size() - offset, remaining);

		scanned += callback(input, offset, to_scan, name_to_vec);
		offset += count;
	}
	return scanned;
}

bool ManifestReader::Finished() const {
	if (!scan) {
		return true;
	}
	return scan->Finished();
}

} // namespace duckdb
