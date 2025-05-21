#include "manifest_reader.hpp"

namespace duckdb {

ManifestReaderInput::ManifestReaderInput(const case_insensitive_map_t<ColumnIndex> &name_to_vec,
                                         sequence_number_t sequence_number, int32_t partition_spec_id,
                                         bool skip_deleted)
    : name_to_vec(name_to_vec), skip_deleted(skip_deleted), sequence_number(sequence_number),
      partition_spec_id(partition_spec_id) {
}

void BaseManifestReader::SetSequenceNumber(sequence_number_t sequence_number_p) {
	sequence_number = sequence_number_p;
}

void BaseManifestReader::SetPartitionSpecID(int32_t partition_spec_id_p) {
	partition_spec_id = partition_spec_id_p;
}

void BaseManifestReader::Initialize(unique_ptr<AvroScan> scan_p) {
	const bool first_init = scan == nullptr;
	scan = std::move(scan_p);
	if (!first_init) {
		chunk.Destroy();
	}
	//! Reinitialize for every new scan, the schema isn't guaranteed to be the same for every scan
	//! the 'partition' of the 'data_file' is based on the partition spec referenced by the manifest
	scan->InitializeChunk(chunk);

	finished = false;
	offset = 0;
	name_to_vec.clear();

	for (idx_t i = 0; i < scan->return_types.size(); i++) {
		auto &type = scan->return_types[i];
		auto &name = scan->return_names[i];
		CreateNameMapping(i, type, name);
	}

	if (!schema_validation(name_to_vec)) {
		throw InvalidInputException("Invalid schema detected in a manifest/manifest entry");
	}
}

idx_t BaseManifestReader::ScanInternal(idx_t remaining) {
	if (!scan || finished) {
		return 0;
	}

	if (offset >= chunk.size()) {
		scan->GetNext(chunk);
		if (chunk.size() == 0) {
			finished = true;
			return 0;
		}
		offset = 0;
	}
	return MinValue(chunk.size() - offset, remaining);
}

bool BaseManifestReader::Finished() const {
	if (!scan) {
		return true;
	}
	return scan->Finished();
}

// --------------- NEW ---------------

ManifestListReader::ManifestListReader(idx_t iceberg_version) : BaseManifestReader(iceberg_version) {
}

idx_t ManifestListReader::Read(idx_t count, vector<IcebergManifest> &result) {
}

idx_t ManifestListReader::ReadChunk(idx_t offset, idx_t size, vector<IcebergManifest> &result) {
	D_ASSERT(offset < chunk.size());
	D_ASSERT(offset + count <= chunk.size());
	auto manifest_path = FlatVector::GetData<string_t>(chunk.data[name_to_vec.at("manifest_path").GetPrimaryIndex()]);
	auto partition_spec_id =
	    FlatVector::GetData<int32_t>(chunk.data[name_to_vec.at("partition_spec_id").GetPrimaryIndex()]);

	int32_t *content = nullptr;
	int64_t *sequence_number = nullptr;
	int64_t *added_rows_count = nullptr;
	int64_t *existing_rows_count = nullptr;

	if (iceberg_version > 1) {
		//! 'content'
		content = FlatVector::GetData<int32_t>(chunk.data[name_to_vec.at("content").GetPrimaryIndex()]);
		//! 'sequence_number'
		sequence_number = FlatVector::GetData<int64_t>(chunk.data[name_to_vec.at("sequence_number").GetPrimaryIndex()]);
		//! 'added_rows_count'
		added_rows_count =
		    FlatVector::GetData<int64_t>(chunk.data[name_to_vec.at("added_rows_count").GetPrimaryIndex()]);
		//! 'existing_rows_count'
		existing_rows_count =
		    FlatVector::GetData<int64_t>(chunk.data[name_to_vec.at("existing_rows_count").GetPrimaryIndex()]);
	}

	//! 'partitions'
	list_entry_t *field_summary;
	optional_ptr<Vector> contains_null = nullptr;
	optional_ptr<Vector> contains_nan = nullptr;
	optional_ptr<Vector> lower_bound = nullptr;
	optional_ptr<Vector> upper_bound = nullptr;

	bool *contains_null_data = nullptr;
	bool *contains_nan_data = nullptr;

	auto partitions_it = name_to_vec.find("partitions");
	if (partitions_it != name_to_vec.end()) {
		auto &partitions = chunk.data[name_to_vec.at("partitions").GetPrimaryIndex()];

		auto &field_summary_vec = ListVector::GetEntry(partitions);
		field_summary = FlatVector::GetData<list_entry_t>(partitions);
		auto &child_vectors = StructVector::GetEntries(field_summary_vec);
		auto &child_types = StructType::GetChildTypes(ListType::GetChildType(partitions.GetType()));
		for (idx_t i = 0; i < child_types.size(); i++) {
			auto &kv = child_types[i];
			auto &name = kv.first;

			if (StringUtil::CIEquals(name, "contains_null")) {
				contains_null = child_vectors[i].get();
				contains_null_data = FlatVector::GetData<bool>(*child_vectors[i]);
			} else if (StringUtil::CIEquals(name, "contains_nan")) {
				contains_nan = child_vectors[i].get();
				contains_nan_data = FlatVector::GetData<bool>(*child_vectors[i]);
			} else if (StringUtil::CIEquals(name, "lower_bound")) {
				lower_bound = child_vectors[i].get();
			} else if (StringUtil::CIEquals(name, "upper_bound")) {
				upper_bound = child_vectors[i].get();
			}
		}
	}

	for (idx_t i = 0; i < count; i++) {
		idx_t index = i + offset;

		IcebergManifest manifest;
		manifest.manifest_path = manifest_path[index].GetString();
		manifest.partition_spec_id = partition_spec_id[index];
		manifest.sequence_number = 0;

		if (iceberg_version > 1) {
			manifest.content = IcebergManifestContentType(content[index]);
			manifest.sequence_number = sequence_number[index];
			manifest.added_rows_count = added_rows_count[index];
			manifest.existing_rows_count = existing_rows_count[index];
		} else {
			manifest.content = IcebergManifestContentType::DATA;
			manifest.sequence_number = 0;
			manifest.added_rows_count = 0;
			manifest.existing_rows_count = 0;
		}

		if (field_summary) {
			auto list_entry = field_summary[index];
			for (idx_t j = 0; j < list_entry.length; j++) {
				FieldSummary summary;
				auto list_idx = list_entry.offset + j;
				if (contains_null && FlatVector::Validity(*contains_null).RowIsValid(list_idx)) {
					summary.contains_null = contains_null_data[list_idx];
				}
				if (contains_nan && FlatVector::Validity(*contains_nan).RowIsValid(list_idx)) {
					summary.contains_nan = contains_nan_data[list_idx];
				}
				if (lower_bound) {
					summary.lower_bound = lower_bound->GetValue(list_idx);
				}
				if (upper_bound) {
					summary.upper_bound = upper_bound->GetValue(list_idx);
				}
				manifest.field_summary.push_back(summary);
			}
		}
		result.push_back(manifest);
	}
	return count;
}

void ManifestListReader::ValidateNameMapping() {
	if (!name_to_vec.count("manifest_path")) {
		return false;
	}
	if (!name_to_vec.count("partition_spec_id")) {
		return false;
	}
	if (iceberg_version > 1) {
		if (!name_to_vec.count("content")) {
			return false;
		}
	}
	return true;
}

ManifestFileReader::ManifestFileReader(idx_t iceberg_version) : BaseManifestReader(iceberg_version) {
}

idx_t ManifestFileReader::Read(idx_t count, vector<IcebergManifestEntry> &result) {
	if (!scan || finished) {
		return;
	}

	idx_t total_read = 0;
	idx_t total_added = 0;
	while (total_read < count && !finished) {
		auto tuples = ScanInternal(count - total_read);
		total_added += ReadChunk(offset, tuples, result);
		offset += tuples;
		total_read += tuples;
	}
	return total_added;
}

void ManifestFileReader::CreateNameMapping(idx_t column_id, LogicalType &type, const string &name) {
	auto lname = StringUtil::Lower(name);
	if (lname != "data_file") {
		name_to_vec[lname] = ColumnIndex(column_id);
		return;
	}

	if (type.id() != LogicalTypeId::STRUCT) {
		throw InvalidInputException("The 'data_file' of the manifest should be a STRUCT");
	}
	auto &children = StructType::GetChildTypes(type);
	for (idx_t child_idx = 0; child_idx < children.size(); child_idx++) {
		auto &child = children[child_idx];
		auto child_name = StringUtil::Lower(child.first);

		name_to_vec[child_name] = ColumnIndex(column_id, {ColumnIndex(child_idx)});
	}
}

void ManifestFileReader::ValidateNameMapping() {
	if (!name_to_vec.count("status")) {
		return false;
	}
	if (!name_to_vec.count("file_path")) {
		return false;
	}
	if (!name_to_vec.count("file_format")) {
		return false;
	}
	if (!name_to_vec.count("record_count")) {
		return false;
	}
	if (iceberg_version > 1) {
		if (!name_to_vec.count("content")) {
			return false;
		}
	}
	return true;
}

idx_t ManifestFileReader::ReadChunk(idx_t offset, idx_t count, vector<IcebergManifestEntry> &result) {
	D_ASSERT(offset < chunk.size());
	D_ASSERT(offset + count <= chunk.size());

	auto status = FlatVector::GetData<int32_t>(chunk.data[name_to_vec.at("status").GetPrimaryIndex()]);

	auto file_path_idx = name_to_vec.at("file_path");
	auto data_file_idx = file_path_idx.GetPrimaryIndex();
	auto &child_entries = StructVector::GetEntries(chunk.data[data_file_idx]);
	D_ASSERT(name_to_vec.at("file_format").GetPrimaryIndex() == data_file_idx);
	D_ASSERT(name_to_vec.at("record_count").GetPrimaryIndex() == data_file_idx);
	if (iceberg_version > 1) {
		D_ASSERT(name_to_vec.at("content").GetPrimaryIndex() == data_file_idx);
	}
	optional_ptr<Vector> equality_ids;
	optional_ptr<Vector> sequence_number;
	int32_t *content;

	auto partition_idx = name_to_vec.at("partition");
	if (iceberg_version > 1) {
		auto equality_ids_it = name_to_vec.find("equality_ids");
		if (equality_ids_it != name_to_vec.end()) {
			equality_ids = *child_entries[equality_ids_it->second.GetChildIndex(0).GetPrimaryIndex()];
		}
		auto sequence_number_it = name_to_vec.find("sequence_number");
		if (sequence_number_it != name_to_vec.end()) {
			sequence_number = chunk.data[sequence_number_it->second.GetPrimaryIndex()];
		}
		content =
		    FlatVector::GetData<int32_t>(*child_entries[name_to_vec.at("content").GetChildIndex(0).GetPrimaryIndex()]);
	}

	auto file_path = FlatVector::GetData<string_t>(*child_entries[file_path_idx.GetChildIndex(0).GetPrimaryIndex()]);
	auto file_format =
	    FlatVector::GetData<string_t>(*child_entries[name_to_vec.at("file_format").GetChildIndex(0).GetPrimaryIndex()]);
	auto record_count =
	    FlatVector::GetData<int64_t>(*child_entries[name_to_vec.at("record_count").GetChildIndex(0).GetPrimaryIndex()]);
	auto file_size_in_bytes = FlatVector::GetData<int64_t>(
	    *child_entries[name_to_vec.at("file_size_in_bytes").GetChildIndex(0).GetPrimaryIndex()]);
	optional_ptr<Vector> lower_bounds;
	optional_ptr<Vector> upper_bounds;
	optional_ptr<Vector> value_counts;
	optional_ptr<Vector> null_value_counts;
	optional_ptr<Vector> nan_value_counts;

	auto lower_bounds_it = name_to_vec.find("lower_bounds");
	if (lower_bounds_it != name_to_vec.end()) {
		lower_bounds = *child_entries[lower_bounds_it->second.GetChildIndex(0).GetPrimaryIndex()];
	}
	auto upper_bounds_it = name_to_vec.find("upper_bounds");
	if (upper_bounds_it != name_to_vec.end()) {
		upper_bounds = *child_entries[upper_bounds_it->second.GetChildIndex(0).GetPrimaryIndex()];
	}
	auto value_counts_it = name_to_vec.find("value_counts");
	if (value_counts_it != name_to_vec.end()) {
		value_counts = *child_entries[value_counts_it->second.GetChildIndex(0).GetPrimaryIndex()];
	}
	auto null_value_counts_it = name_to_vec.find("null_value_counts");
	if (null_value_counts_it != name_to_vec.end()) {
		null_value_counts = *child_entries[null_value_counts_it->second.GetChildIndex(0).GetPrimaryIndex()];
	}
	auto nan_value_counts_it = name_to_vec.find("nan_value_counts");
	if (nan_value_counts_it != name_to_vec.end()) {
		nan_value_counts = *child_entries[nan_value_counts_it->second.GetChildIndex(0).GetPrimaryIndex()];
	}
	auto &partition_vec = child_entries[partition_idx.GetChildIndex(0).GetPrimaryIndex()];

	idx_t produced = 0;
	for (idx_t i = 0; i < count; i++) {
		idx_t index = i + offset;

		IcebergManifestEntry entry;

		entry.status = (IcebergManifestEntryStatusType)status[index];
		if (input.skip_deleted && entry.status == IcebergManifestEntryStatusType::DELETED) {
			//! Skip this entry, we don't care about deleted entries
			continue;
		}

		entry.file_path = file_path[index].GetString();
		entry.file_format = file_format[index].GetString();
		entry.record_count = record_count[index];
		entry.file_size_in_bytes = file_size_in_bytes[index];

		if (lower_bounds && upper_bounds) {
			entry.lower_bounds = GetBounds(*lower_bounds, index);
			entry.upper_bounds = GetBounds(*upper_bounds, index);
		}
		if (value_counts) {
			entry.value_counts = GetCounts(*value_counts, index);
		}
		if (null_value_counts) {
			entry.null_value_counts = GetCounts(*null_value_counts, index);
		}
		if (nan_value_counts) {
			entry.nan_value_counts = GetCounts(*nan_value_counts, index);
		}

		if (iceberg_version > 1) {
			entry.content = (IcebergManifestEntryContentType)content[index];
			if (equality_ids) {
				entry.equality_ids = GetEqualityIds(*equality_ids, index);
			}

			if (sequence_number) {
				auto sequence_numbers = FlatVector::GetData<int64_t>(*sequence_number);
				if (FlatVector::Validity(*sequence_number).RowIsValid(index)) {
					entry.sequence_number = sequence_numbers[index];
				} else {
					//! Value should only be NULL for ADDED manifest entries, to support inheritance
					D_ASSERT(entry.status == IcebergManifestEntryStatusType::ADDED);
					entry.sequence_number = input.sequence_number;
				}
			} else {
				//! Default to sequence number 0
				//! (The 'manifest_file' should also have defaulted to 0)
				D_ASSERT(input.sequence_number == 0);
				entry.sequence_number = 0;
			}
		} else {
			entry.sequence_number = input.sequence_number;
			entry.content = IcebergManifestEntryContentType::DATA;
		}

		entry.partition_spec_id = input.partition_spec_id;
		entry.partition = partition_vec->GetValue(index);
		produced++;
		result.push_back(entry);
	}
	return produced;
}

} // namespace duckdb
