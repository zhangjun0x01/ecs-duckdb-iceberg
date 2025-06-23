#pragma once

#include "duckdb/common/multi_file/multi_file_data.hpp"
#include <roaring/roaring.hh>

#include "yyjson.hpp"

using namespace duckdb_yyjson;

namespace duckdb {

struct PuffinBlobMetadata {
public:
	PuffinBlobMetadata() {
	}

public:
	static PuffinBlobMetadata FromJSON(yyjson_doc *doc, yyjson_val *obj);

public:
	string type;
	vector<int32_t> fields;
	int64_t snapshot_id = 0;
	int64_t sequence_number = 0;
	int64_t offset = 0;
	int64_t length = 0;

	bool has_compression_codec = false;
	int64_t compression_codec = 0;

	unordered_map<string, string> properties;
};

struct PuffinFileMetadata {
public:
	PuffinFileMetadata() {
	}

public:
	static PuffinFileMetadata FromJSON(yyjson_doc *doc, yyjson_val *obj);

public:
	vector<PuffinBlobMetadata> blobs;
	unordered_map<string, string> properties;
};

struct IcebergDeletionVector : public DeleteFilter {
public:
	IcebergDeletionVector() {
	}

public:
	static unique_ptr<IcebergDeletionVector> FromBlob(data_ptr_t blob_start, idx_t blob_length);

public:
	idx_t Filter(row_t start_row_index, idx_t count, SelectionVector &result_sel) override;

public:
	unordered_map<int32_t, roaring::Roaring> bitmaps;
};

} // namespace duckdb
