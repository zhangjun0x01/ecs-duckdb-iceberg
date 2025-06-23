#include "deletes/deletion_vector.hpp"
#include "iceberg_multi_file_list.hpp"

#include "duckdb/storage/caching_file_system.hpp"
#include "duckdb/common/bswap.hpp"

namespace duckdb {

PuffinBlobMetadata PuffinBlobMetadata::FromJSON(yyjson_doc *doc, yyjson_val *obj) {
	PuffinBlobMetadata result;

	// Parse "type"
	{
		yyjson_val *val = yyjson_obj_get(obj, "type");
		if (val && yyjson_is_str(val)) {
			result.type = yyjson_get_str(val);
		}
	}

	// Parse "fields" (array of uint)
	{
		yyjson_val *arr = yyjson_obj_get(obj, "fields");
		if (arr && yyjson_is_arr(arr)) {
			size_t idx, max;
			yyjson_val *val;
			yyjson_arr_foreach(arr, idx, max, val) {
				if (yyjson_is_uint(val)) {
					result.fields.push_back(static_cast<int32_t>(yyjson_get_uint(val)));
				}
			}
		}
	}

	// Parse all uint64 fields
	{
		yyjson_val *val = yyjson_obj_get(obj, "snapshot_id");
		if (val && yyjson_is_uint(val)) {
			result.snapshot_id = yyjson_get_uint(val);
		}
	}
	{
		yyjson_val *val = yyjson_obj_get(obj, "sequence_number");
		if (val && yyjson_is_uint(val)) {
			result.sequence_number = yyjson_get_uint(val);
		}
	}
	{
		yyjson_val *val = yyjson_obj_get(obj, "offset");
		if (val && yyjson_is_uint(val)) {
			result.offset = yyjson_get_uint(val);
		}
	}
	{
		yyjson_val *val = yyjson_obj_get(obj, "length");
		if (val && yyjson_is_uint(val)) {
			result.length = yyjson_get_uint(val);
		}
	}

	// Optional: compression_codec
	{
		yyjson_val *val = yyjson_obj_get(obj, "compression_codec");
		if (val && yyjson_is_uint(val)) {
			result.has_compression_codec = true;
			result.compression_codec = yyjson_get_uint(val);
		}
	}

	// Parse properties (object<string, string>)
	{
		yyjson_val *props = yyjson_obj_get(obj, "properties");
		if (props && yyjson_is_obj(props)) {
			size_t idx, max;
			yyjson_val *key;
			yyjson_val *val;
			yyjson_obj_foreach(props, idx, max, key, val) {
				if (yyjson_is_str(key) && yyjson_is_str(val)) {
					result.properties.emplace(yyjson_get_str(key), yyjson_get_str(val));
				}
			}
		}
	}

	return result;
}

PuffinFileMetadata PuffinFileMetadata::FromJSON(yyjson_doc *doc, yyjson_val *obj) {
	PuffinFileMetadata result;

	// Parse blobs
	{
		yyjson_val *blobs = yyjson_obj_get(obj, "blobs");
		if (blobs && yyjson_is_arr(blobs)) {
			size_t idx, max;
			yyjson_val *item;
			yyjson_arr_foreach(blobs, idx, max, item) {
				if (yyjson_is_obj(item)) {
					result.blobs.push_back(PuffinBlobMetadata::FromJSON(doc, item));
				}
			}
		}
	}

	// Parse properties
	{
		yyjson_val *props = yyjson_obj_get(obj, "properties");
		if (props && yyjson_is_obj(props)) {
			size_t idx, max;
			yyjson_val *key;
			yyjson_val *val;
			yyjson_obj_foreach(props, idx, max, key, val) {
				if (yyjson_is_str(key) && yyjson_is_str(val)) {
					result.properties.emplace(yyjson_get_str(key), yyjson_get_str(val));
				}
			}
		}
	}

	return result;
}

unique_ptr<IcebergDeletionVector> IcebergDeletionVector::FromBlob(data_ptr_t blob_start, idx_t blob_length) {
	//! https://iceberg.apache.org/puffin-spec/#deletion-vector-v1-blob-type

	auto vector_size = Load<uint32_t>(blob_start);
	vector_size = BSwap(vector_size);
	blob_start += sizeof(uint32_t);

	constexpr char DELETION_VECTOR_MAGIC[] = {'\xD1', '\xD3', '\x39', '\x64'};
	char magic_bytes[4];
	memcpy(magic_bytes, blob_start, 4);
	blob_start += 4;
	vector_size -= 4;

	if (memcmp(DELETION_VECTOR_MAGIC, magic_bytes, 4)) {
		throw InvalidInputException("Magic bytes mismatch, deletion vector is corrupt!");
	}

	int64_t amount_of_bitmaps = Load<int64_t>(blob_start);
	blob_start += sizeof(int64_t);
	vector_size -= sizeof(int64_t);

	auto result_p = make_uniq<IcebergDeletionVector>();
	auto &result = *result_p;
	result.bitmaps.reserve(amount_of_bitmaps);
	for (int64_t i = 0; i < amount_of_bitmaps; i++) {
		auto key = Load<int32_t>(blob_start);
		blob_start += sizeof(int32_t);
		vector_size -= sizeof(int32_t);

		size_t bitmap_size =
		    roaring::api::roaring_bitmap_portable_deserialize_size((const char *)blob_start, vector_size);
		auto bitmap = roaring::Roaring::readSafe((const char *)blob_start, bitmap_size);
		blob_start += bitmap_size;
		vector_size -= bitmap_size;

		result.bitmaps.emplace(key, std::move(bitmap));
	}
	return result_p;
}

void IcebergMultiFileList::ScanPuffinFile(const IcebergManifestEntry &entry) const {
	auto file_path = entry.file_path;
	D_ASSERT(!entry.referenced_data_file.empty());

	auto caching_file_system = CachingFileSystem::Get(context);

	auto caching_file_handle = caching_file_system.OpenFile(file_path, FileOpenFlags::FILE_FLAGS_READ);
	auto total_size = caching_file_handle->GetFileSize();
	data_ptr_t data = nullptr;

	auto buf_handle = caching_file_handle->Read(data, total_size);
	auto buffer_data = buf_handle.Ptr();

	// constexpr char PUFFIN_MAGIC[] = {'\x50', '\x46', '\x41', '\x31'};

	// char magic_bytes[4];
	// memcpy(magic_bytes, buffer_data, 4);
	// if (memcmp(magic_bytes, PUFFIN_MAGIC, 4)) {
	//	throw InvalidInputException("Magic bytes mismatch, not a Puffin file!");
	//}

	////! Parsing the footer payload

	// const_data_ptr_t footer_ptr = buffer_data + total_size;
	// footer_ptr -= sizeof(uint32_t);
	// memcpy(magic_bytes, footer_ptr, 4);
	// if (memcmp(magic_bytes, PUFFIN_MAGIC, 4)) {
	//	throw InvalidInputException("Magic bytes mismatch, end of Puffin footer is corrupt!");
	//}

	// TemplatedValidityMask<uint8_t> flag_bytes;
	// flag_bytes.Initialize(4 * 8);

	// footer_ptr -= sizeof(uint32_t);
	// memcpy(flag_bytes.GetData(), footer_ptr, 4);

	////! Lowest bit of the flag bytes indicates whether the (JSON) payload is compressed
	// if (flag_bytes.RowIsValid(31)) {
	//	throw NotImplementedException("Puffin payload is compressed, not supported yet");
	//}

	// footer_ptr -= sizeof(uint32_t);
	// auto footer_payload_size = Load<uint32_t>(footer_ptr);

	// Printer::PrintF("Footer payload size: %d", footer_payload_size);

	// auto start_of_footer = footer_ptr -= footer_payload_size;
	// memcpy(magic_bytes, start_of_footer - 4, 4);
	// if (memcmp(magic_bytes, PUFFIN_MAGIC, 4)) {
	//	throw InvalidInputException("Magic bytes mismatch, start of Puffin footer is corrupt!");
	//}

	// Printer::PrintF("JSON Payload: %s", string((const char *)start_of_footer, footer_payload_size));

	// auto doc = yyjson_read((const char *)start_of_footer, footer_payload_size, 0);

	// PuffinFileMetadata file_metadata;
	// try {
	//	file_metadata = PuffinFileMetadata::FromJSON(doc, yyjson_doc_get_root(doc));
	//} catch (std::exception &ex) {
	//	yyjson_doc_free(doc);
	//	throw;
	//}
	// yyjson_doc_free(doc);

	D_ASSERT(!entry.content_offset.IsNull());
	D_ASSERT(!entry.content_size_in_bytes.IsNull());

	auto offset = entry.content_offset.GetValue<int64_t>();
	auto length = entry.content_size_in_bytes.GetValue<int64_t>();

	auto blob_start = buffer_data + offset;
	positional_delete_data.emplace(entry.referenced_data_file, IcebergDeletionVector::FromBlob(blob_start, length));
}

idx_t IcebergDeletionVector::Filter(row_t start_row_index, idx_t count, SelectionVector &result_sel) {
	if (count == 0) {
		return 0;
	}
	result_sel.Initialize(STANDARD_VECTOR_SIZE);
	idx_t selection_idx = 0;

	idx_t offset = 0;
	while (offset < count) {
		const row_t current_row = start_row_index + offset;
		const int32_t high = static_cast<int32_t>(current_row >> 32);

		const row_t next_high_boundary = ((static_cast<row_t>(high) + 1) << 32);
		//! FIXME: How do we test this? These offsets are **huge**
		const idx_t next_offset = MinValue<idx_t>(start_row_index + count, next_high_boundary) - start_row_index;

		auto it = bitmaps.find(high);
		if (it == bitmaps.end()) {
			for (idx_t i = offset; i < next_offset; ++i) {
				result_sel.set_index(selection_idx++, i);
			}
		} else {
			const roaring::Roaring &bitmap = it->second;
			for (idx_t i = offset; i < next_offset; ++i) {
				uint32_t low_bits = static_cast<uint32_t>((start_row_index + i) & 0xFFFFFFFF);
				const bool is_deleted = bitmap.contains(low_bits);
				result_sel.set_index(selection_idx, i);
				selection_idx += !is_deleted;
			}
		}

		offset = next_offset;
	}
	return selection_idx;
}

} // namespace duckdb
