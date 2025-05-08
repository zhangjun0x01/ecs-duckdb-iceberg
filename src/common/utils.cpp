#include "duckdb.hpp"
#include "iceberg_utils.hpp"
#include "zlib.h"
#include "fstream"
#include "duckdb/common/gzip_file_system.hpp"
#include "storage/irc_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"

namespace duckdb {

idx_t IcebergUtils::CountOccurrences(const string &input, const string &to_find) {
	size_t pos = input.find(to_find);
	idx_t count = 0;
	while (pos != std::string::npos) {
		pos = input.find(to_find, pos + to_find.length()); // move past current match
		count++;
	}
	return count;
}

string IcebergUtils::FileToString(const string &path, FileSystem &fs) {
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
	auto file_size = handle->GetFileSize();
	string ret_val(file_size, ' ');
	handle->Read((char *)ret_val.c_str(), file_size);
	return ret_val;
}

static string ExtractIcebergScanPath(const string &sql) {
	auto lower_sql = StringUtil::Lower(sql);
	auto start = lower_sql.find("iceberg_scan('");
	if (start == std::string::npos) {
		throw InvalidInputException("Could not find ICEBERG_SCAN in referenced view");
	}
	start += 14;
	auto end = sql.find("\'", start);
	if (end == std::string::npos) {
		throw InvalidInputException("Could not find end of the ICEBERG_SCAN in referenced view");
	}
	return sql.substr(start, end - start);
}

string IcebergUtils::GetStorageLocation(ClientContext &context, const string &input) {
	auto qualified_name = QualifiedName::Parse(input);
	string storage_location = input;

	do {
		if (qualified_name.catalog.empty() || qualified_name.schema.empty() || qualified_name.name.empty()) {
			break;
		}
		//! Fully qualified table reference, let's do a lookup
		EntryLookupInfo table_info(CatalogType::TABLE_ENTRY, qualified_name.name);
		auto catalog_entry = Catalog::GetEntry(context, qualified_name.catalog, qualified_name.schema, table_info,
		                                       OnEntryNotFound::RETURN_NULL);
		if (!catalog_entry) {
			break;
		}

		if (catalog_entry->type == CatalogType::VIEW_ENTRY) {
			//! This is a view, which we will assume is wrapping an ICEBERG_SCAN(...) query
			auto &view_entry = catalog_entry->Cast<ViewCatalogEntry>();
			auto &sql = view_entry.sql;
			storage_location = ExtractIcebergScanPath(sql);
			break;
		}
		if (catalog_entry->type == CatalogType::TABLE_ENTRY) {
			//! This is a IRCTableEntry, set up the scan from this
			auto &table_entry = catalog_entry->Cast<ICTableEntry>();
			storage_location = table_entry.PrepareIcebergScanFromEntry(context);
			break;
		}
	} while (false);
	return storage_location;
}

// Function to decompress a gz file content string
string IcebergUtils::GzFileToString(const string &path, FileSystem &fs) {
	// Initialize zlib variables
	string gzipped_string = FileToString(path, fs);
	return GZipFileSystem::UncompressGZIPString(gzipped_string);
}

string IcebergUtils::GetFullPath(const string &iceberg_path, const string &relative_file_path, FileSystem &fs) {
	std::size_t found = relative_file_path.rfind("/metadata/");
	if (found != string::npos) {
		return fs.JoinPath(iceberg_path, relative_file_path.substr(found + 1));
	}

	found = relative_file_path.rfind("/data/");
	if (found != string::npos) {
		return fs.JoinPath(iceberg_path, relative_file_path.substr(found + 1));
	}

	throw InvalidConfigurationException("Did not recognize iceberg path");
}

uint64_t IcebergUtils::TryGetNumFromObject(yyjson_val *obj, const string &field) {
	auto val = yyjson_obj_getn(obj, field.c_str(), field.size());
	if (!val || yyjson_get_type(val) != YYJSON_TYPE_NUM) {
		throw InvalidConfigurationException("Invalid field found while parsing field: " + field);
	}
	return yyjson_get_uint(val);
}

bool IcebergUtils::TryGetBoolFromObject(yyjson_val *obj, const string &field) {
	auto val = yyjson_obj_getn(obj, field.c_str(), field.size());
	if (!val || yyjson_get_type(val) != YYJSON_TYPE_BOOL) {
		throw InvalidConfigurationException("Invalid field found while parsing field: " + field);
	}
	return yyjson_get_bool(val);
}

string IcebergUtils::TryGetStrFromObject(yyjson_val *obj, const string &field) {
	auto val = yyjson_obj_getn(obj, field.c_str(), field.size());
	if (!val || yyjson_get_type(val) != YYJSON_TYPE_STR) {
		throw InvalidConfigurationException("Invalid field found while parsing field: " + field);
	}
	return yyjson_get_str(val);
}

template <class TYPE, uint8_t TYPE_NUM, TYPE (*get_function)(yyjson_val *obj)>
static TYPE TemplatedTryGetYYJson(yyjson_val *obj, const string &field, TYPE default_val, bool fail_on_missing = true) {
	auto val = yyjson_obj_get(obj, field.c_str());
	if (val && yyjson_get_type(val) == TYPE_NUM) {
		return get_function(val);
	} else if (!fail_on_missing) {
		return default_val;
	}
	throw InvalidConfigurationException("Invalid field found while parsing field: " + field);
}

uint64_t IcebergUtils::TryGetNumFromObject(yyjson_val *obj, const string &field, bool fail_on_missing,
                                           uint64_t default_val) {
	return TemplatedTryGetYYJson<uint64_t, YYJSON_TYPE_NUM, yyjson_get_uint>(obj, field, default_val, fail_on_missing);
}
bool IcebergUtils::TryGetBoolFromObject(yyjson_val *obj, const string &field, bool fail_on_missing, bool default_val) {
	return TemplatedTryGetYYJson<bool, YYJSON_TYPE_BOOL, yyjson_get_bool>(obj, field, default_val, fail_on_missing);
}
string IcebergUtils::TryGetStrFromObject(yyjson_val *obj, const string &field, bool fail_on_missing,
                                         const char *default_val) {
	return TemplatedTryGetYYJson<const char *, YYJSON_TYPE_STR, yyjson_get_str>(obj, field, default_val,
	                                                                            fail_on_missing);
}

} // namespace duckdb
