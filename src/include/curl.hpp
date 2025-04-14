#pragma once

#include "duckdb/common/string.hpp"

#ifdef WASM_LOADABLE_EXTENSIONS

#else
//! fwd declare
struct curl_slist;
#endif

namespace duckdb {

#ifdef WASM_LOADABLE_EXTENSIONS

//! TODO: implement WASM Curl alternative.

#else

class CURLHandle {
public:
	CURLHandle() {
	}
	~CURLHandle();

public:
	void AddHeader(const string &header);

public:
	struct curl_slist *extra_headers = nullptr;
};

#endif

} // namespace duckdb
