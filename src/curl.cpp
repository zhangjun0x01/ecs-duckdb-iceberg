#include "curl.hpp"

#ifdef WASM_LOADABLE_EXTENSIONS
#else
#include <curl/curl.h>
#endif

namespace duckdb {

#ifdef WASM_LOADABLE_EXTENSIONS

#else

#include <curl/curl.h>

CURLHandle::~CURLHandle() {
	if (extra_headers) {
		curl_slist_free_all(extra_headers);
	}
}

void CURLHandle::AddHeader(const string &header) {
	extra_headers = curl_slist_append(extra_headers, header.c_str());
}

#endif

} // namespace duckdb
