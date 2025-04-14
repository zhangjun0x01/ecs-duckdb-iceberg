#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

class RequestInput {
public:
	RequestInput() {
	}

public:
	string GET(ClientContext &context);
	string POST(ClientContext &context, const string &post_data);
	string DELETE(ClientContext &context);
	void AddHeader(const string &header) {
		headers.push_back(header);
	}
	void SetURL(const string &url) {
		this->url = url;
	}
	void SetCertPath(const string &cert_path) {
		this->cert_path = cert_path;
	}
	void SetBearerToken(const string &token) {
		bearer_token = token;
	}

public:
	vector<string> headers;
	string url;
	string cert_path;
	string bearer_token;
};

} // namespace duckdb
