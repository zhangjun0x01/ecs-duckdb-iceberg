//===----------------------------------------------------------------------===//
//                         DuckDB
//
// url_utils.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {

class IRCEndpointBuilder {
private:
	struct QueryParameter {
	public:
		QueryParameter(const string &key, const string &value) : key(key), value(value) {}
	public:
		string key;
		string value;
	};
public:
	void AddPathComponent(const string &component);
	void AddQueryParameter(const string &key, const string &value);

	void SetPrefix(const string &prefix_);
	string GetPrefix() const;

	void SetHost(const string &host_);
	string GetHost() const;

	void SetWarehouse(const string &warehouse_);
	string GetWarehouse() const;

	void SetVersion(const string &version_);
	string GetVersion() const;

	void SetParam(const string &key, const string &value);
	string GetParam(const string &key) const;
	const unordered_map<string, string> GetParams();

	string GetURL() const;

	//! path components when querying. Like namespaces/tables etc.
	vector<string> path_components;

	//! query parameters at the end of the url.
	vector<QueryParameter> query_parameters;

private:
	//! host of the endpoint, like `glue` or `polaris`
	string host;
	//! version
	string version;
	//! optional prefix
	string prefix;
	//! warehouse
	string warehouse;

	unordered_map<string, string> params;
};

} // namespace duckdb
