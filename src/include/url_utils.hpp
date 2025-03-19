//===----------------------------------------------------------------------===//
//                         DuckDB
//
// url_utils.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <vector>

namespace duckdb {

class IRCEndpointBuilder {
public:
	void AddPathComponent(std::string component);

	void SetPrefix(std::string prefix_);
	std::string GetPrefix() const;

	void SetHost(std::string host_);
	std::string GetHost() const;

	void SetWarehouse(std::string warehouse_);
	std::string GetWarehouse() const;

	void SetVersion(std::string version_);
	std::string GetVersion() const;

	std::string GetURL() const;

	//! path components when querying. Like namespaces/tables etc.
	std::vector<std::string> path_components;

private:
	//! host of the endpoint, like `glue` or `polaris`
	std::string host;
	//! version
	std::string version;
	//! optional prefix
	std::string prefix;
	//! warehouse
	std::string warehouse;
};

} // namespace duckdb