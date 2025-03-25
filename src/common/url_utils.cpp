
#include "url_utils.hpp"

#include <duckdb.h>

namespace duckdb {

void IRCEndpointBuilder::AddPathComponent(std::string component) {
	path_components.push_back(component);
}

void IRCEndpointBuilder::SetPrefix(std::string prefix_) {
	prefix = prefix_;
}

std::string IRCEndpointBuilder::GetHost() const {
	return host;
}

void IRCEndpointBuilder::SetVersion(std::string version_) {
	version = version_;
}

std::string IRCEndpointBuilder::GetVersion() const {
	return version;
}

void IRCEndpointBuilder::SetWarehouse(std::string warehouse_) {
	warehouse = warehouse_;
}

std::string IRCEndpointBuilder::GetWarehouse() const {
	return warehouse;
}

void IRCEndpointBuilder::SetHost(std::string host_) {
	host = host_;
}

std::string IRCEndpointBuilder::GetPrefix() const {
	return prefix;
}

void IRCEndpointBuilder::SetParam(std::string key, std::string value) {
	params[key] = value;
}

std::string IRCEndpointBuilder::GetParam(std::string key) const {
	if (params.find(key) != params.end()) {
		return params.at(key);
	}
	return "";
}

std::string IRCEndpointBuilder::GetURL() const {
	std::string ret = host;
	if (!version.empty()) {
		ret = ret + "/" + version;
	}
	// usually the warehouse is the prefix.
	if (!prefix.empty()) {
		ret = ret + "/" + prefix;
	}
	for (auto &component : path_components) {
		ret += "/" + component;
	}
	// TODO: URL encode keys and values.
	auto sep = "?";
	if (params.size() > 0) {
		for (auto &param : params) {
			auto &key = param.first;
			auto &value = param.second;
			ret += sep + key + "=" + value;
			sep = "&";
		}
	}
	return ret;
}

}