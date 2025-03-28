#include "url_utils.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

void IRCEndpointBuilder::AddPathComponent(std::string component) {
	if (!component.empty()) {
		path_components.push_back(component);
	}
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

const std::unordered_map<std::string, std::string> IRCEndpointBuilder::GetParams() {
	return params;
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

	// encode params
	auto sep = "?";
	if (params.size() > 0) {
		for (auto &param : params) {
			auto key = StringUtil::URLEncode(param.first);
			auto value = StringUtil::URLEncode(param.second);
			ret += sep + key + "=" + value;
			sep = "&";
		}
	}
	return ret;
}

} // namespace duckdb
