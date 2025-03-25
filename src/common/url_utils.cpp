
#include "url_utils.hpp"

#include "duckdb/common/string_util.hpp"

namespace duckdb {

void IRCEndpointBuilder::AddPathComponent(const string &component) {
	path_components.push_back(component);
}

void IRCEndpointBuilder::AddQueryParameter(const string &key, const string &value) {
	query_parameters.emplace_back(key, value);
}

void IRCEndpointBuilder::SetPrefix(const string &prefix_) {
	prefix = prefix_;
}

string IRCEndpointBuilder::GetHost() const {
	return host;
}

void IRCEndpointBuilder::SetVersion(const string &version_) {
	version = version_;
}

string IRCEndpointBuilder::GetVersion() const {
	return version;
}

void IRCEndpointBuilder::SetWarehouse(const string &warehouse_) {
	warehouse = warehouse_;
}

string IRCEndpointBuilder::GetWarehouse() const {
	return warehouse;
}

void IRCEndpointBuilder::SetHost(const string &host_) {
	host = host_;
}

string IRCEndpointBuilder::GetPrefix() const {
	return prefix;
}

string IRCEndpointBuilder::GetURL() const {
	string ret = host;
	if (!version.empty()) {
		ret = ret + "/" + version;
	}
	// usually the warehouse is the prefix.
	if (prefix.empty() && !warehouse.empty()) {
		ret = ret + "/" + warehouse;
	}
	else if (!prefix.empty()) {
		ret = ret + "/" + prefix;
	}
	for (auto &component : path_components) {
		ret += "/" + component;
	}
	if (!query_parameters.empty()) {
		ret += "?";
		vector<string> parameters;
		for (auto &query_parameter : query_parameters) {
			parameters.push_back(StringUtil::Format("%s=%s", query_parameter.key, query_parameter.value));
			ret += StringUtil::Join(parameters, "&");
		}
	}
	return ret;
}

}
