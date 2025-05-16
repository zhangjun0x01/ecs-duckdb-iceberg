#include "storage/irc_authorization.hpp"
#include "api_utils.hpp"
#include "storage/authorization/oauth2.hpp"

namespace duckdb {

string IRCAuthorization::TypeToString(IRCAuthorizationType type) {
	switch (type) {
	case IRCAuthorizationType::OAUTH2: {
		return "OAUTH2";
	}
	case IRCAuthorizationType::SIGV4: {
		return "SIGV4";
	}
	default:
		throw NotImplementedException("IRCAuthorization::TypeToString type (%d)", static_cast<uint8_t>(type));
	}
}

IRCAuthorizationType IRCAuthorization::TypeFromString(const string &type) {
	static const case_insensitive_map_t<IRCAuthorizationType> mapping {{"oauth2", IRCAuthorizationType::OAUTH2},
	                                                                   {"sigv4", IRCAuthorizationType::SIGV4}};

	for (auto it : mapping) {
		if (StringUtil::CIEquals(it.first, type)) {
			return it.second;
		}
	}

	set<string> accepted_options;
	for (auto it : mapping) {
		accepted_options.insert(it.first);
	}
	throw InvalidConfigurationException("'authorization_type' '%s' is not supported, valid options are: %s", type,
	                                    StringUtil::Join(accepted_options, ", "));
}

} // namespace duckdb
