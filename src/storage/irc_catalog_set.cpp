#include "storage/irc_catalog_set.hpp"
#include "storage/irc_transaction.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "storage/irc_schema_entry.hpp"

namespace duckdb {

IRCCatalogSet::IRCCatalogSet(Catalog &catalog) : catalog(catalog) {
}

optional_ptr<CatalogEntry> IRCCatalogSet::GetEntry(ClientContext &context, const string &name) {
	LoadEntries(context);
	lock_guard<mutex> l(entry_lock);
	auto entry = entries.find(name);
	if (entry == entries.end()) {
		return nullptr;
	}
	FillEntry(context, entry->second);
	return entry->second.get();
}

void IRCCatalogSet::DropEntry(ClientContext &context, DropInfo &info) {
	EraseEntryInternal(info.name);
}

void IRCCatalogSet::EraseEntryInternal(const string &name) {
	lock_guard<mutex> l(entry_lock);
	entries.erase(name);
}

void IRCCatalogSet::Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback) {
	lock_guard<mutex> l(entry_lock);
	LoadEntries(context);
	for (auto &entry : entries) {
		callback(*entry.second);
	}
}

optional_ptr<CatalogEntry> IRCCatalogSet::CreateEntry(unique_ptr<CatalogEntry> entry) {
	auto result = entry.get();
	if (result->name.empty()) {
		throw InternalException("ICCatalogSet::CreateEntry called with empty name");
	}
	entries.insert(make_pair(result->name, std::move(entry)));
	return result;
}

void IRCCatalogSet::ClearEntries() {
	entries.clear();
}

ICInSchemaSet::ICInSchemaSet(IRCSchemaEntry &schema) : IRCCatalogSet(schema.ParentCatalog()), schema(schema) {
}

optional_ptr<CatalogEntry> ICInSchemaSet::CreateEntry(unique_ptr<CatalogEntry> entry) {
	if (!entry->internal) {
		entry->internal = schema.internal;
	}
	return IRCCatalogSet::CreateEntry(std::move(entry));
}

} // namespace duckdb
