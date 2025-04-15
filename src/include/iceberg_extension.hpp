#pragma once

#include "duckdb.hpp"

namespace duckdb {

static bool __AVRO_LOADED__ = false;

class IcebergExtension : public Extension {
public:
	void Load(DuckDB &db) override;
	std::string Name() override;
};

} // namespace duckdb
