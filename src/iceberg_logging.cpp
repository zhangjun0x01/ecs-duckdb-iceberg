#include "iceberg_logging.hpp"

namespace duckdb {

IcebergLogType::IcebergLogType() : LogType(NAME, LEVEL, GetLogType()) {
}

} // namespace duckdb
