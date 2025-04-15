DROP TABLE IF EXISTS quickstart_table;
CREATE TABLE IF NOT EXISTS quickstart_table (
     id BIGINT, data STRING
)
USING ICEBERG;