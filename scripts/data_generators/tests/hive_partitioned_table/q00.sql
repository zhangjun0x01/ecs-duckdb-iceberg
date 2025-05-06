CREATE OR REPLACE TABLE default.hive_partitioned_table (
    event_date DATE,
    user_id BIGINT,
    event_type STRING
)
USING iceberg
PARTITIONED BY (event_date)  -- Identity transform on event_date
TBLPROPERTIES (
    'format-version' = '2',
    'write.update.mode' = 'merge-on-read'
);
