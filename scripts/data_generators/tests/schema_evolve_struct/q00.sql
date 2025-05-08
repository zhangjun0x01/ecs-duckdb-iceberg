CREATE OR REPLACE TABLE default.schema_evolve_struct (
	user_id INT,
	user_details STRUCT<first_name: STRING, last_name: STRING>
)
TBLPROPERTIES (
	'format-version'='2',
	'write.update.mode'='merge-on-read'
);
