CREATE OR REPLACE TABLE default.schema_evolve_struct_in_map (
	preferences MAP<STRING, STRUCT<first_name: STRING, age: INTEGER>>
)
TBLPROPERTIES (
	'format-version'='2',
	'write.update.mode'='merge-on-read'
);
