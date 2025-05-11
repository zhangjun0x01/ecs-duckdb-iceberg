CREATE OR REPLACE TABLE default.schema_evolve_struct_in_list (
	tags ARRAY<STRUCT<first_name: STRING, age: INTEGER>>
)
TBLPROPERTIES (
	'format-version'='2',
	'write.update.mode'='merge-on-read'
);
