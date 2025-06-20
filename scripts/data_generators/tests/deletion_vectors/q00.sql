CREATE or REPLACE TABLE default.deletion_vectors (
	col integer
)
TBLPROPERTIES (
	'format-version'='3',
	'write.delete.mode' = 'merge-on-read',
	'write.delete.format' = 'puffin',
	'write.update.mode' = 'merge-on-read'
);
