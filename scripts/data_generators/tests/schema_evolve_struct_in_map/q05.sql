INSERT INTO default.schema_evolve_struct_in_map VALUES
	(MAP(
		'fifth',
		NAMED_STRUCT(
			'first_name', 'Hello',
			'age', 9223372036854775807,
			'last_name', 'World'
		)
	));