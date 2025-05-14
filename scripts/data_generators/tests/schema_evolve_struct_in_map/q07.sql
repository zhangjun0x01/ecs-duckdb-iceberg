INSERT INTO default.schema_evolve_struct_in_map VALUES
	(MAP(
		'sixth',
		NAMED_STRUCT(
			'given_name', 'Duck',
			'age', 5,
			'last_name', 'DB'
		)
	));