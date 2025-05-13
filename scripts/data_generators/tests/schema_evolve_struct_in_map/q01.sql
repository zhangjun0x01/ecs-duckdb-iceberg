INSERT INTO default.schema_evolve_struct_in_map VALUES
	(MAP('first',
		NAMED_STRUCT(
			'first_name', 'Alice',
			'age', 43
		),
		'second',
		NAMED_STRUCT(
			'first_name', 'Bob',
			'age', 35
		)
	));