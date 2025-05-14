INSERT INTO default.schema_evolve_struct_in_list VALUES
	(ARRAY(
		NAMED_STRUCT(
			'first_name', 'Alice',
			'age', 43
		),
		NAMED_STRUCT(
			'first_name', 'Bob',
			'age', 35
		)
	));