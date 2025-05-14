INSERT INTO default.schema_evolve_struct_in_map VALUES
	(MAP('third',
		NAMED_STRUCT(
			'first_name', 'Ancient Being',
			'age', 9223372036854775807
		),
		'fourth',
		NAMED_STRUCT(
			'first_name', 'Bobby Droptables',
			'age', 2147483649
		)
	));