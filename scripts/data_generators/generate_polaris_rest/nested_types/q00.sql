Drop table if exists nested_types;
CREATE TABLE nested_types (
    id INT,
    name STRING,
    address STRUCT<
        street: STRING,
        city: STRING,
        zip: STRING
    >,
    phone_numbers ARRAY<STRING>,
    metadata MAP<STRING, STRING>
);