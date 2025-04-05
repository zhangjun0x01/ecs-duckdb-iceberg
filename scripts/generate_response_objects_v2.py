import yaml
import os
from typing import Dict, List, Set, Optional
import re
from enum import Enum, auto

SCRIPT_PATH = os.path.dirname(os.path.realpath(__file__))
API_SPEC_PATH = os.path.join(SCRIPT_PATH, 'api.yaml')

OUTPUT_DIR = os.path.join(SCRIPT_PATH, '..', 'src', 'include', 'rest_catalog', 'objects')

CPP_KEYWORDS = {
    'namespace',
    'class',
    'template',
    'operator',
    'private',
    'public',
    'protected',
    'virtual',
    'default',
    'delete',
    'final',
    'override',
}

SCHEMA_BLACKLIST = set(['PrimitiveTypeValue'])

###
### FIXME:
### The following are not generated correctly:
### - DeleteFile
### - Expression
### - FetchPlanningResult
### - MetricResult (anyOf is not supported yet)
### - Metrics
### - OAuthTokenRequest
### - PlanTableScanResult
### - SnapshotReferences
### - TableUpdate
### - Term (missing 'TransformTerm' member)


def to_snake_case(name: str):
    res = ''
    prev_was_lower = False
    for x in name:
        is_lower = x.islower()
        if not is_lower and prev_was_lower:
            res += '_'
        prev_was_lower = is_lower
        res += x.lower()
    return res


def safe_cpp_name(name: str) -> str:
    """Convert property name to safe C++ variable name."""
    name = name.replace('-', '_')
    if name in CPP_KEYWORDS:
        return '_' + name
    return name


PRIMITIVE_TYPES = ['string', 'number', 'integer', 'boolean']


class Property:
    class Type(Enum):
        PRIMITIVE = auto()
        ARRAY = auto()
        OBJECT = auto()

    def __init__(self, reference: Optional[str], type: "Property.Type"):
        self.type = type
        # The name of the schema that this property has been defined by (if $ref)
        self.reference = reference


class ArrayProperty(Property):
    def __init__(self, spec: dict, reference: Optional[str] = None):
        super().__init__(reference, Property.Type.ARRAY)
        self.spec = spec
        self.item_type = None


class PrimitiveProperty(Property):
    def __init__(self, spec: dict, reference: Optional[str] = None):
        super().__init__(reference, Property.Type.PRIMITIVE)
        self.spec = spec
        self.format = None


class ObjectProperty(Property):
    def __init__(self, spec: dict, reference: Optional[str] = None):
        super().__init__(reference, Property.Type.OBJECT)
        self.spec = spec
        self.required = []
        self.properties = Dict[str, Property]
        self.discriminator = None


class ResponseObjectsGenerator:
    def __init__(self, path: str):
        self.path = path
        self.parsed_schemas: Dict[str, Property] = {}
        self.parsed_responses: Dict[str, Response] = {}
        # Since schemas reference other schemas and are potentially recursive
        # We want to keep track of the schemas that are currently being parsed
        self.schemas_being_parsed: Set[str] = set()

        # Load OpenAPI spec
        with open(API_SPEC_PATH) as f:
            spec = yaml.safe_load(f)

        self.schemas = spec['components']['schemas']
        self.responses = spec['components']['responses']

    def generate_object_property(self, spec: dict, result: Property):
        # For polymorphic types, this defines a mapping based on the content of a property
        result.discriminator = spec.get('discriminator')
        # Get the required properties of the schema
        result.required = spec.get('required')
        # Get the defined properties of the schema
        result.properties = spec.get('properties')

    def generate_primitive_property(self, spec: dict, result: Property):
        primitive_type = spec['type']
        format = spec.get('format')
        assert primitive_type in PRIMITIVE_TYPES

    def generate_array_property(self, spec: dict, result: Property):
        pass

    def generate_schema(self, name: str):
        if name in self.parsed_schemas:
            return
        if name in self.schemas_being_parsed:
            print(f"{name} is a recursive schema definition!")
            print(f"schemas in the stack trace: {list(self.schemas_being_parsed)}")
            exit(1)
        if name not in self.schemas:
            print(f"{name} is not a schema in the spec!")
            exit(1)

        self.schemas_being_parsed.add(name)
        schema = self.schemas[name]

        # default to 'object' (see 'AssertViewUUID')
        schema_type = schema.get('type', 'object')

        one_of = schema.get('oneOf')
        all_of = schema.get('allOf')
        any_of = schema.get('anyOf')
        if one_of:
            if schema_type != 'object':
                print(f"{name} contains both 'oneOf' and a non-object 'type' ({schema_type})")
                exit(1)
            assert 'allOf' not in schema
            assert 'anyOf' not in schema
        elif all_of:
            if schema_type != 'object':
                print(f"{name} contains both 'allOf' and a non-object 'type' ({schema_type})")
                exit(1)
            assert 'oneOf' not in schema
            assert 'anyOf' not in schema
        elif any_of:
            assert 'allOf' not in schema
            assert 'oneOf' not in schema
        else:
            if schema_type == 'object':
                result = ObjectProperty(schema, name)
                self.generate_object_property(schema, result)
            elif schema_type == 'array':
                result = ArrayProperty(schema, name)
                self.generate_array_property(schema, result)
            elif schema_type in PRIMITIVE_TYPES:
                result = PrimitiveProperty(schema, name)
                self.generate_primitive_property(schema, result)
            else:
                print(f"Schema '{name} has unrecognized type: '{schema_type}'!")
                exit(1)

            self.schemas_being_parsed.remove(name)
            self.parsed_schemas[name] = result

    def generate_all_schemas(self):
        for name in self.schemas:
            self.generate_schema(name)


# def array_cpp_type(item_type):
#    if item_type in {'string', 'integer', 'boolean'}:
#        type_mapping = {'string': 'string', 'integer': 'int64_t', 'boolean': 'bool'}
#        return f'vector<{type_mapping[item_type]}>'
#    elif item_type == 'object':
#        return 'vector<yyjson_val *>'
#    return f'vector<{item_type}>'


# class Property:
#    def __init__(self, name: str, schema: Dict):
#        self.name = name
#        self.type = schema.get('type', '')
#        self.description = schema.get('description', '')
#        self.ref = schema.get('$ref', '')
#        self.additional_properties_type = None

#        # Handle allOf reference
#        if 'allOf' in schema:
#            for sub_schema in schema['allOf']:
#                if '$ref' in sub_schema:
#                    self.ref = sub_schema['$ref']
#                    self.type = self.ref.split('/')[-1]
#                    break

#        # Store items type for arrays
#        self.items_type = None
#        self.items_ref = None
#        if self.type == 'array' and 'items' in schema:
#            items = schema['items']
#            self.items_type = items.get('type', '')
#            self.items_ref = items.get('$ref', '')
#            if self.items_ref:
#                self.items_type = self.items_ref.split('/')[-1]

#        # Handle additionalProperties for objects
#        if self.type == 'object' and 'additionalProperties' in schema:
#            additional_props = schema['additionalProperties']
#            self.additional_properties_type = additional_props.get('type')

#        if self.ref and not self.type:
#            self.type = self.ref.split('/')[-1]

#    def is_object_of_strings(self) -> bool:
#        return self.type == 'object' and getattr(self, 'additional_properties_type', None) == 'string'

#    def get_cpp_type(self) -> str:
#        """Get the C++ type for this property."""
#        if self.type == 'array':
#            return array_cpp_type(self.items_type)

#        type_mapping = {'string': 'string', 'integer': 'int64_t', 'boolean': 'bool', 'object': 'yyjson_val *'}

#        # Special case for objects with string additionalProperties
#        if self.is_object_of_strings():
#            return 'case_insensitive_map_t<string>'

#        return type_mapping.get(self.type, self.type)


# class SchemaType:
#    def __init__(self, cpp_type: str, yyjson_check: str, yyjson_get: str):
#        self.cpp_type = cpp_type
#        self.yyjson_check = yyjson_check
#        self.yyjson_get = yyjson_get


## Global mapping of schema types to C++ types and their yyjson handlers
# TYPE_MAPPINGS = {
#    'string': SchemaType('string', 'yyjson_is_str', 'yyjson_get_str'),
#    'integer': {
#        'int64': SchemaType('int64_t', 'yyjson_is_int', 'yyjson_get_sint'),
#        'int32': SchemaType('int32_t', 'yyjson_is_int', 'yyjson_get_sint'),
#        'default': SchemaType('int64_t', 'yyjson_is_int', 'yyjson_get_sint'),
#    },
#    'number': {
#        'float': SchemaType('float', 'yyjson_is_real', 'yyjson_get_real'),
#        'double': SchemaType('double', 'yyjson_is_real', 'yyjson_get_real'),
#        'default': SchemaType('double', 'yyjson_is_real', 'yyjson_get_real'),
#    },
#    'boolean': SchemaType('bool', 'yyjson_is_bool', 'yyjson_get_bool'),
#    'uuid': SchemaType('string', 'yyjson_is_str', 'yyjson_get_str'),
#    'date': SchemaType('string', 'yyjson_is_str', 'yyjson_get_str'),
#    'time': SchemaType('string', 'yyjson_is_str', 'yyjson_get_str'),
#    'timestamp': SchemaType('string', 'yyjson_is_str', 'yyjson_get_str'),
#    'timestamp_tz': SchemaType('string', 'yyjson_is_str', 'yyjson_get_str'),
#    'binary': SchemaType('string', 'yyjson_is_str', 'yyjson_get_str'),
#    'decimal': SchemaType('string', 'yyjson_is_str', 'yyjson_get_str'),
# }


# def get_type_mapping(schema_type: str, schema_format: str = None) -> SchemaType:
#    """Get the appropriate type mapping based on type and format."""
#    if schema_type not in TYPE_MAPPINGS:
#        return TYPE_MAPPINGS['string']  # Default to string for unknown types

#    mapping = TYPE_MAPPINGS[schema_type]
#    if isinstance(mapping, dict):
#        # Handle format-specific types
#        if schema_format and schema_format in mapping:
#            return mapping[schema_format]
#        return mapping['default']
#    return mapping


# def generate_array_loop(indent: int, array_name, destination_name, item_type):
#    res = []
#    tab = '\t'
#    # First declare the variables
#    res.append(f'{tab * indent}size_t idx, max;')
#    res.append(f'{tab * indent}yyjson_val *val;')
#    # Then do the array iteration
#    res.append(f'{tab * indent}yyjson_arr_foreach({array_name}, idx, max, val) {{')

#    if item_type in {'string', 'integer', 'boolean'}:
#        parse_func = {'string': 'yyjson_get_str', 'integer': 'yyjson_get_sint', 'boolean': 'yyjson_get_bool'}[item_type]
#        res.append(f"{tab * (indent + 1)}result.{destination_name}.push_back({parse_func}(val));")
#    elif item_type == 'object':
#        res.append(f'{tab * (indent + 1)}result.{destination_name}.push_back(val);')
#    else:
#        res.append(f"{tab * (indent + 1)}result.{destination_name}.push_back({item_type}::FromJSON(val));")
#    res.append(f'{tab * indent}}}')
#    return res


# def create_schema(name: str, schema: Dict, all_schemas: Dict, parsed_schemas: Dict[str, 'Schema']) -> 'Schema':
#    """Factory function to create Schema objects, handling circular dependencies."""
#    # If schema is already parsed, return it
#    if name in parsed_schemas:
#        return parsed_schemas[name]

#    # Create new schema and add it to parsed_schemas before processing
#    # This handles potential circular dependencies
#    schema_obj = Schema(name, schema, all_schemas, parsed_schemas)
#    parsed_schemas[name] = schema_obj
#    return schema_obj


# class Schema:
#    def __init__(self, name: str, schema: Dict, all_schemas: Dict, parsed_schemas: Dict[str, 'Schema']):
#        self.name = name
#        self.schema = schema  # Store the original schema dictionary
#        self.type = schema.get('type', 'object')
#        self.format = schema.get('format')  # Add format support
#        self.required: Set[str] = set(schema.get('required', []))
#        self.properties: Dict[str, Property] = {}
#        self.all_of_refs: Set[str] = set()
#        self.one_of_schemas: List[Schema] = []
#        self.any_of_schemas: List[Schema] = []
#        self.item_type: Optional[str] = None

#        # Handle oneOf
#        if 'oneOf' in schema:
#            for sub_schema in schema['oneOf']:
#                if '$ref' in sub_schema:
#                    ref_path = sub_schema['$ref']
#                    ref_name = ref_path.split('/')[-1]
#                    if ref_name in all_schemas:
#                        ref_schema = create_schema(ref_name, all_schemas[ref_name], all_schemas, parsed_schemas)
#                        self.one_of_schemas.append(ref_schema)
#                else:
#                    # Handle inline schema definitions
#                    inline_name = f"{name}_Inline_{len(self.one_of_schemas)}"
#                    inline_schema = create_schema(inline_name, sub_schema, all_schemas, parsed_schemas)
#                    self.one_of_schemas.append(inline_schema)

#        # Handle anyOf
#        if 'anyOf' in schema:
#            for sub_schema in schema['anyOf']:
#                if '$ref' in sub_schema:
#                    ref_path = sub_schema['$ref']
#                    ref_name = ref_path.split('/')[-1]
#                    if ref_name in all_schemas:
#                        ref_schema = create_schema(ref_name, all_schemas[ref_name], all_schemas, parsed_schemas)
#                        self.any_of_schemas.append(ref_schema)
#                else:
#                    inline_name = f"{name}_Inline_{len(self.any_of_schemas)}"
#                    inline_schema = create_schema(inline_name, sub_schema, all_schemas, parsed_schemas)
#                    self.any_of_schemas.append(inline_schema)

#        # Handle allOf
#        if 'allOf' in schema:
#            for sub_schema in schema['allOf']:
#                if '$ref' in sub_schema:
#                    ref_type = sub_schema['$ref'].split('/')[-1]
#                    self.all_of_refs.add(ref_type)
#                elif 'properties' in sub_schema:
#                    for prop_name, prop_schema in sub_schema['properties'].items():
#                        self.properties[prop_name] = Property(prop_name, prop_schema)
#                if 'required' in sub_schema:
#                    self.required.update(sub_schema['required'])

#        if self.type == 'array':
#            self.item_type = schema.get('items').get('type')

#        # Handle direct properties
#        if 'properties' in schema:
#            for prop_name, prop_schema in schema['properties'].items():
#                self.properties[prop_name] = Property(prop_name, prop_schema)

#    def _get_parse_statement(self, var_name: str, prop: Property) -> str:
#        """Get the parsing statement for a property."""
#        assert prop.type != 'array'

#        type_mapping = {
#            'string': f'yyjson_get_str({var_name}_val)',
#            'integer': f'yyjson_get_sint({var_name}_val)',
#            'boolean': f'yyjson_get_bool({var_name}_val)',
#            'object': f'{var_name}_val',  # Default for objects is raw pointer
#        }

#        # Special case for objects with string additionalProperties
#        if prop.type == 'object' and getattr(prop, 'additional_properties_type', None) == 'string':
#            return f'parse_object_of_strings({var_name}_val)'

#        if prop.type in type_mapping:
#            return type_mapping[prop.type]
#        # For custom types (refs)
#        return f'{prop.type}::FromJSON({var_name}_val)'

#    def get_required_includes(self) -> Set[str]:
#        """Get all header files that need to be included for this schema."""
#        includes = set()
#        for prop in self.properties.values():
#            if prop.type not in {'string', 'integer', 'boolean', 'object', 'array'}:
#                includes.add(prop.type)
#            if prop.type == 'array' and prop.items_type not in {'string', 'integer', 'boolean', 'object'}:
#                includes.add(prop.items_type)
#        includes.update(ref for ref in self.all_of_refs)
#        return [f'rest_catalog/objects/{to_snake_case(x)}.hpp' for x in includes]

#    def generate_header_file(self) -> str:
#        lines = [
#            "#pragma once",
#            "",
#            '#include "yyjson.hpp"',
#            '#include "duckdb/common/string.hpp"',
#            '#include "duckdb/common/vector.hpp"',
#            '#include "duckdb/common/case_insensitive_map.hpp"',
#            '#include "rest_catalog/response_objects.hpp"',
#        ]

#        # Add required includes
#        for include in sorted(self.get_required_includes()):
#            lines.append(f'#include "{include}"')

#        lines.extend(
#            ["", "using namespace duckdb_yyjson;", "", "namespace duckdb {", "namespace rest_api_objects {", ""]
#        )

#        # Check if this is a primitive type schema first
#        if self.type in TYPE_MAPPINGS and not self.properties and not self.one_of_schemas:
#            type_mapping = get_type_mapping(self.type)
#            lines.extend(
#                [
#                    f"class {self.name} {{",
#                    "public:",
#                    f"\tstatic {self.name} FromJSON(yyjson_val *obj) {{",
#                    f"\t\t{self.name} result;",
#                    f"\t\tresult.value = {type_mapping.yyjson_get}(obj);",
#                    "\t\treturn result;",
#                    "\t}",
#                    "",
#                    "public:",
#                    f"\t{type_mapping.cpp_type} value;",
#                    "};",
#                ]
#            )
#        # Handle array types with primitive items
#        elif self.type == 'array':
#            # Generate regular class
#            lines.extend(
#                [
#                    f"class {self.name} {{",
#                    "public:",
#                    f"\tstatic {self.name} FromJSON(yyjson_val *obj) {{",
#                    f"\t\t{self.name} result;",
#                ]
#            )
#            lines.extend(generate_array_loop(2, 'obj', 'value', self.item_type))
#            lines.extend(
#                ["\t\treturn result;", "\t}", "", "public:", f"\t{array_cpp_type(self.item_type)} value;", "};"]
#            )

#        elif self.one_of_schemas:
#            lines.extend(self._generate_oneof_class())
#        elif self.any_of_schemas:
#            lines.extend(self._generate_anyof_class())
#        else:
#            # Generate regular class
#            lines.extend(
#                [
#                    f"class {self.name} {{",
#                    "public:",
#                    f"\tstatic {self.name} FromJSON(yyjson_val *obj) {{",
#                    f"\t\t{self.name} result;",
#                    "",
#                ]
#            )

#            # Generate parsing for allOf references
#            for ref in sorted(self.all_of_refs):
#                lines.extend(
#                    [f"\t\t// Parse {ref} fields", f"\t\tresult.{to_snake_case(ref)} = {ref}::FromJSON(obj);", ""]
#                )

#            # Generate parsing for properties
#            for prop_name, prop in sorted(self.properties.items()):
#                val_name = f"{safe_cpp_name(prop_name)}_val"
#                lines.extend(
#                    [f"\t\tauto {val_name} = yyjson_obj_get(obj, \"{prop_name}\");", f"\t\tif ({val_name}) {{"]
#                )

#                if prop.type == 'array':
#                    lines.extend(
#                        generate_array_loop(
#                            3, f'{safe_cpp_name(prop_name)}_val', safe_cpp_name(prop_name), prop.items_type
#                        )
#                    )
#                else:
#                    parse_stmt = self._get_parse_statement(safe_cpp_name(prop_name), prop)
#                    lines.append(f"\t\t\tresult.{safe_cpp_name(prop_name)} = {parse_stmt};")

#                lines.append("\t\t}")
#                if prop_name in self.required:
#                    lines.append(
#                        f"\t\telse {{\n\t\t\tthrow IOException(\"{self.name} required property '{prop_name}' is missing\");\n\t\t}}"
#                    )
#                lines.append("")

#            lines.extend(["\t\treturn result;", "\t}", "", "public:"])

#            # Generate member variables
#            for ref in sorted(self.all_of_refs):
#                lines.append(f"\t{ref} {to_snake_case(ref)};")

#            for prop_name, prop in sorted(self.properties.items()):
#                cpp_type = prop.get_cpp_type()
#                lines.append(f"\t{cpp_type} {safe_cpp_name(prop_name)};")

#            lines.append("};")

#        lines.extend(["} // namespace rest_api_objects", "} // namespace duckdb"])

#        return '\n'.join(lines)

#    def _generate_oneof_class(self) -> List[str]:
#        # If there's only one schema in oneOf, we can parse it directly
#        if len(self.one_of_schemas) == 1:
#            schema = self.one_of_schemas[0]
#            lines = [
#                f"class {self.name} {{",
#                "public:",
#                f"\tstatic {self.name} FromJSON(yyjson_val *obj) {{",
#                f"\t\t{self.name} result;",
#                f"\t\tresult.{to_snake_case(schema.name)} = {schema.name}::FromJSON(obj);",
#                f"\t\tresult.has_{to_snake_case(schema.name)} = true;",
#                "\t\treturn result;",
#                "\t}",
#                "",
#                "public:",
#                f"\t{schema.name} {to_snake_case(schema.name)};",
#                f"\tbool has_{to_snake_case(schema.name)} = false;",
#                "};",
#            ]
#            return lines

#        lines = [
#            f"class {self.name} {{",
#            "public:",
#            f"\tstatic {self.name} FromJSON(yyjson_val *obj) {{",
#            f"\t\t{self.name} result;",
#        ]

#        # Generate object type checks with else if chain
#        if 'discriminator' in self.schema:
#            discriminator = self.schema['discriminator']
#            property_name = discriminator['propertyName']
#            mapping = discriminator['mapping']

#            lines.append(f"\t\tauto discriminator_val = yyjson_obj_get(obj, \"{property_name}\");")

#            first = True
#            for value, ref in sorted(mapping.items()):
#                schema_name = ref.split('/')[-1]
#                if first:
#                    lines.append(
#                        f"\t\tif (discriminator_val && strcmp(yyjson_get_str(discriminator_val), \"{value}\") == 0) {{"
#                    )
#                    first = False
#                else:
#                    lines.append(
#                        f"\t\telse if (discriminator_val && strcmp(yyjson_get_str(discriminator_val), \"{value}\") == 0) {{"
#                    )

#                lines.extend(
#                    [
#                        f"\t\t\tresult.{to_snake_case(schema_name)} = {schema_name}::FromJSON(obj);",
#                        f"\t\t\tresult.has_{to_snake_case(schema_name)} = true;",
#                        "\t\t}",
#                    ]
#                )
#        else:
#            lines.append("\t\tif (yyjson_is_obj(obj)) {")
#            lines.append("\t\t\tauto type_val = yyjson_obj_get(obj, \"type\");")

#            first = True
#            for schema in self.one_of_schemas:
#                if 'properties' in schema.schema and 'type' in schema.schema['properties']:
#                    type_prop = schema.schema['properties']['type']
#                    if 'const' in type_prop:
#                        type_const = type_prop['const']
#                        if first:
#                            lines.append(
#                                f"\t\t\tif (type_val && strcmp(yyjson_get_str(type_val), \"{type_const}\") == 0) {{"
#                            )
#                            first = False
#                        else:
#                            lines.append(
#                                f"\t\t\telse if (type_val && strcmp(yyjson_get_str(type_val), \"{type_const}\") == 0) {{"
#                            )

#                        lines.extend(
#                            [
#                                f"\t\t\t\tresult.{to_snake_case(schema.name)} = {schema.name}::FromJSON(obj);",
#                                f"\t\t\t\tresult.has_{to_snake_case(schema.name)} = true;",
#                                "\t\t\t}",
#                            ]
#                        )
#            lines.append("\t\t}")

#        # Add the else clause with error
#        lines.append(f"\t\telse {{")
#        lines.append(f"\t\t\tthrow IOException(\"{self.name} failed to parse, none of the accepted schemas found\");")
#        lines.append("\t\t}")

#        lines.extend(["\t\treturn result;", "\t}", "", "public:"])

#        # Generate member variables
#        for schema in self.one_of_schemas:
#            lines.extend(
#                [f"\t{schema.name} {to_snake_case(schema.name)};", f"\tbool has_{to_snake_case(schema.name)} = false;"]
#            )

#        lines.append("};")
#        return lines

#    def _generate_anyof_class(self) -> List[str]:
#        lines = [
#            f"class {self.name} {{",
#            "public:",
#            f"\tstatic {self.name} FromJSON(yyjson_val *obj) {{",
#            f"\t\t{self.name} result;",
#            "\t\tif (yyjson_is_obj(obj)) {",
#        ]

#        # For each schema in anyOf, check its identifying fields
#        for schema in self.any_of_schemas:
#            # Get required fields for this schema
#            required_fields = schema.required if hasattr(schema, 'required') else []

#            # Generate field checks
#            field_checks = []
#            for field in required_fields:
#                field_checks.append(f'yyjson_obj_get(obj, "{field}")')

#            if field_checks:
#                lines.append(f"\t\t\tif ({' && '.join(sorted(field_checks))}) {{")
#                lines.append(f"\t\t\t\tresult.{to_snake_case(schema.name)} = {schema.name}::FromJSON(obj);")
#                lines.append(f"\t\t\t\tresult.has_{to_snake_case(schema.name)} = true;")
#                lines.append("\t\t\t}")

#        # Add validation that at least one schema matched
#        lines.extend(
#            [
#                "\t\t\tif (!("
#                + " || ".join([f"result.has_{to_snake_case(s.name)}" for s in self.any_of_schemas])
#                + ")) {",
#                f'\t\t\t\tthrow IOException("{self.name} failed to parse, none of the accepted schemas found");',
#                "\t\t\t}",
#                "\t\t} else {",
#                f'\t\t\tthrow IOException("{self.name} must be an object");',
#                "\t\t}",
#                "\t\treturn result;",
#                "\t}",
#                "",
#                "public:",
#            ]
#        )

#        # Generate member variables
#        for schema in self.any_of_schemas:
#            lines.extend(
#                [f"\t{schema.name} {to_snake_case(schema.name)};", f"\tbool has_{to_snake_case(schema.name)} = false;"]
#            )

#        lines.append("};")
#        return lines


# def generate_list_header(schema_objects: Dict[str, Schema]) -> str:
#    lines = ["", "// This file is automatically generated and contains all REST API object headers", ""]

#    # Add includes for all generated headers
#    for name in schema_objects:
#        lines.append(f'#include "rest_catalog/objects/{to_snake_case(name)}.hpp"')

#    return '\n'.join(lines)


def main():
    # Load OpenAPI spec
    with open(API_SPEC_PATH) as f:
        spec = yaml.safe_load(f)

    schemas = spec['components']['schemas']
    parsed_schemas: Dict[str, Schema] = {}

    # Create schema objects with access to all schemas
    for name, schema in schemas.items():
        create_schema(name, schema, schemas, parsed_schemas)

    # Create directory if it doesn't exist
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Generate a header file for each schema
    for name, schema in parsed_schemas.items():
        if name in SCHEMA_BLACKLIST:
            # We don't want to generate this, this file is written/edited manually
            continue
        output_path = os.path.join(OUTPUT_DIR, f'{to_snake_case(name)}.hpp')
        with open(output_path, 'w') as f:
            f.write(schema.generate_header_file())

    with open(os.path.join(OUTPUT_DIR, 'list.hpp'), 'w') as f:
        f.write(generate_list_header(parsed_schemas))


if __name__ == '__main__':
    # main()
    generator = ResponseObjectsGenerator(API_SPEC_PATH)
    generator.generate_all_schemas()
