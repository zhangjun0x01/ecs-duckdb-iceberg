import yaml
import os
from typing import Dict, List, Set
import re

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
    'override'
}


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

class Property:
    def __init__(self, name: str, schema: Dict):
        self.name = name
        self.type = schema.get('type', '')
        self.description = schema.get('description', '')
        self.ref = schema.get('$ref', '')
        self.additional_properties_type = None
        
        # Handle allOf reference
        if 'allOf' in schema:
            for sub_schema in schema['allOf']:
                if '$ref' in sub_schema:
                    self.ref = sub_schema['$ref']
                    self.type = self.ref.split('/')[-1]
                    break

        # Store items type for arrays
        self.items_type = None
        self.items_ref = None
        if self.type == 'array' and 'items' in schema:
            items = schema['items']
            self.items_type = items.get('type', '')
            self.items_ref = items.get('$ref', '')
            if self.items_ref:
                self.items_type = self.items_ref.split('/')[-1]

        # Handle additionalProperties for objects
        if self.type == 'object' and 'additionalProperties' in schema:
            additional_props = schema['additionalProperties']
            self.additional_properties_type = additional_props.get('type')

        if self.ref and not self.type:
            self.type = self.ref.split('/')[-1]

    def is_object_of_strings(self) -> bool:
        return self.type == 'object' and getattr(self, 'additional_properties_type', None) == 'string'

    def get_cpp_type(self) -> str:
        """Get the C++ type for this property."""
        if self.type == 'array':
            if self.items_type in {'string', 'integer', 'boolean'}:
                type_mapping = {
                    'string': 'string',
                    'integer': 'int64_t',
                    'boolean': 'bool'
                }
                return f'vector<{type_mapping[self.items_type]}>'
            elif self.items_type == 'object':
                return 'vector<yyjson_val *>'
            return f'vector<{self.items_type}>'
        
        type_mapping = {
            'string': 'string',
            'integer': 'int64_t',
            'boolean': 'bool',
            'object': 'yyjson_val *'
        }
        
        # Special case for objects with string additionalProperties
        if self.is_object_of_strings():
            return 'ObjectOfStrings'

        return type_mapping.get(self.type, self.type)

class SchemaType:
    def __init__(self, cpp_type: str, yyjson_check: str, yyjson_get: str):
        self.cpp_type = cpp_type
        self.yyjson_check = yyjson_check
        self.yyjson_get = yyjson_get

# Global mapping of schema types to C++ types and their yyjson handlers
TYPE_MAPPINGS = {
    'string': SchemaType('string', 'yyjson_is_str', 'yyjson_get_str'),
    'integer': {
        'int64': SchemaType('int64_t', 'yyjson_is_int', 'yyjson_get_sint'),
        'int32': SchemaType('int32_t', 'yyjson_is_int', 'yyjson_get_sint'),
        'default': SchemaType('int64_t', 'yyjson_is_int', 'yyjson_get_sint')
    },
    'number': {
        'float': SchemaType('float', 'yyjson_is_num', 'yyjson_get_real'),
        'double': SchemaType('double', 'yyjson_is_num', 'yyjson_get_real'),
        'default': SchemaType('double', 'yyjson_is_num', 'yyjson_get_real')
    },
    'boolean': SchemaType('bool', 'yyjson_is_bool', 'yyjson_get_bool'),
    'uuid': SchemaType('string', 'yyjson_is_str', 'yyjson_get_str'),
    'date': SchemaType('string', 'yyjson_is_str', 'yyjson_get_str'),
    'time': SchemaType('string', 'yyjson_is_str', 'yyjson_get_str'),
    'timestamp': SchemaType('string', 'yyjson_is_str', 'yyjson_get_str'),
    'timestamp_tz': SchemaType('string', 'yyjson_is_str', 'yyjson_get_str'),
    'binary': SchemaType('string', 'yyjson_is_str', 'yyjson_get_str'),
    'decimal': SchemaType('string', 'yyjson_is_str', 'yyjson_get_str'),
}

def get_type_mapping(schema_type: str, schema_format: str = None) -> SchemaType:
    """Get the appropriate type mapping based on type and format."""
    if schema_type not in TYPE_MAPPINGS:
        return TYPE_MAPPINGS['string']  # Default to string for unknown types
    
    mapping = TYPE_MAPPINGS[schema_type]
    if isinstance(mapping, dict):
        # Handle format-specific types
        if schema_format and schema_format in mapping:
            return mapping[schema_format]
        return mapping['default']
    return mapping

class Schema:
    def __init__(self, name: str, schema: Dict, all_schemas: Dict):
        self.name = name
        self.type = schema.get('type', 'object')
        self.required: Set[str] = set(schema.get('required', []))
        self.properties: Dict[str, Property] = {}
        self.all_of_refs: Set[str] = set()
        self.one_of_schemas: List[Dict] = []  # Replace one_of_types with one_of_schemas
        
        # Handle oneOf
        if 'oneOf' in schema:
            for sub_schema in schema['oneOf']:
                if '$ref' in sub_schema:
                    ref_path = sub_schema['$ref']
                    ref_name = ref_path.split('/')[-1]
                    ref_schema = all_schemas.get(ref_name)
                    if ref_schema:
                        self.one_of_schemas.append(ref_schema)
                else:
                    # Handle inline schema definitions
                    self.one_of_schemas.append(sub_schema)
        
        # Handle allOf
        if 'allOf' in schema:
            for sub_schema in schema['allOf']:
                if '$ref' in sub_schema:
                    ref_type = sub_schema['$ref'].split('/')[-1]
                    self.all_of_refs.add(ref_type)
                elif 'properties' in sub_schema:
                    for prop_name, prop_schema in sub_schema['properties'].items():
                        self.properties[prop_name] = Property(prop_name, prop_schema)
                if 'required' in sub_schema:
                    self.required.update(sub_schema['required'])
        
        # Handle direct properties
        if 'properties' in schema:
            for prop_name, prop_schema in schema['properties'].items():
                self.properties[prop_name] = Property(prop_name, prop_schema)

    def _get_parse_statement(self, var_name: str, prop: Property) -> str:
        """Get the parsing statement for a property."""
        if prop.type == 'array':
            if prop.items_type in {'string', 'integer', 'boolean'}:
                parse_func = {
                    'string': 'yyjson_get_str',
                    'integer': 'yyjson_get_sint',
                    'boolean': 'yyjson_get_bool'
                }[prop.items_type]
                return None  # Signal that we need special handling for arrays
            else:
                return None  # Signal that we need special handling for arrays

        type_mapping = {
            'string': f'yyjson_get_str({var_name}_val)',
            'integer': f'yyjson_get_sint({var_name}_val)',
            'boolean': f'yyjson_get_bool({var_name}_val)',
            'object': f'{var_name}_val'  # Default for objects is raw pointer
        }
        
        # Special case for objects with string additionalProperties
        if prop.type == 'object' and getattr(prop, 'additional_properties_type', None) == 'string':
            return f'parse_object_of_strings({var_name}_val)'
            
        if prop.type in type_mapping:
            return type_mapping[prop.type]
        # For custom types (refs)
        return f'{prop.type}::FromJSON({var_name}_val)'

    def get_required_includes(self) -> Set[str]:
        """Get all header files that need to be included for this schema."""
        includes = set()
        for prop in self.properties.values():
            if prop.type not in {'string', 'integer', 'boolean', 'object', 'array'}:
                includes.add(prop.type)
            if prop.type == 'array' and prop.items_type not in {'string', 'integer', 'boolean', 'object'}:
                includes.add(prop.items_type)
        includes.update(ref for ref in self.all_of_refs)
        return [f'rest_catalog/objects/{to_snake_case(x)}.hpp' for x in includes]

    def _get_underlying_type(self, schema: Dict) -> str:
        """Extract the underlying primitive type from a schema."""
        if 'type' in schema:
            return schema['type']
        
        # Handle specific schema names that imply types
        schema_type_mapping = {
            'BooleanTypeValue': 'boolean',
            'IntegerTypeValue': 'integer',
            'LongTypeValue': 'integer',
            'FloatTypeValue': 'float',
            'DoubleTypeValue': 'double',
            'DecimalTypeValue': 'decimal',
            'StringTypeValue': 'string',
            'UUIDTypeValue': 'uuid',
            'DateTypeValue': 'date',
            'TimeTypeValue': 'time',
            'TimestampTypeValue': 'timestamp',
            'TimestampTzTypeValue': 'timestamp_tz',
            'TimestampNanoTypeValue': 'timestamp',
            'TimestampTzNanoTypeValue': 'timestamp_tz',
            'FixedTypeValue': 'binary',
            'BinaryTypeValue': 'binary',
        }
        
        for name, type_name in schema_type_mapping.items():
            if name in str(schema):
                return type_name
        
        return 'unknown'

    def generate_header_file(self) -> str:
        lines = [
            "#pragma once",
            "",
            '#include "yyjson.hpp"',
            '#include "duckdb/common/string.hpp"',
            '#include "duckdb/common/vector.hpp"',
            '#include "duckdb/common/unordered_map.hpp"',
            '#include "rest_catalog/response_objects.hpp"'
        ]
        
        # Add required includes
        for include in self.get_required_includes():
            lines.append(f'#include "{include}"')
        
        lines.extend([
            "",
            "using namespace duckdb_yyjson;",
            "",
            "namespace duckdb {",
            "namespace rest_api_objects {",
            ""
        ])

        if self.one_of_schemas:
            lines.extend(self._generate_oneof_class())
        else:
            # Generate regular class
            lines.extend([
                f"class {self.name} {{",
                "public:",
                f"\tstatic {self.name} FromJSON(yyjson_val *obj) {{",
                f"\t\t{self.name} result;",
                ""
            ])

            # Generate parsing for allOf references
            for ref in self.all_of_refs:
                lines.extend([
                    f"\t\t// Parse {ref} fields",
                    f"\t\tresult.{to_snake_case(ref)} = {ref}::FromJSON(obj);",
                    ""
                ])

            # Generate parsing for properties
            for prop_name, prop in self.properties.items():
                val_name = f"{safe_cpp_name(prop_name)}_val"
                lines.extend([
                    f"\t\tauto {val_name} = yyjson_obj_get(obj, \"{prop_name}\");",
                    f"\t\tif ({val_name}) {{"
                ])

                if prop.type == 'array':
                    # First declare the variables
                    lines.append('\t\t\tsize_t idx, max;')
                    lines.append('\t\t\tyyjson_val *val;')
                    # Then do the array iteration
                    lines.append(f'\t\t\tyyjson_arr_foreach({safe_cpp_name(prop_name)}_val, idx, max, val) {{')
                    
                    if prop.items_type in {'string', 'integer', 'boolean'}:
                        parse_func = {
                            'string': 'yyjson_get_str',
                            'integer': 'yyjson_get_sint',
                            'boolean': 'yyjson_get_bool'
                        }[prop.items_type]
                        lines.append(f"\t\t\t\tresult.{safe_cpp_name(prop_name)}.push_back({parse_func}(val));")
                    elif prop.items_type == 'object':
                        lines.append(f'\t\t\t\tresult.{safe_cpp_name(prop_name)}.push_back(val);')
                    else:
                        lines.append(f"\t\t\t\tresult.{safe_cpp_name(prop_name)}.push_back({prop.items_type}::FromJSON(val));")
                    
                    lines.append("\t\t\t}")
                else:
                    parse_stmt = self._get_parse_statement(safe_cpp_name(prop_name), prop)
                    lines.append(f"\t\t\tresult.{safe_cpp_name(prop_name)} = {parse_stmt};")
                
                lines.append("\t\t}")
                if prop_name in self.required:
                    lines.append(f"\t\telse {{\n\t\t\tthrow IOException(\"{self.name} required property '{prop_name}' is missing\");\n\t\t}}")
                lines.append("")

            lines.extend([
                "\t\treturn result;",
                "\t}",
                "",
                "public:"
            ])

            # Generate member variables
            for ref in self.all_of_refs:
                lines.append(f"\t{ref} {to_snake_case(ref)};")
            
            for prop_name, prop in self.properties.items():
                cpp_type = prop.get_cpp_type()
                lines.append(f"\t{cpp_type} {safe_cpp_name(prop_name)};")

            lines.append("};")

        lines.extend([
            "} // namespace rest_api_objects",
            "} // namespace duckdb"
        ])
        
        return '\n'.join(lines)

    def _generate_oneof_class(self) -> List[str]:
        lines = [
            f"class {self.name} {{",
            "public:",
            f"\tstatic {self.name} FromJSON(yyjson_val *obj) {{",
            f"\t\t{self.name} result;"
        ]

        # format -> (type, format)
        one_of_schemas: Dict[str, object] = {}

        # Generate type checking and value assignment
        for schema in self.one_of_schemas:
            schema_type = schema.get('type')
            schema_format = schema.get('format')
            
            if schema_type in ['integer', 'number', 'boolean', 'string']:
                if schema_format in [
                    'int64',
                    'double',
                    'float',
                ]:
                    one_of_schemas[schema_format] = (schema_type, schema_format)
                elif schema_type == 'string':
                    one_of_schemas[schema_type] = (schema_type, schema_format)

        for _, value in one_of_schemas.items():
            schema_type, schema_format = value
            type_mapping = get_type_mapping(schema_type, schema_format)
            var_name = f"value_{schema_format or schema_type}"
            has_name = f"has_{schema_format or schema_type}"
            
            lines.extend([
                f"\t\tif ({type_mapping.yyjson_check}(obj)) {{",
                f"\t\t\tresult.{var_name} = {type_mapping.yyjson_get}(obj);",
                f"\t\t\tresult.{has_name} = true;",
                "\t\t}"
            ])

        lines.extend([
            "\t\treturn result;",
            "\t}",
            "",
            "public:"
        ])

        # Generate member variables
        for _, value in one_of_schemas.items():
            schema_type, schema_format = value
            type_mapping = get_type_mapping(schema_type, schema_format)
            var_name = f"value_{schema_format or schema_type}"
            has_name = f"has_{schema_format or schema_type}"
            
            lines.extend([
                f"\t{type_mapping.cpp_type} {var_name};",
                f"\tbool {has_name} = false;"
            ])

        lines.append("};")
        return lines

def generate_list_header(schema_objects: Dict[str, Schema]) -> str:
    lines = [
        "",
        "// This file is automatically generated and contains all REST API object headers",
        ""
    ]
    
    # Add includes for all generated headers
    for name in schema_objects:
        lines.append(f'#include "rest_catalog/objects/{to_snake_case(name)}.hpp"')
    
    return '\n'.join(lines)


def main():
    # Load OpenAPI spec
    with open(API_SPEC_PATH) as f:
        spec = yaml.safe_load(f)
    
    # Get schemas from components
    schemas = spec['components']['schemas']
    
    # Create schema objects with access to all schemas
    schema_objects = {
        name: Schema(name, schema, schemas)
        for name, schema in schemas.items()
    }

    # Create directory if it doesn't exist
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Generate a header file for each schema
    for name in schema_objects:
        schema = schema_objects[name]
        output_path = os.path.join(OUTPUT_DIR, f'{to_snake_case(name)}.hpp')
        with open(output_path, 'w') as f:
            f.write(schema.generate_header_file())

    with open(os.path.join(OUTPUT_DIR, 'list.hpp'), 'w') as f:
        f.write(generate_list_header(schema_objects))

if __name__ == '__main__':
    main()
