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

class Schema:
    def __init__(self, name: str, schema: Dict):
        self.name = name
        self.type = schema.get('type', 'object')
        self.required: Set[str] = set(schema.get('required', []))
        self.properties: Dict[str, Property] = {}
        self.all_of_refs: Set[str] = set()
        
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

    def generate_class_declaration(self) -> str:
        lines = [f'class {self.name} {{']
        lines.append('public:')
        lines.append(f'\tstatic {self.name} FromJSON(yyjson_val *obj);')
        lines.append('public:')

        # Special member functions
        lines.append(f'\t{self.name}();')  # Declaration only
        lines.append(f'\t{self.name}(const {self.name} &other);')  # Declaration only
        lines.append(f'\t{self.name}({self.name} &&other) noexcept;')  # Declaration only
        lines.append(f'\t{self.name}& operator=(const {self.name} &other);')  # Declaration only
        lines.append(f'\t{self.name}& operator=({self.name} &&other) noexcept;')  # Declaration only
        lines.append(f'\t~{self.name}();')  # Declaration only

        lines.append('public:')
        # Generate properties
        for prop_name, prop in self.properties.items():
            var_name = safe_cpp_name(prop_name)
            lines.append(f'\t{prop.get_cpp_type()} {var_name};')
        
        lines.append('};')
        return '\n'.join(lines)
    
    def generate_class_implementation(self) -> str:
        lines = []
        # Default constructor
        lines.append(f'{self.name}::{self.name}() = default;')
        
        # Copy constructor
        lines.append(f'{self.name}::{self.name}(const {self.name} &other) = default;')
        
        # Move constructor
        lines.append(f'{self.name}::{self.name}({self.name} &&other) noexcept = default;')
        
        # Copy assignment
        lines.append(f'{self.name}& {self.name}::operator=(const {self.name} &other) = default;')
        
        # Move assignment
        lines.append(f'{self.name}& {self.name}::operator=({self.name} &&other) noexcept = default;')
        
        # Destructor
        lines.append(f'{self.name}::~{self.name}() = default;')
        lines.extend([f'{self.name} {self.name}::FromJSON(yyjson_val *obj) {{'])
        lines.append(f'\t{self.name} result;')
        
        # Generate property parsing
        for prop_name, prop in self.properties.items():
            var_name = safe_cpp_name(prop_name)
            lines.append(f'\tauto {var_name}_val = yyjson_obj_get(obj, "{prop_name}");')
            
            parse_statement = self._get_parse_statement(var_name, prop)
            
            if prop_name in self.required:
                lines.append(f'\t\tif ({var_name}_val) {{')
                if prop.type == 'array':
                    if prop.items_type in {'string', 'integer', 'boolean'}:
                        parse_func = {
                            'string': 'yyjson_get_str',
                            'integer': 'yyjson_get_sint',
                            'boolean': 'yyjson_get_bool'
                        }[prop.items_type]
                        lines.extend([
                            '\t\t\tsize_t idx, max;',
                            '\t\t\tyyjson_val *val;',
                            f'\t\t\tyyjson_arr_foreach({var_name}_val, idx, max, val) {{',
                            f'\t\t\t\tresult.{var_name}.push_back({parse_func}(val));',
                            '\t\t\t}'
                        ])
                    else:
                        lines.extend([
                            '\t\t\tsize_t idx, max;',
                            '\t\t\tyyjson_val *val;',
                            f'\t\t\tyyjson_arr_foreach({var_name}_val, idx, max, val) {{',
                            f'\t\t\t\tresult.{var_name}.push_back({prop.items_type}::FromJSON(val));',
                            '\t\t\t}'
                        ])
                else:
                    lines.append(f'\t\t\tresult.{var_name} = {parse_statement};')
                lines.append('\t\t} else {')
                lines.append(f'\t\t\tthrow IOException("{self.name} required property \'{prop_name}\' is missing");')
                lines.append('\t\t}')
            else:
                lines.append(f'\t\tif ({var_name}_val) {{')
                if prop.type == 'array':
                    if prop.items_type in {'string', 'integer', 'boolean'}:
                        parse_func = {
                            'string': 'yyjson_get_str',
                            'integer': 'yyjson_get_sint',
                            'boolean': 'yyjson_get_bool'
                        }[prop.items_type]
                        lines.extend([
                            '\t\t\tsize_t idx, max;',
                            '\t\t\tyyjson_val *val;',
                            f'\t\t\tyyjson_arr_foreach({var_name}_val, idx, max, val) {{',
                            f'\t\t\t\tresult.{var_name}.push_back({parse_func}(val));',
                            '\t\t\t}'
                        ])
                    else:
                        lines.extend([
                            '\t\t\tsize_t idx, max;',
                            '\t\t\tyyjson_val *val;',
                            f'\t\t\tyyjson_arr_foreach({var_name}_val, idx, max, val) {{',
                            f'\t\t\t\tresult.{var_name}.push_back({prop.items_type}::FromJSON(val));',
                            '\t\t\t}'
                        ])
                else:
                    lines.append(f'\t\t\tresult.{var_name} = {parse_statement};')
                lines.append('\t\t}')

        lines.append('\t\treturn result;')
        lines.append('\t}')
        return '\n'.join(lines)

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

    def generate_header_file(self) -> str:
        lines = [
            "#pragma once",
            "",
            '#include "yyjson.hpp"',
            '#include <string>',
            '#include <vector>',
            '#include <unordered_map>',
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

        # Class definition with inline implementation
        lines.append(f'class {self.name} {{')
        lines.append('public:')
        
        # FromJSON declaration and implementation
        lines.extend([
            f'\tstatic {self.name} FromJSON(yyjson_val *obj) {{',
            f'\t\t{self.name} result;'
        ])
        
        # Generate property parsing
        for prop_name, prop in self.properties.items():
            var_name = safe_cpp_name(prop_name)
            lines.append(f'\t\tauto {var_name}_val = yyjson_obj_get(obj, "{prop_name}");')
            
            if prop.type == 'array':
                lines.append(f'\t\tif ({var_name}_val) {{')
                # First declare the variables
                lines.append('\t\t\tsize_t idx, max;')
                lines.append('\t\t\tyyjson_val *val;')
                # Then do the array iteration
                lines.append(f'\t\t\tyyjson_arr_foreach({var_name}_val, idx, max, val) {{')
                if prop.items_type in {'string', 'integer', 'boolean'}:
                    parse_func = {
                        'string': 'yyjson_get_str',
                        'integer': 'yyjson_get_sint',
                        'boolean': 'yyjson_get_bool'
                    }[prop.items_type]
                    lines.append(f'\t\t\t\tresult.{var_name}.push_back({parse_func}(val));')
                elif prop.items_type == 'object':
                    lines.append(f'\t\t\t\tresult.{var_name}.push_back(val);')
                else:
                    lines.append(f'\t\t\t\tresult.{var_name}.push_back({prop.items_type}::FromJSON(val));')
                lines.append('\t\t\t}')
                lines.append('\t\t}')
            else:
                parse_statement = self._get_parse_statement(var_name, prop)
                lines.append(f'\t\tif ({var_name}_val) {{')
                lines.append(f'\t\t\tresult.{var_name} = {parse_statement};')
                lines.append('\t\t}')
        
        lines.extend([
            '\t\treturn result;',
            '\t}',
            'public:'
        ])
        
        # Generate properties
        for prop_name, prop in self.properties.items():
            var_name = safe_cpp_name(prop_name)
            cpp_type = prop.get_cpp_type()
            lines.append(f'\t{cpp_type} {var_name};')
        
        lines.extend([
            '};',
            '',
            '} // namespace rest_api_objects',
            '} // namespace duckdb'
        ])
        
        return '\n'.join(lines)

def generate_header_declarations():
    return '''#pragma once

#include "yyjson.hpp"

using namespace duckdb_yyjson;
namespace duckdb {
namespace rest_api_objects {

template<typename T>
vector<T> parse_obj_array(yyjson_val *arr);

vector<string> parse_str_array(yyjson_val *arr);

// Forward declarations
'''

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
    schema_objects = {
        name: Schema(name, schema)
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
