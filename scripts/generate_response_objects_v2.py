import yaml
import os
from typing import Dict, List, Set

SCRIPT_PATH = os.path.dirname(os.path.realpath(__file__))
API_SPEC_PATH = os.path.join(SCRIPT_PATH, 'api.yaml')

HEADER_PATH = os.path.join(SCRIPT_PATH, '..', 'src', 'include', 'rest_catalog', 'response_objects.hpp')

CPP_KEYWORDS = {'namespace', 'class', 'template', 'operator', 'private', 'public', 
                'protected', 'virtual', 'default', 'delete', 'final', 'override'}

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
        # Store items type for arrays
        self.items_type = None
        self.items_ref = None
        if self.type == 'array' and 'items' in schema:
            items = schema['items']
            self.items_type = items.get('type', '')
            self.items_ref = items.get('$ref', '')
            if self.items_ref:
                self.items_type = self.items_ref.split('/')[-1]

        if self.ref:
            self.type = self.ref.split('/')[-1]
        
    def get_cpp_type(self) -> str:
        type_mapping = {
            'string': 'string',
            'integer': 'int64_t',
            'boolean': 'bool',
            'object': 'ObjectOfStrings'
        }
        if self.type == 'array':
            if self.items_type == 'string':
                return 'vector<string>'
            elif self.items_type in type_mapping:
                return f'vector<{type_mapping[self.items_type]}>'
            else:
                return f'vector<{self.items_type}>'
        if self.type in type_mapping:
            return type_mapping[self.type]
        return self.type  # For custom types (refs)

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
            
            if prop_name in self.required:
                lines.extend([
                    f'\tif ({var_name}_val) {{',
                    f'\t\tresult.{var_name} = {self._get_parse_statement(var_name, prop)};',
                    '\t} else {',
                    f'\t\tthrow IOException("{self.name} required property \'{prop_name}\' is missing");',
                    '\t}'
                ])
            else:
                lines.extend([
                    f'\tif ({var_name}_val) {{',
                    f'\t\tresult.{var_name} = {self._get_parse_statement(var_name, prop)};',
                    '\t}'
                ])
        
        lines.append('\treturn result;')
        lines.append('}')
        return '\n'.join(lines)

    def _get_parse_statement(self, var_name: str, prop: Property) -> str:
        type_mapping = {
            'string': f'yyjson_get_str({var_name}_val)',
            'integer': f'yyjson_get_sint({var_name}_val)',
            'boolean': f'yyjson_get_bool({var_name}_val)',
            'object': f'parse_object_of_strings({var_name}_val)'
        }

        cpp_types = {
            'integer': 'int64_t',
            'string': 'string',
            'boolean': 'bool'
        }

        if prop.type == 'array':
            if prop.items_type == 'string':
                return f'parse_str_array({var_name}_val)'
            elif prop.items_type in cpp_types:
                return f'parse_obj_array<{cpp_types[prop.items_type]}>({var_name}_val)'
            else:
                return f'parse_obj_array<{prop.items_type}>({var_name}_val)'

        if prop.type in type_mapping:
            return type_mapping[prop.type]
        # For custom types (refs)
        return f'{prop.type}::FromJSON({var_name}_val)'

    def get_required_includes(self) -> Set[str]:
        """Get all header files that need to be included for this schema."""
        includes = set()
        for prop in self.properties.values():
            if prop.type not in {'string', 'integer', 'boolean', 'object', 'array'}:
                includes.add(f"rest_catalog/objects/{prop.type}.hpp")
            if prop.type == 'array' and prop.items_type not in {'string', 'integer', 'boolean', 'object'}:
                includes.add(f"rest_catalog/objects/{prop.items_type}.hpp")
        includes.update(f"rest_catalog/objects/{ref}.hpp" for ref in self.all_of_refs)
        return includes

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
            
            if prop_name in self.required:
                lines.extend([
                    f'\t\tif ({var_name}_val) {{',
                    f'\t\t\tresult.{var_name} = {self._get_parse_statement(var_name, prop)};',
                    '\t\t} else {',
                    f'\t\t\tthrow IOException("{self.name} required property \'{prop_name}\' is missing");',
                    '\t\t}'
                ])
            else:
                lines.extend([
                    f'\t\tif ({var_name}_val) {{',
                    f'\t\t\tresult.{var_name} = {self._get_parse_statement(var_name, prop)};',
                    '\t\t}'
                ])
        
        lines.extend([
            '\t\treturn result;',
            '\t}',
            'public:'
        ])
        
        # Generate properties
        for prop_name, prop in self.properties.items():
            var_name = safe_cpp_name(prop_name)
            lines.append(f'\t{prop.get_cpp_type()} {var_name};')
        
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

def generate_header_implementations():
    return '''#include "rest_catalog/response_objects.hpp"

namespace duckdb {
namespace rest_api_objects {

template<typename T>
vector<T> parse_obj_array(yyjson_val *arr) {
    vector<T> result;
    size_t idx, max;
    yyjson_val *val;
    yyjson_arr_foreach(arr, idx, max, val) {
        result.push_back(T::FromJSON(val));
    }
    return result;
}

inline vector<string> parse_str_array(yyjson_val *arr) {
    vector<string> result;
    size_t idx, max;
    yyjson_val *val;
    yyjson_arr_foreach(arr, idx, max, val) {
        result.push_back(yyjson_get_str(val));
    }
    return result;
}

ObjectOfStrings ObjectOfStrings::FromJSON(yyjson_val *obj) {
    ObjectOfStrings result;
    size_t idx, max;
    yyjson_val *key, *val;
    yyjson_obj_foreach(obj, idx, max, key, val) {
        auto key_str = yyjson_get_str(key);
        auto val_str = yyjson_get_str(val);
        result[key_str] = val_str;
    }
    return result;
}

'''

def get_dependencies(schema: Schema) -> Set[str]:
    """Get all type dependencies for a schema."""
    deps = set()
    for prop in schema.properties.values():
        if prop.type != prop.name and prop.type not in {'string', 'integer', 'boolean', 'object'}:
            deps.add(prop.type)
        # Add array item type dependencies
        if prop.type == 'array':
            if prop.items_type not in {'string', 'integer', 'boolean', 'object'}:
                deps.add(prop.items_type)
    
    # Also check allOf references
    if hasattr(schema, 'all_of_refs'):
        deps.update(schema.all_of_refs)
    return deps

def generate_list_header(schema_objects: Dict[str, Schema]) -> str:
    lines = [
        "",
        "// This file is automatically generated and contains all REST API object headers",
        ""
    ]
    
    # Add includes for all generated headers
    for name in schema_objects:
        lines.append(f'#include "rest_catalog/objects/{name}.hpp"')
    
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
    output_dir = os.path.join(SCRIPT_PATH, '..', 'src', 'include', 'rest_catalog', 'objects')
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate a header file for each schema
    for name in schema_objects:
        schema = schema_objects[name]
        output_path = os.path.join(output_dir, f'{name}.hpp')
        with open(output_path, 'w') as f:
            f.write(schema.generate_header_file())

    with open(os.path.join(output_dir, 'list.hpp'), 'w') as f:
        f.write(generate_list_header(schema_objects))

    # Generate base header file with only helper methods
    base_header_path = os.path.join(SCRIPT_PATH, '..', 'src', 'include', 'rest_catalog', 'response_objects.hpp')
    with open(base_header_path, 'w') as f:
        f.write('''#pragma once

#include "yyjson.hpp"
#include <string>
#include <vector>
#include <unordered_map>

using namespace duckdb_yyjson;

namespace duckdb {
namespace rest_api_objects {

template<typename T>
vector<T> parse_obj_array(yyjson_val *arr) {
    vector<T> result;
    size_t idx, max;
    yyjson_val *val;
    yyjson_arr_foreach(arr, idx, max, val) {
        result.push_back(T::FromJSON(val));
    }
    return result;
}

inline vector<string> parse_str_array(yyjson_val *arr) {
    vector<string> result;
    size_t idx, max;
    yyjson_val *val;
    yyjson_arr_foreach(arr, idx, max, val) {
        result.push_back(yyjson_get_str(val));
    }
    return result;
}

using ObjectOfStrings = unordered_map<string, string>;

inline ObjectOfStrings parse_object_of_strings(yyjson_val *obj) {
    ObjectOfStrings result;
    size_t idx, max;
    yyjson_val *key, *val;
    yyjson_obj_foreach(obj, idx, max, key, val) {
        auto key_str = yyjson_get_str(key);
        auto val_str = yyjson_get_str(val);
        result[key_str] = val_str;
    }
    return result;
}

} // namespace rest_api_objects
} // namespace duckdb
''')

if __name__ == '__main__':
    main()
