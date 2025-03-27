import yaml
import os
from typing import Dict, List, Set

SCRIPT_PATH = os.path.dirname(os.path.realpath(__file__))
API_SPEC_PATH = os.path.join(SCRIPT_PATH, 'api.yaml')
OUTPUT_PATH = os.path.join(SCRIPT_PATH, '..', 'src', 'include', 'rest_catalog', 'response_objects.hpp')
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
        
        # Handle allOf
        if 'allOf' in schema:
            for sub_schema in schema['allOf']:
                if '$ref' in sub_schema:
                    # Handle referenced schema properties later in dependency resolution
                    pass
                elif 'properties' in sub_schema:
                    for prop_name, prop_schema in sub_schema['properties'].items():
                        self.properties[prop_name] = Property(prop_name, prop_schema)
                if 'required' in sub_schema:
                    self.required.update(sub_schema['required'])
        
        # Handle regular properties
        for prop_name, prop_schema in schema.get('properties', {}).items():
            if 'allOf' in prop_schema:
                # If property has allOf, use the first reference as the type
                ref = prop_schema['allOf'][0].get('$ref')
                if ref:
                    prop_schema = {'$ref': ref}
            self.properties[prop_name] = Property(prop_name, prop_schema)

    def generate_class(self) -> str:
        lines = [f'class {self.name} {{']
        lines.append('public:')
        
        # Generate FromJSON method
        lines.append(f'\tstatic {self.name} FromJSON(yyjson_val *obj) {{')
        lines.append(f'\t\t{self.name} result;')
        
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
        
        lines.append('\t\treturn result;')
        lines.append('\t}')
        
        # Generate constructor
        lines.append('public:')
        lines.append(f'\t{self.name}() {{}}')
        
        # Generate properties
        lines.append('public:')
        for prop_name, prop in self.properties.items():
            var_name = safe_cpp_name(prop_name)
            lines.append(f'\t{prop.get_cpp_type()} {var_name};')
        
        lines.append('};')
        return '\n'.join(lines)

    def _get_parse_statement(self, var_name: str, prop: Property) -> str:
        type_mapping = {
            'string': f'yyjson_get_str({var_name}_val)',
            'integer': f'yyjson_get_sint({var_name}_val)',
            'boolean': f'yyjson_get_bool({var_name}_val)',
            'object': f'ObjectOfStrings::FromJSON({var_name}_val)'
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

def generate_header():
    return '''#pragma once

#include "yyjson.hpp"

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

class ObjectOfStrings {
public:
    static ObjectOfStrings FromJSON(yyjson_val *obj) {
        ObjectOfStrings result;
        yyjson_val *key, *val;
        yyjson_obj_iter iter = yyjson_obj_iter_with(obj);
        while ((key = yyjson_obj_iter_next(&iter))) {
            val = yyjson_obj_iter_get_val(key);
            result.properties[yyjson_get_str(key)] = yyjson_get_str(val);
        }
        return result;
    }
public:
    ObjectOfStrings() {}
public:
    case_insensitive_map_t<string> properties;
};

'''

def generate_footer():
    return '''
} // namespace rest_api_objects

} // namespace duckdb
'''

def main():
    # Load OpenAPI spec
    with open(API_SPEC_PATH) as f:
        spec = yaml.safe_load(f)
    
    schemas = spec['components']['schemas']
    
    # Generate code
    with open(OUTPUT_PATH, 'w') as f:
        f.write(generate_header())
        
        # Process schemas in dependency order
        processed_schemas = set()
        schema_objects = {}
        
        for name, schema in schemas.items():
            schema_objects[name] = Schema(name, schema)
        
        while len(processed_schemas) < len(schema_objects):
            for name, schema in schema_objects.items():
                if name in processed_schemas:
                    continue
                    
                # Check if all dependencies are processed
                deps_processed = True
                for prop in schema.properties.values():
                    if prop.ref and prop.type not in processed_schemas:
                        deps_processed = False
                        break
                
                if deps_processed:
                    f.write(schema.generate_class())
                    f.write('\n\n')
                    processed_schemas.add(name)
        
        f.write(generate_footer())

if __name__ == '__main__':
    main()
