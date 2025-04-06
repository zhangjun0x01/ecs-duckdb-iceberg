from parse_openapi_spec import (
    ResponseObjectsGenerator,
    Property, ArrayProperty,
    PrimitiveProperty,
    SchemaReferenceProperty,
    ObjectProperty
)
import os
from typing import Dict, List, Set, Optional, cast
from enum import Enum, auto
from dataclasses import dataclass

SCRIPT_PATH = os.path.dirname(os.path.realpath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_PATH, '..', 'src', 'include', 'rest_catalog', 'objects')
API_SPEC_PATH = os.path.join(SCRIPT_PATH, 'api.yaml')

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
    'error',  # add 'error' to avoid conflicts with the 'error' variable in TryFromJSON
}

"""
TODO:
We have to write a method to check for each schema if it's recursive
(follow the references until we've hit ourselves or we run out of references to follow)

We need to split declaration (header) and definition (source), because some schemas are recursive
and if we don't, then we run into a cyclic dependency in the includes

We probably want to split off the 'generate_*' methods into a new class
there's too much state that we need and scoping that we want to justify jamming it all into a single class
"""

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

HEADER_FORMAT = """
#pragma once

#include "yyjson.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest_catalog/response_objects.hpp"
{ADDITIONAL_HEADERS}

using namespace duckdb_yyjson;

namespace duckdb {{
namespace rest_api_objects {{

{CLASS_DEFINITION}

}} // namespace rest_api_objects
}} // namespace duckdb

"""

CLASS_FORMAT = """
class {CLASS_NAME} {{
public:
    {CLASS_NAME}() {{}}
{NESTED_CLASSES}
public:
    static {CLASS_NAME} FromJSON(yyjson_val *obj) {{
        {CLASS_NAME} res;
        auto error = res.TryFromJSON(obj);
        if (!error.empty()) {{
            throw InvalidInputException(error);
        }}
        return res;
    }}
public:
    string TryFromJSON(yyjson_val *obj) {{
        string error;
{BASE_CLASS_PARSING}
{REQUIRED_PROPERTIES}
{OPTIONAL_PROPERTIES}
{ADDITIONAL_PROPERTIES}
        return string();
    }}
public:
{BASE_CLASS_VARIABLES}
public:
{PROPERTY_VARIABLES}
}};
"""

"""
    SnapshotReference tmp;
    error = tmp.TryFromJSON(val);
    if (!error.empty()) {
        return error;
    }
"""

ADDITIONAL_PROPERTIES_FORMAT = """
{HANDLED_PROPERTIES}
size_t idx, max;
yyjson_val *key, *val;
yyjson_obj_foreach(obj, idx, max, key, val) {{
    auto key_str = yyjson_get_str(key);
    {SKIP_HANDLED_PROPERTIES}
    {PARSE_PROPERTY}
    additional_properties[key_str] = tmp;
}}
"""

class Variable:
    class Type(Enum):
        PRIMITIVE = auto()
        ARRAY = auto()
        OBJECT = auto()
        SCHEMA_REFERENCE = auto()

    def __init__(self, name: str, type: "Variable.Type"):
        self.name = name
        self.type = type

@dataclass
class OneOf:
    """An option of the OneOf construct"""
    name: str
    dereference_style: str
    class_name: str

@dataclass
class AnyOf:
    """An option of the AnyOf construct"""
    name: str
    dereference_style: str
    class_name: str

@dataclass
class AllOf:
    """An option of the AllOf construct"""
    name: str
    dereference_style: str
    class_name: str

@dataclass
class RequiredProperty:
    """A property that is required to be present in the JSON"""
    name: str
    body: List[str]

@dataclass
class OptionalProperty:
    """A property that is can or can't be present in the JSON"""
    name: str
    body: List[str]

@dataclass
class AdditionalProperty:
    """The additional (typed) properties not covered by the spec"""
    body: List[str]
    exclude_list: List[str] = []
    skip_if_excluded: List[str] = []

class CPPClass:
    def __init__(self, class_name):
        self.name = class_name
        # The base classes that make up this class
        self.one_of: List[OneOf] = []
        self.all_of: List[AllOf] = []
        self.any_of: List[AnyOf] = []

        # Parsing code of the TryFromJSON method
        self.required_properties: Dict[str, Variable] = {}
        self.optional_properties: Dict[str, Variable] = {}
        self.additional_properties: Optional[Variable] = None

        # Nested classes of this class (referenced by variables)
        self.nested_classes: Dict[str, "CPPClass"] = {}
        # (member) variables of the class
        self.variables: List[str] = []

    def from_object_property(self, name: str, schema: ObjectProperty):
        assert schema.type == Property.Type.OBJECT
        object_property = cast(ObjectProperty, schema)

        # Parse any base classes required for the schema (anyOf, allOf, oneOf)
        self.generate_all_of(name, schema)
        self.generate_one_of(name, schema)
        self.generate_any_of(name, schema)

        #base_class_parsing = []
        #if all_of_parsing:
        #    base_class_parsing.append(all_of_parsing)
        #if one_of_parsing:
        #    base_class_parsing.append(one_of_parsing)
        #if any_of_parsing:
        #    base_class_parsing.append(any_of_parsing)

        #referenced_schemas.update(base_classes)

        #required = object_property.required
        #if not required:
        #    required = []
        #remaining_properties = [x for x in object_property.properties if x not in required]

        #required_properties = {}
        #optional_properties = {}
        #for item in remaining_properties:
        #    optional_properties[item] = object_property.properties[item]
        #for item in required:
        #    required_properties[item] = object_property.properties[item]

        #required_property_parsing = self.generate_required_properties(name, required_properties, referenced_schemas)
        #required_property_parsing = '\n'.join([f'\t{x}' for x in required_property_parsing.split('\n')])

        #optional_property_parsing = self.generate_optional_properties(name, optional_properties, referenced_schemas)
        #optional_property_parsing = '\n'.join([f'\t{x}' for x in optional_property_parsing.split('\n')])

        #additional_property_parsing = self.generate_additional_properties(
        #    object_property.properties.keys(), object_property.additional_properties, referenced_schemas
        #)
        #additional_property_parsing = '\n'.join([f'\t{x}' for x in additional_property_parsing.split('\n')])

        #if any([x.startswith('Object') for x in referenced_schemas]):
        #    print("referenced_schemas", referenced_schemas)

        #base_class_variables = []
        #for item in base_classes:
        #    base_class = self.parsed_schemas[item]
        #    variable_name = to_snake_case(item)
        #    base_class_variables.append(f'\t{item} {variable_name};')

        #nested_classes = self.generate_nested_class_definitions(referenced_schemas)

        #variable_definitions = []
        #for item in sorted(list(object_property.properties.keys())):
        #    variable = object_property.properties[item]
        #    variable_type = self.generate_variable_type(variable)
        #    variable_definitions.append(f'\t{variable_type} {safe_cpp_name(item)};')

        #for item in object_property.any_of:
        #    item_name = to_snake_case(item.ref)
        #    variable_definitions.append(f'\tbool has_{safe_cpp_name(item_name)} = false;')
        #for item in object_property.one_of:
        #    item_name = to_snake_case(item.ref)
        #    variable_definitions.append(f'\tbool has_{safe_cpp_name(item_name)} = false;')

        #if object_property.additional_properties:
        #    variable_type = self.generate_variable_type(object_property.additional_properties)
        #    variable_definitions.append(f'\tcase_insensitive_map_t<{variable_type}> additional_properties;')

        #class_definition = CLASS_FORMAT.format(
        #    CLASS_NAME=name,
        #    NESTED_CLASSES=nested_classes,
        #    BASE_CLASS_PARSING='\n'.join(base_class_parsing),
        #    REQUIRED_PROPERTIES=required_property_parsing,
        #    OPTIONAL_PROPERTIES=optional_property_parsing,
        #    ADDITIONAL_PROPERTIES=additional_property_parsing,
        #    BASE_CLASS_VARIABLES='\n'.join(base_class_variables),
        #    PROPERTY_VARIABLES='\n'.join(variable_definitions),
        #)

    def from_array_property(self, name: str, schema: ArrayProperty):
        #assert schema.type == Property.Type.ARRAY
        #array_property = cast(ArrayProperty, schema)

        #assert not array_property.all_of
        #assert not array_property.one_of
        #assert not array_property.any_of

        #body = self.generate_array_loop('obj', 'value', array_property.item_type, referenced_schemas)

        #nested_classes = self.generate_nested_class_definitions(referenced_schemas)

        #variable_type = self.generate_variable_type(schema)
        #variable_definition = f'\t{variable_type} value;'

        #body = '\n'.join([f'\t{x}' for x in body.split('\n')])
        #class_definition = CLASS_FORMAT.format(
        #    CLASS_NAME=name,
        #    NESTED_CLASSES=nested_classes,
        #    BASE_CLASS_PARSING='',
        #    REQUIRED_PROPERTIES=body,
        #    OPTIONAL_PROPERTIES='',
        #    ADDITIONAL_PROPERTIES='',
        #    BASE_CLASS_VARIABLES='',
        #    PROPERTY_VARIABLES=variable_definition,
        #)

    def from_primitive_property(self, name: str, schema: PrimitiveProperty):
        #assert not schema.all_of
        #assert not schema.one_of
        #assert not schema.any_of

        ## TODO: implement this

        #variable_parsing = self.generate_assignment(schema, 'value', 'obj', referenced_schemas)

        #variable_type = self.generate_variable_type(schema)
        #variable_definition = f'\t{variable_type} value;'

        #class_definition = CLASS_FORMAT.format(
        #    CLASS_NAME=name,
        #    NESTED_CLASSES='',
        #    BASE_CLASS_PARSING='',
        #    REQUIRED_PROPERTIES=variable_parsing,
        #    OPTIONAL_PROPERTIES='',
        #    ADDITIONAL_PROPERTIES='',
        #    BASE_CLASS_VARIABLES='',
        #    PROPERTY_VARIABLES=variable_definition,
        #)

    def from_property(self, name: str, schema: Property) -> None:
        if schema.type == Property.Type.OBJECT:
            self.from_object_property(name, schema)
        elif schema.type == Property.Type.ARRAY:
            self.from_array_property(name, schema)
        elif schema.type == Property.Type.PRIMITIVE:
            self.from_primitive_property(name, schema)
        else:
            print(f"Unrecognized 'from_property' type {schema.type}")
            exit(1)


    def write_required_property(self, required_property: RequiredProperty) -> List[str]:
        res = []
        res.extend([
            f'auto {required_property.name}_val = yyjson_obj_get(obj, "{required_property.name}");',
            f'if (!{required_property.name}_val) {{',
            f'    return "{self.name} required property '{required_property.name}' is missing";',
            '} else {',
        ])
        res.extend(required_property.body)
        res.append('}')
        return res

    def write_optional_property(self, optional_property: OptionalProperty) -> List[str]:
        res = []
        res.extend([
            f'auto {optional_property.name}_val = yyjson_obj_get(obj, "{optional_property.name}");',
            f'if ({optional_property.name}_val) {{',
        ])
        res.extend(optional_property.body)
        res.append('}')
        return res

    def write_additional_properties(self, additional_property: AdditionalProperty) -> List[str]:
        res = []

        res.extend(additional_property.exclude_list)
        res.extend([
            'size_t idx, max;',
            'yyjson_val *key, *val;',
            'yyjson_obj_foreach(obj, idx, max, key, val) {',
        ])
        res.extend(additional_property.skip_if_excluded)
        res.append('\tauto key_str = yyjson_get_str(key);')
        res.extend(additional_property.body)
        res.extend([
            '\tadditional_properties[key_str] = tmp;',
            '}',
        ])
        return res

    def write_all_of(self) -> List[str]:
        if not self.all_of:
            return []
        res = []
        for item in self.all_of:
            if item.dereference_style == '->':
                res.append(f'{item.name} = make_uniq<{item.class_name}>();')
            res.extend([
                f'error = {item.name}{item.dereference_style}TryFromJSON(obj);'
                'if (!error.empty()) {',
                '\treturn error;',
                '}'
            ])
        return res

    def write_one_of(self) -> List[str]:
        if not self.one_of:
            return []
        res = []
        res.append('do {')
        for item in self.one_of:
            if item.dereference_style == '->':
                res.append(f'{item.name} = make_uniq<{item.class_name}>();')
            res.extend([
                f'\terror = {item.name}{item.dereference_style}TryFromJSON(obj);',
                '\tif (error.empty()) {',
                f'\t\thas_{item.name} = true;',
                '\t\tbreak;',
                '\t}'
            ])
        res.append(f'\treturn "{self.name} failed to parse, none of the oneOf candidates matched";')
        res.append('} while (false);')
        return res

    def write_any_of(self) -> List[str]:
        if not self.any_of:
            return []
        res = []

        all_options = [
            f'!has_{item.name}' for item in self.any_of
        ]
        sort(all_options)
        condition = ' && '.join(all_options)

        for item in self.any_of:
            if item.dereference_style == '->':
                res.append(f'{item.name} = make_uniq<{item.class_name}>();')
            res.extend([
                f'error = {item.name}{item.dereference_style}TryFromJSON(obj);',
                'if (error.empty()) {',
                f'\thas_{item.name} = true;',
                '}'
            ])
        res.extend([
            f'if ({condition}) {{'
            f'\treturn "{self.name} failed to parse, none of the anyOf candidates matched";'
            '}'
        ])
        return res

    def write_class(self) -> List[str]
        pass


    def generate_all_of(self, property: Property):
        if not property.all_of:
            return
        for item in property.all_of:
            assert item.type == Property.Type.SCHEMA_REFERENCE

            class_name = item.ref
            property_name = to_snake_case(class_name)
            dereference_style = '->' if item.ref in self.recursive_schemas else '.'

            self.all_of.append(
                AllOf(
                    name=property_name,
                    dereference_style=dereference_style,
                    class_name=class_name
                )
            )
            self.variables.append(
                f'\t{item.ref} {property_name};'
            )

    def generate_any_of(self, property: Property):
        if not property.any_of:
            return
        for item in property.any_of:
            assert item.type == Property.Type.SCHEMA_REFERENCE

            class_name = item.ref
            property_name = to_snake_case(class_name)
            dereference_style = '->' if item.ref in self.recursive_schemas else '.'

            self.any_of.append(
                AnyOf(
                    name=property_name,
                    dereference_style=dereference_style,
                    class_name=class_name
                )
            )
            self.variables.append(
                f'\t{item.ref} {property_name};'
            )
            self.variables.append(
                f'\tbool has_{property_name} = false;'
            )

    def generate_one_of(self, property: Property):
        if not property.one_of:
            return
        for item in property.one_of:
            assert item.type == Property.Type.SCHEMA_REFERENCE

            class_name = item.ref
            property_name = to_snake_case(class_name)
            dereference_style = '->' if item.ref in self.recursive_schemas else '.'

            self.one_of.append(
                OneOf(
                    name=property_name,
                    dereference_style=dereference_style,
                    class_name=class_name
                )
            )
            self.variables.append(
                f'\t{item.ref} {property_name};'
            )
            self.variables.append(
                f'\tbool has_{property_name} = false;'
            )






    def generate_array_loop(self, array_name, destination_name, item_type: Property, referenced_schemas: set) -> List[str]:
        res = []
        res.append('size_t idx, max;')
        res.append('yyjson_val *val;')
        res.append(f'yyjson_arr_foreach({array_name}, idx, max, val) {{')

        assignment = 'tmp'
        if item_type.type != Property.Type.SCHEMA_REFERENCE:
            item_parse = self.generate_item_parse(item_type, 'val', referenced_schemas)
            res.append(f'\tauto tmp = {item_parse};')
        else:
            schema_property = cast(SchemaReferenceProperty, item_type)
            referenced_schemas.add(schema_property.ref)
            item_definition = ''
            if schema_property.ref in self.recursive_schemas:
                res.extend([
                    f'\tauto tmp_p = make_uniq<{schema_property.ref}>();'
                    '\tauto &tmp = *tmp_p;'
                ])
                assignment = 'std::move(tmp_p)'
            else:
                res.append(f'\t{schema_property.ref} tmp;')
            res.extend([
                '\terror = tmp.TryFromJSON(val);',
                '\tif (!error.empty()) {',
                '\t\treturn error;',
                '\t}'
            ])
        res.append(f'\t{destination_name}.push_back({assignment});')
        res.append('}')
        return res

    def generate_item_parse(self, property: Property, source: str, referenced_schemas: set) -> str:
        if property.type == Property.Type.SCHEMA_REFERENCE:
            print(f"Unrecognized property type {property.type}, {source}")
            exit(1)
        if property.type == Property.Type.ARRAY:
            # TODO: maybe we move the array parse to a function that creates a vector<...>, instead of parsing it inline
            print(f'Nested arrays are not supported, hopefully we dont have to!')
            exit(1)
        elif property.type == Property.Type.PRIMITIVE:
            PRIMITIVE_PARSE_FUNCTIONS = {
                'string': 'yyjson_get_str',
                'integer': 'yyjson_get_sint',
                'boolean': 'yyjson_get_bool',
            }
            primitive_property = cast(PrimitiveProperty, property)
            item_type = primitive_property.primitive_type
            if item_type in PRIMITIVE_PARSE_FUNCTIONS:
                parse_func = PRIMITIVE_PARSE_FUNCTIONS[item_type]
            elif item_type == 'number':
                format = primitive_property.format
                if not format:
                    print(f"'number' without a 'format' property in the spec!")
                    exit(1)
                NUMBER_PARSE_FUNCTIONS = {'double': 'yyjson_get_real', 'float': 'yyjson_get_real'}
                if format not in NUMBER_PARSE_FUNCTIONS:
                    print(f"'number' without an unrecognized 'format' found, {format}")
                    exit(1)
                parse_func = NUMBER_PARSE_FUNCTIONS[format]
            else:
                print(f"Unrecognized primitive type '{item_type}' encountered in array")
                exit(1)
            return f'{parse_func}({source})'
        elif property.type == Property.Type.OBJECT and property.is_object_of_strings():
            return f'parse_object_of_strings({source})'
        elif property.type == Property.Type.OBJECT and property.is_raw_object():
            return source
        else:
            print(f"Unrecognized type in 'generate_item_parse', {property.type}")
            exit(1)

    def generate_assignment(self, schema: Property, target: str, source: str, referenced_schemas: set) -> List[str]:
        if schema.type == Property.Type.ARRAY:
            array_property = cast(ArrayProperty, schema)
            return self.generate_array_loop(source, target, array_property.item_type, referenced_schemas)
        elif schema.type == Property.Type.SCHEMA_REFERENCE:
            schema_property = cast(SchemaReferenceProperty, schema)
            referenced_schemas.add(schema_property.ref)
            result = []
            dereference_style = '.'
            if schema_property.ref in self.recursive_schemas:
                result.append(f'{target} = make_uniq<{schema_property.ref}>();')
                dereference_style = '->'
            result.extend([
                f'error = {target}{dereference_style}TryFromJSON({source});',
                'if (!error.empty()) {',
                '    return error;',
                '}'
            ])
            return result
        else:
            item_parse = self.generate_item_parse(schema, source, referenced_schemas)
            return [f'{target} = {item_parse};']

    def generate_optional_properties(self, name: str, properties: Dict[str, Property], referenced_schemas: set):
        if not properties:
            return
        res = []
        for item, optional_property in properties.items():
            variable_name = safe_cpp_name(item)
            body = self.generate_assignment(
                optional_property, variable_name, f'{variable_name}_val', referenced_schemas
            )
            self.required_properties.append(
                OptionalProperty(
                    name=variable_name,
                    body=body
                )
            )
            variable_type = self.generate_variable_type(optional_property)
            self.variables.append(
                f'\t{variable_type} {variable_name};'
            )

    def generate_required_properties(self, name: str, properties: Dict[str, Property], referenced_schemas: set):
        if not properties:
            return
        res = []
        for item, required_property in properties.items():
            variable_name = safe_cpp_name(item)
            body = self.generate_assignment(
                required_property, variable_name, f'{variable_name}_val', referenced_schemas
            )
            self.required_properties.append(
                RequiredProperty(
                    name=variable_name,
                    body=body
                )
            )
            variable_type = self.generate_variable_type(required_property)
            self.variables.append(
                f'\t{variable_type} {variable_name};'
            )

    def generate_additional_properties(self, properties: List[str], additional_properties: Property, referenced_schemas: set):
        if not additional_properties:
            return

        skip_if_excluded = []
        exclude_list = []
        if properties:
            exclude_list = [
                'case_insensitive_set_t handled_properties {',
                f'\t\t{', '.join(f'"{x}"' for x in properties)}'
                '}'
            ]
            skip_if_excluded = [
                '\tif (handled_properties.count(key_str)) {',
                '\t\tcontinue;',
                '\t}',
            ]

        body = []
        if additional_properties.type != Property.Type.SCHEMA_REFERENCE:
            item_definition = self.generate_item_parse(additional_properties, 'val', referenced_schemas)
            body.append(f'\tauto tmp = {item_definition};')
        else:
            schema_property = cast(SchemaReferenceProperty, additional_properties)
            referenced_schemas.add(schema_property.ref)
            if schema_property.ref in self.recursive_schemas:
                print(f"Encountered recursive schema '{schema_property.ref}' in 'generate_additional_properties'")
                exit(1)
            body.append(f'\t{schema_property.ref} tmp;')
            body.extend([
                'error = tmp.TryFromJSON(val);',
                'if (!error.empty()) {',
                '\treturn error;',
                '}',
            ])
        self.additional_properties = AdditionalProperty(
            body=body,
            exclude_list=exclude_list,
            skip_if_excluded=skip_if_excluded
        )
        variable_type = self.generate_variable_type(additional_properties)
        self.variables.append(
            f'\tcase_insensitive_map_t<{variable_type}> additional_properties;'
        )

    def generate_variable_type(self, schema: Property) -> str:
        if schema.type == Property.Type.OBJECT:
            object_property = cast(ObjectProperty, schema)
            assert not object_property.properties
            if object_property.additional_properties:
                variable_type = self.generate_variable_type(object_property.additional_properties)
                return f'case_insensitive_map_t<{variable_type}>'
            return 'yyjson_val *'
        elif schema.type == Property.Type.ARRAY:
            array_property = cast(ArrayProperty, schema)
            item_type = self.generate_variable_type(array_property.item_type)
            return f'vector<{item_type}>'
        elif schema.type == Property.Type.PRIMITIVE:
            PRIMITIVE_TYPE_MAPPING = {
                'string': 'string',
                'integer': 'int64_t',
                'boolean': 'bool',
            }
            primitive_property = cast(PrimitiveProperty, schema)
            primitive_type = primitive_property.primitive_type
            if primitive_type in PRIMITIVE_TYPE_MAPPING:
                return PRIMITIVE_TYPE_MAPPING[primitive_type]
            elif primitive_type == 'number':
                if not primitive_property.format:
                    print(f"'number' without a 'format' property in the spec!")
                    exit(1)
                return primitive_property.format
            else:
                print(f"Unrecognized primitive type '{primitive_type}' in 'generate_variable_type'")
                exit(1)
        elif schema.type == Property.Type.SCHEMA_REFERENCE:
            schema_property = cast(SchemaReferenceProperty, schema)
            if schema_property.ref in self.recursive_schemas:
                return f'unique_ptr<{schema_property.ref}>'
            return schema_property.ref
        else:
            print(f"Unrecognized 'generate_variable_type' type {schema.type}")
            exit(1)

    #def generate_nested_class_definitions(self, referenced_schemas: set):
    #    generated_schemas_referenced = [x for x in referenced_schemas if x not in self.schemas]
    #    nested_classes = []
    #    for item in generated_schemas_referenced:
    #        parsed_schema = self.parsed_schemas[item]
    #        content = self.generate_class(parsed_schema, item, referenced_schemas)
    #        nested_classes.append(content)

    #    if not nested_classes:
    #        return ''
    #    nested_classes = '\n'.join(nested_classes)
    #    nested_classes = '\n'.join([f'\t{x}' for x in nested_classes.split('\n')])
    #    return 'public:\n' + nested_classes

    #def generate_schema(self, schema: Property, name: str):
    #    referenced_schemas = set()
    #    class_definition = self.generate_class(schema, name, referenced_schemas)

    #    include_schemas = [x for x in referenced_schemas if x in self.schemas]
    #    additional_headers = [
    #        f'#include "rest_catalog/objects/{to_snake_case(x)}.hpp"' for x in sorted(list(include_schemas))
    #    ]

    #    file_contents = HEADER_FORMAT.format(
    #        ADDITIONAL_HEADERS='\n'.join(additional_headers), CLASS_DEFINITION=class_definition
    #    )
    #    return file_contents


if __name__ == '__main__':
    ResponseObjectsGenerator openapi_parser(API_SPEC_PATH)
    openapi_parser.parse_all_schemas()

    # Create directory if it doesn't exist
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    with open(os.path.join(OUTPUT_DIR, 'list.hpp'), 'w') as f:
        lines = ["", "// This file is automatically generated and contains all REST API object headers", ""]
        # Add includes for all generated headers
        for name in openapi_parser.schemas:
            lines.append(f'#include "rest_catalog/objects/{to_snake_case(name)}.hpp"')
        f.write('\n'.join(lines))

    for name in openapi_parser.schemas:
        schema = openapi_parser.parsed_schemas[name]


        file_content = self.generate_schema(schema, name)
        output_path = os.path.join(OUTPUT_DIR, f'{to_snake_case(name)}.hpp')
        with open(output_path, 'w') as f:
            f.write(file_content)

    CPPGenerator generator

