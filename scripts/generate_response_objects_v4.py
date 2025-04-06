import yaml
import os
from typing import Dict, List, Set, Optional, cast
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


class Property:
    class Type(Enum):
        PRIMITIVE = auto()
        ARRAY = auto()
        OBJECT = auto()
        SCHEMA_REFERENCE = auto()

    def __init__(self, type: "Property.Type"):
        self.type = type
        self.all_of: List[Property] = []
        self.any_of: List[Property] = []
        self.one_of: List[Property] = []

    def is_string(self):
        if self.type != Property.Type.PRIMITIVE:
            return False
        primitive_property = cast(PrimitiveProperty, self)
        return primitive_property.primitive_type == 'string'


class SchemaReferenceProperty(Property):
    def __init__(self, name):
        super().__init__(Property.Type.SCHEMA_REFERENCE)
        self.ref = name


class ArrayProperty(Property):
    def __init__(self):
        super().__init__(Property.Type.ARRAY)
        self.item_type: Optional[Property] = None


class PrimitiveProperty(Property):
    def __init__(self):
        super().__init__(Property.Type.PRIMITIVE)
        self.primitive_type: Optional[str] = None
        self.format = None
        # TODO: if 'enum' is present, we should verify that the value of the property is one of the accepted values
        self.enum: Optional[List[str]] = None
        # TODO: same for this, this property *has* to have this value
        self.const: Optional[str] = None


class ObjectProperty(Property):
    def __init__(self):
        super().__init__(Property.Type.OBJECT)
        self.required = []
        self.properties: Dict[str, Property] = {}
        # TODO: if present, we should collect these in a 'case_insensitive_map_t'
        self.additional_properties: Optional[Property] = None
        # TODO: do we need this? the schema validation shouldn't need it
        self.discriminator = None

    def is_object_of_strings(self):
        if self.properties:
            return False
        if not self.additional_properties:
            return False
        if self.additional_properties.type != Property.Type.PRIMITIVE:
            return False
        primitive_property = cast(PrimitiveProperty, self.additional_properties)
        return primitive_property.primitive_type == 'string'

    def is_raw_object(self):
        if self.properties:
            return False
        if self.additional_properties:
            return False
        return True


class ResponseObjectsGenerator:
    def __init__(self, path: str):
        self.path = path
        self.parsed_schemas: Dict[str, Property] = {}
        self.parsed_responses: Dict[str, Response] = {}
        # Since schemas reference other schemas and are potentially recursive
        # We want to keep track of the schemas that are currently being parsed
        self.schemas_being_parsed: Set[str] = set()
        # Whenever this schema is referenced, the instance has to be wrapped in a unique_ptr
        # otherwise the constructor will either be an infinite recursion
        # or it won't compile (hopefully this)
        self.recursive_schemas: Set[str] = set()
        self.object_schema_count = 0

        # Load OpenAPI spec
        with open(API_SPEC_PATH) as f:
            spec = yaml.safe_load(f)

        self.schemas = spec['components']['schemas']
        self.responses = spec['components']['responses']

    def parse_object_property(self, spec: dict, result: Property) -> None:
        # For polymorphic types, this defines a mapping based on the content of a property
        discriminator = spec.get('discriminator')
        # Get the required properties of the schema
        required = spec.get('required')
        # Get the defined properties of the schema
        properties = spec.get('properties', {})
        # Get the type for any additional undefined properties
        additional_properties = spec.get('additionalProperties')

        assert result.type == Property.Type.OBJECT
        object_result = cast(ObjectProperty, result)

        if additional_properties:
            object_result.additional_properties = self.parse_property(additional_properties)

        object_result.required = required
        for name in properties:
            property_spec = properties[name]
            object_result.properties[name] = self.parse_property(property_spec)

    def parse_primitive_property(self, spec: dict, result: Property) -> None:
        primitive_type = spec['type']
        format = spec.get('format')
        assert primitive_type in PRIMITIVE_TYPES
        assert result.type == Property.Type.PRIMITIVE
        primitive_result = cast(PrimitiveProperty, result)
        primitive_result.format = format
        primitive_result.primitive_type = primitive_type

    def parse_array_property(self, spec: dict, result: Property) -> None:
        item_type = spec['items']
        assert result.type == Property.Type.ARRAY
        array_result = cast(ArrayProperty, result)
        array_result.item_type = self.parse_property(item_type)

    def parse_property(self, spec: dict, reference: Optional[str] = None) -> Property:
        ref = spec.get('$ref')
        if not reference:
            if ref:
                parts = ref.split('/')
                assert parts[-2] == 'schemas'
                reference = parts[-1]
                self.parse_schema(reference)
                return SchemaReferenceProperty(reference)
        elif ref:
            print(f"Schema {reference} spec contains '$ref' ???")
            exit(1)

        # default to 'object' (see 'AssertViewUUID')
        property_type = spec.get('type', 'object')

        one_of = spec.get('oneOf')
        all_of = spec.get('allOf')
        any_of = spec.get('anyOf')

        if property_type == 'object':
            result = ObjectProperty()
            self.parse_object_property(spec, result)
        elif property_type == 'array':
            result = ArrayProperty()
            self.parse_array_property(spec, result)
        elif property_type in PRIMITIVE_TYPES:
            result = PrimitiveProperty()
            self.parse_primitive_property(spec, result)
        else:
            print(f"Property has unrecognized type: '{property_type}'!")
            exit(1)

        if one_of:
            if property_type != 'object':
                print(f"Property contains both 'oneOf' and a non-object 'type' ({property_type})")
                exit(1)
            assert 'allOf' not in spec
            assert 'anyOf' not in spec
            for item in one_of:
                res = self.parse_property(item)
                result.one_of.append(res)
        if all_of:
            if property_type != 'object':
                print(f"Property contains both 'allOf' and a non-object 'type' ({property_type})")
                exit(1)
            assert 'oneOf' not in spec
            assert 'anyOf' not in spec
            for item in all_of:
                res = self.parse_property(item)
                result.all_of.append(res)
        if any_of:
            if property_type != 'object':
                print(f"Property contains both 'allOf' and a non-object 'type' ({property_type})")
                exit(1)
            assert 'allOf' not in spec
            assert 'oneOf' not in spec
            for item in any_of:
                res = self.parse_property(item)
                result.any_of.append(res)

        if (
            not reference
            and result.type == Property.Type.OBJECT
            and not result.is_object_of_strings()
            and not result.is_raw_object()
        ):
            self.object_schema_count += 1
            new_name = f'Object{self.object_schema_count}'
            self.parsed_schemas[new_name] = result
            print("CUSTOM SCHEMA", new_name, spec)
            return SchemaReferenceProperty(new_name)
        return result

    def parse_schema(self, name: str) -> None:
        if name in self.parsed_schemas:
            return
        if name in self.schemas_being_parsed:
            self.recursive_schemas.add(name)
            return
        if name not in self.schemas:
            print(f"{name} is not a schema in the spec!")
            exit(1)

        self.schemas_being_parsed.add(name)
        schema = self.schemas[name]

        result = self.parse_property(schema, name)
        result.reference = name

        self.schemas_being_parsed.remove(name)
        self.parsed_schemas[name] = result

    def parse_all_schemas(self):
        for name in self.schemas:
            self.parse_schema(name)

    # Generation of CPP code

    def generate_all_of(self, name: str, property: Property, base_classes: set):
        if not property.all_of:
            return ''
        res = []
        for item in property.all_of:
            assert item.type == Property.Type.SCHEMA_REFERENCE
            class_name = item.ref
            base_classes.add(class_name)
            property_name = to_snake_case(class_name)
            result = ''
            dereference_style = '.'
            if item.ref in self.recursive_schemas:
                result = f'{property_name} = make_uniq<{item.ref}>();\n'
                dereference_style = '->'
            result += f"""\terror = {property_name}{dereference_style}TryFromJSON(obj);
\tif (!error.empty()) {{
\t\treturn error;
\t}}"""
            res.append(result)
        return '\n'.join(res)

    def generate_any_of(self, name: str, property: Property, base_classes: set):
        if not property.any_of:
            return ''
        all_base_classes = set()
        res = []
        for item in property.any_of:
            assert item.type == Property.Type.SCHEMA_REFERENCE
            class_name = item.ref
            base_classes.add(class_name)
            property_name = to_snake_case(class_name)
            all_base_classes.add(property_name)
            result = ''
            dereference_style = '.'
            if item.ref in self.recursive_schemas:
                result = f'{property_name} = make_uniq<{item.ref}>();\n'
                dereference_style = '->'
            result += f"""\terror = {property_name}{dereference_style}TryFromJSON(obj);
\tif (error.empty()) {{
\t\thas_{property_name} = true;
\t}}"""
            res.append(result)
        condition = ' && '.join(f'!has_{x}' for x in sorted(list(all_base_classes)))
        res.append(
            f"""
if ({condition}) {{
\treturn "{name} failed to parse, none of the anyOf candidates matched";
}}"""
        )
        return '\n'.join(res)

    def generate_one_of(self, name: str, property: Property, base_classes: set):
        if not property.one_of:
            return ''
        res = []

        res.append('do {')
        for item in property.one_of:
            assert item.type == Property.Type.SCHEMA_REFERENCE
            class_name = item.ref
            base_classes.add(class_name)
            property_name = to_snake_case(class_name)
            result = ''
            dereference_style = '.'
            if item.ref in self.recursive_schemas:
                result = f'{property_name} = make_uniq<{item.ref}>();\n'
                dereference_style = '->'
            result += f"""\terror = {property_name}{dereference_style}TryFromJSON(obj);
\tif (error.empty()) {{
\t\thas_{property_name} = true;
\t\tbreak;
\t}}"""
            res.append(result)
        res.append(f'\treturn "{name} failed to parse, none of the oneOf candidates matched";')
        res.append('} while (false);')
        return '\n'.join(res)

    def generate_array_loop(self, array_name, destination_name, item_type: Property, referenced_schemas: set):
        res = []
        # First declare the variables
        res.append('size_t idx, max;')
        res.append('yyjson_val *val;')
        # Then do the array iteration
        res.append(f'yyjson_arr_foreach({array_name}, idx, max, val) {{')

        assignment = 'tmp'
        if item_type.type != Property.Type.SCHEMA_REFERENCE:
            item_definition = self.generate_item_parse(item_type, 'val', referenced_schemas)
            item_definition = f'\tauto tmp = {item_definition};\n'
        else:
            schema_property = cast(SchemaReferenceProperty, item_type)
            referenced_schemas.add(schema_property.ref)
            item_definition = ''
            if schema_property.ref in self.recursive_schemas:
                item_definition = f'\tauto tmp_p = make_uniq<{schema_property.ref}>();\n'
                item_definition += f'\tauto &tmp = *tmp_p;'
                assignment = 'std::move(tmp_p)'
            else:
                item_definition = f'\t{schema_property.ref} tmp;\n'
            item_definition += """error = tmp.TryFromJSON(val);
if (!error.empty()) {
    return error;
}"""

        ARRAY_PUSHBACK_FORMAT = """
{ITEM_DEFINITION}\t{ARRAY_VARIABLE}.push_back({ASSIGNMENT});"""

        body = ''
        body += ARRAY_PUSHBACK_FORMAT.format(
            ITEM_DEFINITION=item_definition, ARRAY_VARIABLE=destination_name, ASSIGNMENT=assignment
        )
        res.append(body)
        res.append('}')
        return '\n'.join(res)

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

    def generate_assignment(self, property: Property, target: str, source: str, referenced_schemas: set) -> str:
        if property.type == Property.Type.ARRAY:
            array_property = cast(ArrayProperty, property)
            result = self.generate_array_loop(source, target, array_property.item_type, referenced_schemas)
        elif property.type == Property.Type.SCHEMA_REFERENCE:
            schema_property = cast(SchemaReferenceProperty, property)
            referenced_schemas.add(schema_property.ref)
            result = ''
            dereference_style = '.'
            if schema_property.ref in self.recursive_schemas:
                result = f'{target} = make_uniq<{schema_property.ref}>();\n'
                dereference_style = '->'
            result += f"""error = {target}{dereference_style}TryFromJSON({source});
if (!error.empty()) {{
    return error;
}}"""
        else:
            item_parse = self.generate_item_parse(property, source, referenced_schemas)
            result = f'{target} = {item_parse};'
        return result

    def generate_optional_properties(self, name: str, properties: Dict[str, Property], referenced_schemas: set):
        if not properties:
            return ''
        res = []
        for item, optional_property in properties.items():
            optional_property = properties[item]
            variable_name = safe_cpp_name(item)
            variable_assignment = self.generate_assignment(
                optional_property, variable_name, f'{variable_name}_val', referenced_schemas
            )
            variable_assignment = '\n'.join([f'\t{x}' for x in variable_assignment.split('\n')])
            res.append(
                f"""
auto {variable_name}_val = yyjson_obj_get(obj, "{variable_name}");
if ({variable_name}_val) {{
{variable_assignment}
}}"""
            )
        return '\n'.join(res)

    def generate_required_properties(self, name: str, properties: Dict[str, Property], referenced_schemas: set):
        if not properties:
            return ''
        res = []
        for item, required_property in properties.items():
            variable_name = safe_cpp_name(item)
            variable_assignment = self.generate_assignment(
                required_property, variable_name, f'{variable_name}_val', referenced_schemas
            )
            res.append(
                f"""
auto {variable_name}_val = yyjson_obj_get(obj, "{variable_name}");
if (!{variable_name}_val) {{
    return "{name} required property '{variable_name}' is missing";
}} else {{
{variable_assignment}
}}"""
            )
        return '\n'.join(res)

    def generate_additional_properties(
        self, properties: List[str], additional_properties: Property, referenced_schemas: set
    ) -> str:
        if not additional_properties:
            return ''

        if properties:
            handled_properties = f"""case_insensitive_set_t handled_properties {{
    {', '.join(f'"{x}"' for x in properties)}
}};
"""
            skip_handled_properties = """if (handled_properties.count(key_str)) {
    continue;
}"""
        else:
            handled_properties = ''
            skip_handled_properties = ''

        if additional_properties.type != Property.Type.SCHEMA_REFERENCE:
            item_definition = self.generate_item_parse(additional_properties, 'val', referenced_schemas)
            item_definition = f'\tauto tmp = {item_definition};\n'
        else:
            schema_property = cast(SchemaReferenceProperty, additional_properties)
            referenced_schemas.add(schema_property.ref)
            if schema_property.ref in self.recursive_schemas:
                print(f"Encountered recursive schema '{schema_property.ref}' in 'generate_additional_properties'")
                exit(1)
            item_definition = f'\t{schema_property.ref} tmp;\n'
            item_definition += """error = tmp.TryFromJSON(val);
if (!error.empty()) {
    return error;
}"""

        return ADDITIONAL_PROPERTIES_FORMAT.format(
            HANDLED_PROPERTIES=handled_properties,
            SKIP_HANDLED_PROPERTIES=skip_handled_properties,
            PARSE_PROPERTY=item_definition,
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

    def generate_array_class(self, schema: Property, name: str, referenced_schemas: set):
        assert schema.type == Property.Type.ARRAY
        array_property = cast(ArrayProperty, schema)

        assert not array_property.all_of
        assert not array_property.one_of
        assert not array_property.any_of

        body = self.generate_array_loop('obj', 'value', array_property.item_type, referenced_schemas)

        nested_classes = self.generate_nested_class_definitions(referenced_schemas)

        variable_type = self.generate_variable_type(schema)
        variable_definition = f'\t{variable_type} value;'

        body = '\n'.join([f'\t{x}' for x in body.split('\n')])
        class_definition = CLASS_FORMAT.format(
            CLASS_NAME=name,
            NESTED_CLASSES=nested_classes,
            BASE_CLASS_PARSING='',
            REQUIRED_PROPERTIES=body,
            OPTIONAL_PROPERTIES='',
            ADDITIONAL_PROPERTIES='',
            BASE_CLASS_VARIABLES='',
            PROPERTY_VARIABLES=variable_definition,
        )
        return class_definition

    def generate_primitive_class(self, schema: Property, name: str, referenced_schemas: set):
        assert not schema.all_of
        assert not schema.one_of
        assert not schema.any_of

        # TODO: implement this

        variable_parsing = self.generate_assignment(schema, 'value', 'obj', referenced_schemas)

        variable_type = self.generate_variable_type(schema)
        variable_definition = f'\t{variable_type} value;'

        class_definition = CLASS_FORMAT.format(
            CLASS_NAME=name,
            NESTED_CLASSES='',
            BASE_CLASS_PARSING='',
            REQUIRED_PROPERTIES=variable_parsing,
            OPTIONAL_PROPERTIES='',
            ADDITIONAL_PROPERTIES='',
            BASE_CLASS_VARIABLES='',
            PROPERTY_VARIABLES=variable_definition,
        )
        return class_definition

    def generate_nested_class_definitions(self, referenced_schemas: set):
        generated_schemas_referenced = [x for x in referenced_schemas if x not in self.schemas]
        nested_classes = []
        for item in generated_schemas_referenced:
            parsed_schema = self.parsed_schemas[item]
            content = self.generate_class(parsed_schema, item, referenced_schemas)
            nested_classes.append(content)

        if not nested_classes:
            return ''
        nested_classes = '\n'.join(nested_classes)
        nested_classes = '\n'.join([f'\t{x}' for x in nested_classes.split('\n')])
        return 'public:\n' + nested_classes

    def generate_object_class(self, schema: Property, name: str, referenced_schemas: set):
        assert schema.type == Property.Type.OBJECT
        object_property = cast(ObjectProperty, schema)
        base_classes = set()

        # Parse any base classes required for the schema (anyOf, allOf, oneOf)
        all_of_parsing = self.generate_all_of(name, schema, base_classes)
        one_of_parsing = self.generate_one_of(name, schema, base_classes)
        any_of_parsing = self.generate_any_of(name, schema, base_classes)

        base_class_parsing = []
        if all_of_parsing:
            base_class_parsing.append(all_of_parsing)
        if one_of_parsing:
            base_class_parsing.append(one_of_parsing)
        if any_of_parsing:
            base_class_parsing.append(any_of_parsing)

        referenced_schemas.update(base_classes)

        required = object_property.required
        if not required:
            required = []
        remaining_properties = [x for x in object_property.properties if x not in required]

        required_properties = {}
        optional_properties = {}
        for item in remaining_properties:
            optional_properties[item] = object_property.properties[item]
        for item in required:
            required_properties[item] = object_property.properties[item]

        required_property_parsing = self.generate_required_properties(name, required_properties, referenced_schemas)
        required_property_parsing = '\n'.join([f'\t{x}' for x in required_property_parsing.split('\n')])

        optional_property_parsing = self.generate_optional_properties(name, optional_properties, referenced_schemas)
        optional_property_parsing = '\n'.join([f'\t{x}' for x in optional_property_parsing.split('\n')])

        additional_property_parsing = self.generate_additional_properties(
            object_property.properties.keys(), object_property.additional_properties, referenced_schemas
        )
        additional_property_parsing = '\n'.join([f'\t{x}' for x in additional_property_parsing.split('\n')])

        if any([x.startswith('Object') for x in referenced_schemas]):
            print("referenced_schemas", referenced_schemas)

        base_class_variables = []
        for item in base_classes:
            base_class = self.parsed_schemas[item]
            variable_name = to_snake_case(item)
            base_class_variables.append(f'\t{item} {variable_name};')

        nested_classes = self.generate_nested_class_definitions(referenced_schemas)

        variable_definitions = []
        for item in sorted(list(object_property.properties.keys())):
            variable = object_property.properties[item]
            variable_type = self.generate_variable_type(variable)
            variable_definitions.append(f'\t{variable_type} {safe_cpp_name(item)};')

        for item in object_property.any_of:
            item_name = to_snake_case(item.ref)
            variable_definitions.append(f'\tbool has_{safe_cpp_name(item_name)} = false;')
        for item in object_property.one_of:
            item_name = to_snake_case(item.ref)
            variable_definitions.append(f'\tbool has_{safe_cpp_name(item_name)} = false;')

        if object_property.additional_properties:
            variable_type = self.generate_variable_type(object_property.additional_properties)
            variable_definitions.append(f'\tcase_insensitive_map_t<{variable_type}> additional_properties;')

        class_definition = CLASS_FORMAT.format(
            CLASS_NAME=name,
            NESTED_CLASSES=nested_classes,
            BASE_CLASS_PARSING='\n'.join(base_class_parsing),
            REQUIRED_PROPERTIES=required_property_parsing,
            OPTIONAL_PROPERTIES=optional_property_parsing,
            ADDITIONAL_PROPERTIES=additional_property_parsing,
            BASE_CLASS_VARIABLES='\n'.join(base_class_variables),
            PROPERTY_VARIABLES='\n'.join(variable_definitions),
        )
        return class_definition

    def generate_class(self, schema: Property, name: str, referenced_schemas: set):
        new_referenced_schemas = set()
        if schema.type == Property.Type.OBJECT:
            result = self.generate_object_class(schema, name, new_referenced_schemas)
        elif schema.type == Property.Type.ARRAY:
            result = self.generate_array_class(schema, name, new_referenced_schemas)
        elif schema.type == Property.Type.PRIMITIVE:
            result = self.generate_primitive_class(schema, name, new_referenced_schemas)
        else:
            print(f"Unrecognized 'generate_schema' type {schema.type}")
        referenced_schemas.update(new_referenced_schemas)
        return result

    def generate_schema(self, schema: Property, name: str):
        referenced_schemas = set()
        class_definition = self.generate_class(schema, name, referenced_schemas)

        include_schemas = [x for x in referenced_schemas if x in self.schemas]
        additional_headers = [
            f'#include "rest_catalog/objects/{to_snake_case(x)}.hpp"' for x in sorted(list(include_schemas))
        ]

        file_contents = HEADER_FORMAT.format(
            ADDITIONAL_HEADERS='\n'.join(additional_headers), CLASS_DEFINITION=class_definition
        )
        return file_contents

    def generate_list_header(self) -> str:
        lines = ["", "// This file is automatically generated and contains all REST API object headers", ""]

        # Add includes for all generated headers
        for name in self.schemas:
            lines.append(f'#include "rest_catalog/objects/{to_snake_case(name)}.hpp"')
        return '\n'.join(lines)

    def generate_all_schemas(self):
        # Create directory if it doesn't exist
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        with open(os.path.join(OUTPUT_DIR, 'list.hpp'), 'w') as f:
            f.write(self.generate_list_header())

        for name in self.schemas:
            schema = self.parsed_schemas[name]
            file_content = self.generate_schema(schema, name)
            output_path = os.path.join(OUTPUT_DIR, f'{to_snake_case(name)}.hpp')
            with open(output_path, 'w') as f:
                f.write(file_content)


if __name__ == '__main__':
    generator = ResponseObjectsGenerator(API_SPEC_PATH)
    generator.parse_all_schemas()

    # schema = generator.parsed_schemas['AndOrExpression']
    # content = generator.generate_schema(schema, 'AndOrExpression')
    # print(content)
    # exit(1)

    generator.generate_all_schemas()
    print("finito")
