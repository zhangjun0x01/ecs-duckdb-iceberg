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
}

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
    {CLASS_NAME}::{CLASS_NAME}() {{}}
public:
    static {CLASS_NAME} FromJSON(yyjson_val *obj) {{
        auto error = TryFromJSON(obj);
        if (!error.empty()) {{
            throw InvalidInputException(error);
        }}
        return *this;
    }}
public:
    string TryFromJSON(yyjson_val *obj) {{
        string error;
{BASE_CLASS_PARSING}
{REQUIRED_PROPERTIES}
{OPTIONAL_PROPERTIES}
        return string();
    }}
public:
{BASE_CLASS_VARIABLES}
public:
{PROPERTY_VARIABLES}
}};
"""


class Property:
    class Type(Enum):
        PRIMITIVE = auto()
        ARRAY = auto()
        OBJECT = auto()
        SCHEMA_REFERENCE = auto()

    def __init__(self, type: "Property.Type"):
        self.type = type
        # TODO: need to be prepared to generate additional structs, not always a '$ref' (see 'Schema')
        self.all_of: List[Property] = []
        self.any_of: List[Property] = []
        self.one_of: List[Property] = []

    def get_referenced_schemas_base(self):
        res = set()
        for item in self.all_of:
            res.update(item.get_referenced_schemas())
        for item in self.any_of:
            res.update(item.get_referenced_schemas())
        for item in self.one_of:
            res.update(item.get_referenced_schemas())
        return res

    def get_referenced_schemas(self):
        print(f"'get_referenced_schemas' not implemented for property type {self.type}")
        exit(1)


class SchemaReferenceProperty(Property):
    def __init__(self, name):
        super().__init__(Property.Type.SCHEMA_REFERENCE)
        self.ref = name

    def get_referenced_schemas(self):
        res = super().get_referenced_schemas_base()
        res.add(self.ref)
        return res


class ArrayProperty(Property):
    def __init__(self):
        super().__init__(Property.Type.ARRAY)
        self.item_type: Optional[Property] = None

    def get_referenced_schemas(self):
        res = super().get_referenced_schemas_base()
        res.update(self.item_type.get_referenced_schemas())
        return res


class PrimitiveProperty(Property):
    def __init__(self):
        super().__init__(Property.Type.PRIMITIVE)
        self.primitive_type: Optional[str] = None
        self.format = None
        # TODO: if 'enum' is present, we should verify that the value of the property is one of the accepted values
        self.enum: Optional[List[str]] = None
        # TODO: same for this, this property *has* to have this value
        self.const: Optional[str] = None

    def get_referenced_schemas(self):
        return super().get_referenced_schemas_base()


"""
allOf: the object should satisfy all of the constraints of every entry of the allOf list
(we can add each reference as member variable to the created class, using the base 'obj' as argument to the FromJSON)

class AllOfExample {
public:
    AllOfExample() {}
public:
    static AllOfExample FromJSON(yyjson_val *obj) {
        AllOfExample res;
        auto error = res.TryFromJSON(obj);
        if (!error.empty()) {
            throw InvalidInputException(error);
        }
        return res;
    }
    string TryFromJSON(yyjson_val *obj) {
        string error;

        error = base_foo.TryFromJSON(obj);
        if (!error.empty()) {
            return error;
        }
        error = base_bar.TryFromJSON(obj);
        if (!error.empty()) {
            return error;
        }
        error = base_baz.TryFromJSON(obj);
        if (!error.empty()) {
            return error;
        }

        auto type_val = yyjson_obj_get(obj, "type");
        if (type_val) {
            type = yyjson_get_str(type_val);
        }

        auto content_val = yyjson_obj_get(obj, "content");
        if (content_val) {
            content = yyjson_get_str(content_val);
        }

        auto id_val = yyjson_obj_get(obj, "id");
        if (id_val) {
            id = yyjson_get_sint(id_val);
        }
        return string();
    }
public:
    Foo base_foo;
    bool has_foo = false;
    Bar base_bar;
    bool has_bar = false;
    Baz base_baz;
    bool has_baz = false;
public:
    string type;
    string content;
    int64_t id;
};

oneOf: the property will satisfy one of the referenced schemas
(we can add 'has_<ref>' boolean member variables to the class, along with every referenced schema)
we probably want to create a TryFromJSON method, which takes in an instance of the class, returns a string
if string is empty - it was successful, otherwise it wasn't

class OneOfExample {
public:
    OneOfExample() {}
public:
    static OneOfExample FromJSON(yyjson_val *obj) {
        OneOfExample res;
        auto error = res.TryFromJSON(obj);
        if (!error.empty()) {
            throw InvalidInputException(error);
        }
        return res;
    }
    string TryFromJSON(yyjson_val *obj) {
        string error;

        error = base_foo.TryFromJSON(obj);
        if (error.empty()) {
            has_foo = true;
            return string();
        }
        error = base_bar.TryFromJSON(obj);
        if (error.empty()) {
            has_bar = true;
            return string();
        }
        error = base_baz.TryFromJSON(obj);
        if (error.empty()) {
            has_baz = true;
            return string();
        }
        return "OneOfExample failed to parse, none of the oneOf candidates matched";

        auto type_val = yyjson_obj_get(obj, "type");
        if (type_val) {
            type = yyjson_get_str(type_val);
        }

        auto content_val = yyjson_obj_get(obj, "content");
        if (content_val) {
            content = yyjson_get_str(content_val);
        }

        auto id_val = yyjson_obj_get(obj, "id");
        if (id_val) {
            id = yyjson_get_sint(id_val);
        }
        return string();
    }
public:
    Foo base_foo;
    bool has_foo = false;
    Bar base_bar;
    bool has_bar = false;
    Baz base_baz;
    bool has_baz = false;
public:
    string type;
    string content;
    int64_t id;
};

anyOf: same as oneOf but it matches one or more of the referenced schemas
the TryFromJSON will also be handy here
(TODO: make sure the base class variables dont collide with any property variable)

class AnyOfExample {
public:
    AnyOfExample() {}
public:
    static AnyOfExample FromJSON(yyjson_val *obj) {
        AnyOfExample res;
        auto error = res.TryFromJSON(obj);
        if (!error.empty()) {
            throw InvalidInputException(error);
        }
        return res;
    }
    string TryFromJSON(yyjson_val *obj) {
        string error;

        error = base_foo.TryFromJSON(obj);
        if (error.empty()) {
            has_foo = true;
        }
        error = base_bar.TryFromJSON(obj);
        if (error.empty()) {
            has_bar = true;
        }
        error = base_baz.TryFromJSON(obj);
        if (error.empty()) {
            has_baz = true;
        }
        if (!has_foo && !has_bar && !has_baz) {
            return "AnyOfExample failed to parse, none of the anyOf candidates matched";
        }

        auto type_val = yyjson_obj_get(obj, "type");
        if (type_val) {
            type = yyjson_get_str(type_val);
        }

        auto content_val = yyjson_obj_get(obj, "content");
        if (content_val) {
            content = yyjson_get_str(content_val);
        }

        auto id_val = yyjson_obj_get(obj, "id");
        if (id_val) {
            id = yyjson_get_sint(id_val);
        }
        return string();
    }
public:
    Foo base_foo;
    bool has_foo = false;
    Bar base_bar;
    bool has_bar = false;
    Baz base_baz;
    bool has_baz = false;
public:
    string type;
    string content;
    int64_t id;
};

"""


class ObjectProperty(Property):
    def __init__(self):
        super().__init__(Property.Type.OBJECT)
        # TODO: when generating the C++ code, these properties should be present, anything else is optional
        self.required = []
        self.properties: Dict[str, Property] = {}
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

    def get_referenced_schemas(self):
        res = super().get_referenced_schemas_base()
        for item in self.properties:
            res.update(self.properties[item].get_referenced_schemas())
        return res


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

    def parse_object_property(self, spec: dict, result: Property):
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

    def parse_primitive_property(self, spec: dict, result: Property):
        primitive_type = spec['type']
        format = spec.get('format')
        assert primitive_type in PRIMITIVE_TYPES
        assert result.type == Property.Type.PRIMITIVE
        primitive_result = cast(PrimitiveProperty, result)
        primitive_result.format = format
        primitive_result.primitive_type = primitive_type

    def parse_array_property(self, spec: dict, result: Property):
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

    def parse_schema(self, name: str):
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

        property = self.parse_property(schema, name)
        property.reference = name

        self.schemas_being_parsed.remove(name)
        self.parsed_schemas[name] = property

    def parse_all_schemas(self):
        for name in self.schemas:
            self.parse_schema(name)

    # Generation of CPP code

    def generate_all_of(self, name: str, property: Property, nested_classes: Dict[str, Property], base_classes: set):
        if not property.all_of:
            return ''
        res = []
        for item in property.all_of:
            if item.type == Property.Type.SCHEMA_REFERENCE:
                class_name = item.ref
            else:
                class_name = f'{name}Partial{len(nested_classes) + 1}'
                nested_classes[class_name] = item
            base_classes.add(class_name)
            property_name = to_snake_case(class_name)
            res.append(
                f"""
error = base_{property_name}.TryFromJSON(obj);
if (!error.empty()) {{
    return error;
}}"""
            )
        return '\n'.join(res)

    def generate_any_of(self, name: str, property: Property, nested_classes: Dict[str, Property], base_classes: set):
        if not property.any_of:
            return ''
        all_base_classes = set()
        res = []
        for item in property.any_of:
            if item.type == Property.Type.SCHEMA_REFERENCE:
                class_name = item.ref
            else:
                class_name = f'{name}Partial{len(nested_classes) + 1}'
                nested_classes[class_name] = item
            base_classes.add(class_name)
            property_name = to_snake_case(class_name)
            all_base_classes.add(property_name)
            res.append(
                f"""
error = base_{property_name}.TryFromJSON(obj);
if (error.empty()) {{
    has_{property_name} = true;
}}"""
            )
        condition = ' && '.join(f'!has_{x}' for x in sorted(list(all_base_classes)))
        res.append(
            f"""
if ({condition}) {{
\treturn "{name} failed to parse, none of the anyOf candidates matched";
}}"""
        )
        return '\n'.join(res)

    def generate_one_of(self, name: str, property: Property, nested_classes: Dict[str, Property], base_classes: set):
        if not property.one_of:
            return ''
        res = []

        res.append('do {')
        for item in property.one_of:
            if item.type == Property.Type.SCHEMA_REFERENCE:
                class_name = item.ref
            else:
                class_name = f'{name}Partial{len(nested_classes) + 1}'
                nested_classes[class_name] = item
            base_classes.add(class_name)
            property_name = to_snake_case(class_name)
            res.append(
                f"""\terror = base_{property_name}.TryFromJSON(obj);
\tif (error.empty()) {{
\t\thas_{property_name} = true;
\t\tbreak;
\t}}"""
            )
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

        ARRAY_PUSHBACK_FORMAT = "\tresult.{ARRAY_VARIABLE}.push_back({ITEM_PARSE});"
        item_parse = self.generate_item_parse(item_type, 'val', referenced_schemas)

        body = ''
        body += ARRAY_PUSHBACK_FORMAT.format(ARRAY_VARIABLE=destination_name, ITEM_PARSE=item_parse)
        res.append(body)
        res.append('}')
        return '\n'.join(res)

    def generate_item_parse(self, property: Property, source: str, referenced_schemas: set) -> str:
        if property.type == Property.Type.SCHEMA_REFERENCE:
            schema_property = cast(SchemaReferenceProperty, property)
            referenced_schemas.add(schema_property.ref)
            return f'{schema_property.ref}::FromJSON({source})'
        elif property.type == Property.Type.ARRAY:
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
            if primitive_property.primitive_type not in PRIMITIVE_PARSE_FUNCTIONS:
                print(f"Unrecognized primitive type '{primitive_property.primitive_type}' encountered in array")
                exit(1)
            parse_func = PRIMITIVE_PARSE_FUNCTIONS[primitive_property.primitive_type]
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
        else:
            item_parse = self.generate_item_parse(property, source, referenced_schemas)
            result = f'result.{target} = {item_parse};'
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
{variable_assignment};
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
    return "{name} required property '{variable_name}' is missing");
}}
{variable_assignment}"""
            )
        return '\n'.join(res)

    def generate_array_schema(self, schema: Property, name: str):
        assert schema.type == Property.Type.ARRAY
        array_property = cast(ArrayProperty, schema)

        referenced_schemas = schema.get_referenced_schemas()
        include_schemas = [x for x in referenced_schemas if x not in self.schemas]
        additional_headers = [
            f'#include "rest_catalog/objects/{to_snake_case(x)}.hpp' for x in sorted(list(include_schemas))
        ]

        referenced_schemas: Set[str] = set()
        body = self.generate_array_loop('obj', 'value', array_property.item_type, referenced_schemas)
        body = '\n'.join([f'\t{x}' for x in body.split('\n')])
        class_definition = CLASS_FORMAT.format(
            CLASS_NAME=name,
            BASE_CLASS_PARSING='',
            REQUIRED_PROPERTIES=body,
            OPTIONAL_PROPERTIES='',
            BASE_CLASS_VARIABLES='',
            PROPERTY_VARIABLES='',
        )

        file_contents = HEADER_FORMAT.format(
            ADDITIONAL_HEADERS='\n'.join(additional_headers), CLASS_DEFINITION=class_definition
        )
        return file_contents

    def generate_primitive_schema(self, schema: Property, name: str):
        # TODO: implement this
        return ''

    def generate_object_schema(self, schema: Property, name: str):
        assert schema.type == Property.Type.OBJECT
        object_property = cast(ObjectProperty, schema)

        # Find all the headers we need to include (direct references)
        referenced_schemas = schema.get_referenced_schemas()
        include_schemas = [x for x in referenced_schemas if x not in self.schemas]
        additional_headers = [
            f'#include "rest_catalog/objects/{to_snake_case(x)}.hpp' for x in sorted(list(include_schemas))
        ]

        nested_classes: Dict[str, Property] = {}
        base_classes = set()

        # Parse any base classes required for the schema (anyOf, allOf, oneOf)
        all_of_parsing = self.generate_all_of(name, schema, nested_classes, base_classes)
        one_of_parsing = self.generate_one_of(name, schema, nested_classes, base_classes)
        any_of_parsing = self.generate_any_of(name, schema, nested_classes, base_classes)

        base_class_parsing = []
        if all_of_parsing:
            base_class_parsing.append(all_of_parsing)
        if one_of_parsing:
            base_class_parsing.append(one_of_parsing)
        if any_of_parsing:
            base_class_parsing.append(any_of_parsing)

        referenced_schemas: Set[str] = set()

        required = object_property.required
        if not required:
            required = []
        remaining_properties = [x for x in object_property.properties if x not in required]
        if not remaining_properties:
            return ''

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

        if any([x.startswith('Object') for x in referenced_schemas]):
            print("referenced_schemas", referenced_schemas)

        if any([x.startswith('Object') for x in nested_classes.keys()]):
            print("nested_classes", nested_classes)

        # Parse the nested classes (objects that are not $ref, split for easy of generation)
        if nested_classes:
            keys = list(nested_classes.keys())
            nested_class = nested_classes[keys[0]]
            content = self.generate_schema(nested_class, keys[0])

        base_class_variables = []
        for item in base_classes:
            if item in nested_classes:
                base_class = nested_classes[item]
            else:
                base_class = self.parsed_schemas[item]
            variable_name = to_snake_case(item)
            base_class_variables.append(f'\t{item} {variable_name};')

        class_definition = CLASS_FORMAT.format(
            CLASS_NAME=name,
            BASE_CLASS_PARSING='\n'.join(base_class_parsing),
            REQUIRED_PROPERTIES=required_property_parsing,
            OPTIONAL_PROPERTIES=optional_property_parsing,
            BASE_CLASS_VARIABLES='\n'.join(base_class_variables),
            PROPERTY_VARIABLES='',
        )

        file_contents = HEADER_FORMAT.format(
            ADDITIONAL_HEADERS='\n'.join(additional_headers), CLASS_DEFINITION=class_definition
        )
        print(file_contents)
        exit(1)
        return file_contents

    def generate_schema(self, schema: Property, name: str):
        if schema.type == Property.Type.OBJECT:
            return self.generate_object_schema(schema, name)
        elif schema.type == Property.Type.ARRAY:
            return self.generate_array_schema(schema, name)
        elif schema.type == Property.Type.PRIMITIVE:
            return self.generate_primitive_schema(schema, name)
        else:
            print(f"Unrecognized 'generate_schema' type {schema.type}")

    def generate_all_schemas(self):
        for name in self.schemas:
            schema = self.parsed_schemas[name]
            self.generate_schema(schema, name)


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
            f.write(schema.parse_header_file())

    with open(os.path.join(OUTPUT_DIR, 'list.hpp'), 'w') as f:
        f.write(parse_list_header(parsed_schemas))


if __name__ == '__main__':
    # main()
    generator = ResponseObjectsGenerator(API_SPEC_PATH)
    generator.parse_all_schemas()
    generator.generate_all_schemas()
