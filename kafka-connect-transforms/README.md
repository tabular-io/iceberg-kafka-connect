# SMTs for the Apache Iceberg Sink Connector

This project contains some SMTs that could be useful when transforming Kafka data for use by
the Iceberg sink connector.

# CopyValue
_(Experimental)_

The `CopyValue` SMT copies a value from one field to a new field.

## Configuration

| Property         | Description       |
|------------------|-------------------|
| source.field     | Source field name |
| target.field     | Target field name |

## Example

```
"transforms": "copyId",
"transforms.copyId.type": "io.tabular.iceberg.connect.transforms.CopyValue",
"transforms.copyId.source.field": "id",
"transforms.copyId.target.field": "id_copy",
```

# DmsTransform
_(Experimental)_

The `DmsTransform` SMT transforms an AWS DMS formatted message for use by the sink's CDC feature.
It will promote the `data` element fields to top level and add the following metadata fields:
`_cdc.op`, `_cdc.ts`, and `_cdc.source`.

## Configuration

The SMT currently has no configuration.

# DebeziumTransform
_(Experimental)_

The `DebeziumTransform` SMT transforms a Debezium formatted message for use by the sink's CDC feature.
It will promote the `before` or `after` element fields to top level and add the following metadata fields:
`_cdc.op`, `_cdc.ts`, `_cdc.offset`, `_cdc.source`, `_cdc.target`, and `_cdc.key`.

## Configuration

| Property            | Description                                                                       |
|---------------------|-----------------------------------------------------------------------------------|
| cdc.target.pattern  | Pattern to use for setting the CDC target field value, default is `{db}.{table}`  |

# JsonToMapTransform
_(Experimental)_ 

The `JsonToMapTransform` SMT parses Json object payloads.  It is intended for use when the incoming data is too 
inconsistent for the `iceberg-connector` to infer `Struct` schemas from (e.g. the object keys are too variable 
and this leads to an explosion of schema evolutions and columns).  This will get the data into Iceberg where it can 
be further processed by query engines into a more manageable form.  

It will parse string values into `SinkRecords` with a Schema and a Struct.  It assumes the messages are json objects 
themselves and will throw exceptions if the records are primitives. 

## Configuration

| Property             | Description  (default value)             |
|----------------------|------------------------------------------|
| transforms.json.root | (false) Boolean value to start at root   |

The `transform.json.root` is meant for the most inconsistent data.  It will construct a Struct with a single field 
called `payload` with a Schema of `Map<String, String>`.  

If `transform.json.root` is false (the default), it will construct a Struct with inferred schemas for primitive and
array fields.  Nested objects become fields of type `Map<String, String>`.

Keys with empty arrays and empty objects are filtered out from the final schema.  Arrays will be typed unless the 
json arrays have mixed types in which case they are converted to arrays of strings.

Example json: 

```json
{
  "key": 1, 
  "array": [1,"two",3],
  "empty_obj": {},
  "nested_obj": {"some_key": ["one", "two"]}
}
```

Will become the following if `transform.json.root` is true:

```
SinkRecord.schema: 
  "payload" : (Optional) Map<String, String>
  
Sinkrecord.value (Struct): 
  "payload"  : Map(
    "key" : "1",
    "array" : "[1,"two",3]"
    "empty_obj": "{}"
    "nested_obj": "{"some_key":["one","two"]}}"
   )
```

Will become the following if `transform.json.root` is false

```
SinkRecord.schema: 
  "key": (Optional) Int32,
  "array": (Optional) Array<String>,
  "nested_object": (Optional) Map<string, String>
  
SinkRecord.value (Struct):
 "key" 1, 
 "array" ["1", "two", "3"] 
 "nested_object" Map ("some_key" : "["one", "two"]") 
```
