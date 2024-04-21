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

# MongoDebeziumTransform
_(Experimental)_ 

The `MongoDebeziumTransform` SMT transforms a Mongo Debezium formatted message with `before`/`after` BSON
strings into `before`/`after` typed Structs that the `DebeziumTransform` SMT expects. 

It does not (yet) support renaming columns if mongodb column is not supported by your underlying 
catalog type.  

## Configuration

| Property            | Description                                      |
|---------------------|--------------------------------------------------|
| array_handling_mode  | `array` or `document` to set array handling mode |

Value array (the default) will encode arrays as the array datatype. It is user’s responsibility to ensure that 
all elements for a given array instance are of the same type. This option is a restricting one but offers 
easy processing of arrays by downstream clients.

Value document will convert the array into a struct of structs in the similar way as done by BSON serialization. 
The main struct contains fields named _0, _1, _2 etc. where the name represents the index of the element in the array.
Every element is then passed as the value for the given field.