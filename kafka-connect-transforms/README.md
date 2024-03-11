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

# KafkaMetadataTransform
_(Experimental)_

The `KafkaMetadata` injects `topic`, `partition`, `offset`, `record timestamp`.

## Configuration

| Property                                   | Description (default value)                                                       |
|--------------------------------------------|-----------------------------------------------------------------------------------|
| transforms.kafka_metadata.include_metadata | (true) includes kafka metadata.  False becomes a no-op                            | 
| transforms.kafka_metadata.metadata_field   | (_kafka_metadata) prefix for fields                                               | 
| transforms.kafka_metadata.nested           | (false) if true, nests data on a struct else adds to top level as prefixed fields |
| transforms.kafka_metadata.external_field   | (none) appends a constant `key,value` to the metadata (e.g. cluster name)         | 

If `nested` is on: 

`_kafka_metadata.topic`, `_kafka_metadata.partition`, `kafka_metadata.offset`, `kafka_metadata.record_timestamp`

If `nested` is off:
`_kafka_metdata_topic`, `kafka_metadata_partition`, `kafka_metadata_offset`, `kafka_metadata_record_timestamp`

# DmsTransform
_(Experimental)_

The `DmsTransform` SMT transforms an AWS DMS formatted message for use by the sink's CDC feature.
It will promote the `data` element fields to top level and add the following metadata fields:
`_cdc.op`, `_cdc.ts`, and `_cdc.source`.

## Configuration

The DMS transform can also append Kafka Metadata without an additional record copy as per the `KafkaMetadataTransform` with the following
configuration: 

| Property                                   | Description (default value)                                                       |
|--------------------------------------------|-----------------------------------------------------------------------------------|
| transforms.kafka_metadata.include_metadata | (false) includes kafka metadata.  False will not append data to DMS transform     | 
| transforms.kafka_metadata.metadata_field   | (_kafka_metadata) prefix for fields                                               | 
| transforms.kafka_metadata.nested           | (false) if true, nests data on a struct else adds to top level as prefixed fields |
| transforms.kafka_metadata.external_field   | (none) appends a constant `key,value` to the metadata (e.g. cluster name)         | 


# DebeziumTransform
_(Experimental)_

The `DebeziumTransform` SMT transforms a Debezium formatted message for use by the sink's CDC feature.
It will promote the `before` or `after` element fields to top level and add the following metadata fields:
`_cdc.op`, `_cdc.ts`, `_cdc.offset`, `_cdc.source`, `_cdc.target`, and `_cdc.key`.

## Configuration

| Property                     | Description                                                                                             |
|:-----------------------------|:--------------------------------------------------------------------------------------------------------|
| cdc.target.pattern           | Pattern to use for setting the CDC target field value, default is `{db}.{table}`                        |

The Debezium transform can also append Kafka Metadata without an additional record copy as per the `KafkaMetadataTransform` with the following
configuration:

| Property                                   | Description (default value)                                                       |
|--------------------------------------------|-----------------------------------------------------------------------------------|
| transforms.kafka_metadata.include_metadata | (false) includes kafka metadata.  False will not append data to DMS transform     | 
| transforms.kafka_metadata.metadata_field   | (_kafka_metadata) prefix for fields                                               | 
| transforms.kafka_metadata.nested           | (false) if true, nests data on a struct else adds to top level as prefixed fields |
| transforms.kafka_metadata.external_field   | (none) appends a constant `key,value` to the metadata (e.g. cluster name)         | 