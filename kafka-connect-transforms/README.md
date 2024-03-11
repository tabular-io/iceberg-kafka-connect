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

| Property                     | Description                                                                                             |
|:-----------------------------|:--------------------------------------------------------------------------------------------------------|
| cdc.kafka.include_metadata   | Boolean (true) to append Kafka topic/partition/offset/timestamp metadta to each record                  |
| cdc.kafka.metadata_field     | Key to append metadata on.  Defaults to `_kafka_metadta`                                                |
| cdc.kafka.metadata_is_nested | If true nests metadata on a struct under field name, otherwise prefixes metadata fields with field name | 
| cdc.kafka.external_field     | Optional `key,value` string to append a static field with Kafka Metadata.  E.g. cluster name            |

# DebeziumTransform
_(Experimental)_

The `DebeziumTransform` SMT transforms a Debezium formatted message for use by the sink's CDC feature.
It will promote the `before` or `after` element fields to top level and add the following metadata fields:
`_cdc.op`, `_cdc.ts`, `_cdc.offset`, `_cdc.source`, `_cdc.target`, and `_cdc.key`.

If `cdc.kafka.include_metadata` it will add the following metadata fields:
`_kafka_metadata_topic`, `kafka_metadata_partition`, `kafka_metadata_offset`, and `kafka_metadata_record_timestamp`.

## Configuration

| Property                     | Description                                                                                             |
|:-----------------------------|:--------------------------------------------------------------------------------------------------------|
| cdc.target.pattern           | Pattern to use for setting the CDC target field value, default is `{db}.{table}`                        |
| cdc.kafka.include_metadata   | Boolean (true) to append Kafka topic/partition/offset/timestamp metadta to each record                  |
| cdc.kafka.metadata_field     | Key to append metadata on.  Defaults to `_kafka_metadta`                                                |
| cdc.kafka.metadata_is_nested | If true nests metadata on a struct under field name, otherwise prefixes metadata fields with field name |
| cdc.kafka.external_field     | Optional `key,value` string to append a static field with Kafka Metadata.  E.g. cluster name            |

If `cdc.kafka.include_metadata` it will add the following metadata fields:
`_kafka_metadata_topic`, `kafka_metadata_partition`, `kafka_metadata_offset`, and `kafka_metadata_record_timestamp`. 