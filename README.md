# Iceberg Sink Connector
The Iceberg Sink Connector for Kafka Connect is a sink connector for writing data from Kafka into Iceberg tables.

# Features
* Commit coordination for centralized Iceberg commits
* Exactly-once delivery semantics
* Multi-table output
* Row mutations (update/delete rows), upsert mode
* Message conversion using the Iceberg schema as the source of truth
* Field name mapping via Iceberg’s column mapping functionality

# Installation
The Iceberg Sink Connector is under active development, with an official release coming soon. For
now you can build the connector zip archive by running:
```bash
./gradlew -xtest clean build
```
The zip archive will be found under `./kafka-connect-runtime/build/distributions`.

# Examples

## Initial setup

### Source topic
This assumes the source topic already exists and is named `events`.

### Control topic
If your Kafka cluster has `auto.create.topics.enable` set to `true` (the default), then the control topic will be automatically created. If not, then you will need to create the topic first. The default topic name is `control-<connector name>`:
```bash
bin/kafka-topics  \
  --command-config command-config.props \
  --bootstrap-server ${CONNECT_BOOTSTRAP_SERVERS} \
  --create \
  --topic control-events-sink \
  --partitions 1
```
*NOTE: Clusters running on Confluent Cloud have `auto.create.topics.enable` set to `false` by default.*

## Single destination table
This example writes all incoming records to a single table.

### Create the destination table
```sql
CREATE TABLE default.events (
    id STRING,
    type STRING,
    ts TIMESTAMP,
    payload STRING)
PARTITIONED BY (hours(ts))
```

### Connector config
```
{
"name": "events-sink",
"config": {
    "connector.class": "io.tabular.iceberg.connect.IcebergSinkConnector",
    "tasks.max": "2",
    "topics": "events",
    "iceberg.tables": "default.events",
    "iceberg.catalog": "org.apache.iceberg.rest.RESTCatalog",
    "iceberg.catalog.uri": ...
    }
}
```

## Multiple destination tables, static routing
This example will write records with `type` set to `LIST` to the table `default.events_list`, and
will write records with `type` set to `CREATE` to the table `default.events_create`. Other records
will be skipped.

### Create two destination tables
```sql
CREATE TABLE default.events_list (
    id STRING,
    type STRING,
    ts TIMESTAMP,
    payload STRING)
PARTITIONED BY (hours(ts));

CREATE TABLE default.events_create (
    id STRING,
    type STRING,
    ts TIMESTAMP,
    payload STRING)
PARTITIONED BY (hours(ts));
```

### Connector config
```
{
"name": "events-sink",
"config": {
    "connector.class": "io.tabular.iceberg.connect.IcebergSinkConnector",
    "tasks.max": "2",
    "topics": "events",
    "iceberg.tables": "default.events_list,default.events_create",
    "iceberg.tables.routeField": "type",
    "iceberg.table.default.events_list.routeRegex": "list",
    "iceberg.table.default.events_create.routeRegex": "create",
    "iceberg.catalog": "org.apache.iceberg.rest.RESTCatalog",
    "iceberg.catalog.uri": ...
    }
}
```

## Multiple destination tables, dynamic routing
This example will write to tables with names based on the value in the `type` field. If a table with
the name does not exist, then the record will be skipped. For example, if the record's `type` field
is set to `list`, then the record will be written to the `default.events_list` table.

### Create two destination tables
See above for creating two tables.

### Connector config
```
{
"name": "events-sink",
"config": {
    "connector.class": "io.tabular.iceberg.connect.IcebergSinkConnector",
    "tasks.max": "2",
    "topics": "events",
    "iceberg.tables.dynamic.namePrefix": "default.events_",
    "iceberg.tables.routeField": "type",
    "iceberg.catalog": "org.apache.iceberg.rest.RESTCatalog",
    "iceberg.catalog.uri": ...
    }
}
```

## Change data capture
This example will apply inserts, updates, and deletes based on the value of a field in the record.
For example, if the `_cdc_op` field is set to `I` then the record will be inserted, if `U` then it will
be upserted, and if `D` then it will be deleted. This requires that the table be in Iceberg v2 format
and have an identity field (or fields) defined.

### Create the destination table
See above for creating the table

### Connector config
```
{
"name": "events-sink",
"config": {
    "connector.class": "io.tabular.iceberg.connect.IcebergSinkConnector",
    "tasks.max": "2",
    "topics": "events",
    "iceberg.tables": "default.events",
    "iceberg.tables.cdcField": "_cdc_op",
    "iceberg.catalog": "org.apache.iceberg.rest.RESTCatalog",
    "iceberg.catalog.uri": ...
    }
}
```
