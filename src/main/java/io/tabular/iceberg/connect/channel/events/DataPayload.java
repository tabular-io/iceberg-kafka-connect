// Copyright 2023 Tabular Technologies Inc.
package io.tabular.iceberg.connect.channel.events;

import static org.apache.iceberg.avro.AvroSchemaUtil.FIELD_ID_PROP;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.types.Types.StructType;

public class DataPayload implements Payload {

  private TableName tableName;
  private List<DataFile> dataFiles;
  private List<TopicAndPartition> assignments;
  private Schema avroSchema;

  public DataPayload(Schema avroSchema) {
    this.avroSchema = avroSchema;
  }

  public DataPayload(
      StructType partitionType,
      TableName tableName,
      List<DataFile> dataFiles,
      List<TopicAndPartition> assignments) {
    this.tableName = tableName;
    this.dataFiles = dataFiles;
    this.assignments = assignments;

    StructType dataFileStruct = DataFile.getType(partitionType);
    Schema dataFileSchema =
        AvroSchemaUtil.convert(
            dataFileStruct,
            ImmutableMap.of(
                dataFileStruct, "org.apache.iceberg.GenericDataFile",
                partitionType, "org.apache.iceberg.PartitionData"));

    this.avroSchema =
        SchemaBuilder.builder()
            .nullable()
            .record(getClass().getName())
            .fields()
            .name("tableName")
            .prop(FIELD_ID_PROP, "90")
            .type(TableName.AVRO_SCHEMA)
            .noDefault()
            .name("dataFiles")
            .prop(FIELD_ID_PROP, "91")
            .type()
            .nullable()
            .array()
            .items(dataFileSchema)
            .noDefault()
            .name("assignments")
            .prop(FIELD_ID_PROP, "92")
            .type()
            .nullable()
            .array()
            .items(TopicAndPartition.AVRO_SCHEMA)
            .noDefault()
            .endRecord();
  }

  public TableName getTableName() {
    return tableName;
  }

  public List<DataFile> getDataFiles() {
    return dataFiles;
  }

  public List<TopicAndPartition> getAssignments() {
    return assignments;
  }

  @Override
  public Schema getSchema() {
    return avroSchema;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void put(int i, Object v) {
    switch (i) {
      case 0:
        this.tableName = (TableName) v;
        return;
      case 1:
        this.dataFiles = (List<DataFile>) v;
        return;
      case 2:
        this.assignments = (List<TopicAndPartition>) v;
        return;
      default:
        // ignore the object, it must be from a newer version of the format
    }
  }

  @Override
  public <T> void set(int pos, T value) {
    put(pos, value);
  }

  @Override
  public Object get(int i) {
    switch (i) {
      case 0:
        return tableName;
      case 1:
        return dataFiles;
      case 2:
        return assignments;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + i);
    }
  }

  @Override
  public <T> T get(int pos, Class<T> javaClass) {
    return javaClass.cast(get(pos));
  }

  @Override
  public int size() {
    return 3;
  }
}
