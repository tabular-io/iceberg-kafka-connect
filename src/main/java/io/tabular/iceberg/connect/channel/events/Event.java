// Copyright 2023 Tabular Technologies Inc.
package io.tabular.iceberg.connect.channel.events;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.Serializable;
import java.util.List;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificData.SchemaConstructable;
import org.apache.avro.util.Utf8;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;

public class Event implements StructLike, IndexedRecord, SchemaConstructable, Serializable {

  private UUID commitId;
  private EventType type;
  private List<DataFile> dataFiles;
  private List<TopicAndPartition> assignments;
  private Schema avroSchema;

  public Event(Schema avroSchema) {
    this.avroSchema = avroSchema;
  }

  public Event(StructType partitionType, UUID commitId, EventType type) {
    this(partitionType, commitId, type, null, null);
  }

  public Event(
      StructType partitionType,
      UUID commitId,
      EventType type,
      List<DataFile> dataFiles,
      List<TopicAndPartition> assignments) {
    this.commitId = commitId;
    this.type = type;
    this.dataFiles = dataFiles;
    this.assignments = assignments;

    StructType dataFileStruct = DataFile.getType(partitionType);
    StructType eventStruct =
        StructType.of(
            required(1, "commit_id", StringType.get()),
            required(2, "type", IntegerType.get()),
            optional(3, "data_files", ListType.ofRequired(4, dataFileStruct)),
            optional(5, "assignments", ListType.ofRequired(6, TopicAndPartition.STRUCT_TYPE)));
    this.avroSchema =
        AvroSchemaUtil.convert(
            eventStruct,
            ImmutableMap.of(
                eventStruct,
                Event.class.getName(),
                TopicAndPartition.STRUCT_TYPE,
                TopicAndPartition.class.getName(),
                dataFileStruct,
                "org.apache.iceberg.GenericDataFile",
                partitionType,
                PartitionData.class.getName()));
  }

  public UUID getCommitId() {
    return commitId;
  }

  public EventType getType() {
    return type;
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
        this.commitId = v == null ? null : UUID.fromString(((Utf8) v).toString());
        return;
      case 1:
        this.type = v == null ? null : EventType.values()[(Integer) v];
        return;
      case 2:
        this.dataFiles = (List<DataFile>) v;
        return;
      case 3:
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
        return commitId == null ? null : commitId.toString();
      case 1:
        return type == null ? null : type.getId();
      case 2:
        return dataFiles;
      case 3:
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
    return 4;
  }
}
