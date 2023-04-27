// Copyright 2023 Tabular Technologies Inc.
package io.tabular.iceberg.connect.commit;

import static java.util.stream.Collectors.toSet;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.Serializable;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import lombok.Getter;
import lombok.Setter;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificData.SchemaConstructable;
import org.apache.avro.util.Utf8;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;
import org.apache.kafka.common.TopicPartition;

@Getter
@Setter
public class Message implements StructLike, IndexedRecord, SchemaConstructable, Serializable {

  public enum Type {
    BEGIN_COMMIT,
    DATA_FILES
  }

  private UUID commitId;
  private Type type;
  private List<DataFile> dataFiles;
  private Set<TopicPartition> assignments;
  private Schema avroSchema;

  public Message(Schema avroSchema) {
    this.avroSchema = avroSchema;
  }

  public Message(StructType partitionType) {
    StructType dataFileType = DataFile.getType(partitionType);
    StructType messageType =
        StructType.of(
            required(1, "commit_id", StringType.get()),
            required(2, "type", StringType.get()),
            optional(3, "data_files", ListType.ofRequired(4, dataFileType)),
            optional(5, "assignments", ListType.ofRequired(6, StringType.get())));

    this.avroSchema =
        AvroSchemaUtil.convert(
            messageType,
            ImmutableMap.of(
                messageType, Message.class.getName(),
                dataFileType, "org.apache.iceberg.GenericDataFile",
                partitionType, PartitionData.class.getName()));
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
        this.type = v == null ? null : Type.valueOf(((Utf8) v).toString());
        return;
      case 2:
        this.dataFiles = (List<DataFile>) v;
        return;
      case 3:
        this.assignments =
            v == null
                ? null
                : ((List<Utf8>) v)
                    .stream()
                        .map(
                            s -> {
                              String[] parts = s.toString().split(":", 2);
                              return new TopicPartition(parts[0], Integer.parseInt(parts[1]));
                            })
                        .collect(toSet());
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
        return type == null ? null : type.name();
      case 2:
        return dataFiles;
      case 3:
        return assignments == null
            ? null
            : assignments.stream().map(tp -> tp.topic() + ":" + tp.partition()).collect(toSet());
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
