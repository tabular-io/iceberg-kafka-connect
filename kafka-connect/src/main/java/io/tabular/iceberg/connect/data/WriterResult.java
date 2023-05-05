// Copyright 2023 Tabular Technologies Inc.
package io.tabular.iceberg.connect.data;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types.StructType;
import org.apache.kafka.common.TopicPartition;

public class WriterResult {

  private final TableIdentifier tableIdentifier;
  private final List<DataFile> dataFiles;
  private final List<DeleteFile> deleteFiles;
  private final StructType partitionStruct;
  private final Map<TopicPartition, Long> offsets;

  public WriterResult(
      TableIdentifier tableIdentifier,
      List<DataFile> dataFiles,
      List<DeleteFile> deleteFiles,
      StructType partitionStruct,
      Map<TopicPartition, Long> offsets) {
    this.tableIdentifier = tableIdentifier;
    this.dataFiles = dataFiles;
    this.deleteFiles = deleteFiles;
    this.partitionStruct = partitionStruct;
    this.offsets = offsets;
  }

  public TableIdentifier getTableIdentifier() {
    return tableIdentifier;
  }

  public List<DataFile> getDataFiles() {
    return dataFiles;
  }

  public List<DeleteFile> getDeleteFiles() {
    return deleteFiles;
  }

  public StructType getPartitionStruct() {
    return partitionStruct;
  }

  public Map<TopicPartition, Long> getOffsets() {
    return offsets;
  }
}
