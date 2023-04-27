// Copyright 2023 Tabular Technologies Inc.
package io.tabular.iceberg.connect.data;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.types.Types.StructType;
import org.apache.kafka.common.TopicPartition;

public class WriterResult {

  private final List<DataFile> dataFiles;
  private final StructType partitionStruct;
  private final Map<TopicPartition, Long> offsets;

  public WriterResult(
      List<DataFile> dataFiles, StructType partitionStruct, Map<TopicPartition, Long> offsets) {
    this.dataFiles = dataFiles;
    this.partitionStruct = partitionStruct;
    this.offsets = offsets;
  }

  public List<DataFile> getDataFiles() {
    return dataFiles;
  }

  public StructType getPartitionStruct() {
    return partitionStruct;
  }

  public Map<TopicPartition, Long> getOffsets() {
    return offsets;
  }
}
