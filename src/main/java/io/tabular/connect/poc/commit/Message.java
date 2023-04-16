// Copyright 2023 Tabular Technologies Inc.
package io.tabular.connect.poc.commit;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import lombok.Builder;
import lombok.Getter;
import org.apache.iceberg.DataFile;
import org.apache.kafka.common.TopicPartition;

@Builder
@Getter
public class Message implements Serializable {
  public enum Type {
    BEGIN_COMMIT,
    DATA_FILES
  }

  private Type type;
  private List<DataFile> dataFiles;
  private Map<TopicPartition, Long> offsets;
}
