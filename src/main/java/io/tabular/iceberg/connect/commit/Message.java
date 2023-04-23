// Copyright 2023 Tabular Technologies Inc.
package io.tabular.iceberg.connect.commit;

import java.io.Serializable;
import java.util.List;
import java.util.Set;
import lombok.Builder;
import lombok.Getter;
import org.apache.iceberg.DataFile;
import org.apache.kafka.common.TopicPartition;

// FIXME!!! support schema evolution (don't use Java/Kryo serialization)
@Builder
@Getter
public class Message implements Serializable {
  public enum Type {
    BEGIN_COMMIT,
    DATA_FILES
  }

  private Type type;
  private List<DataFile> dataFiles;
  private Set<TopicPartition> assignments;
}
