// Copyright 2023 Tabular Technologies Inc.
package io.tabular.connect.poc;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;

public class IcebergSinkConnectorTask extends SinkTask {

  private Path output;

  @Override
  public String version() {
    return "0.0.1";
  }

  @Override
  public void initialize(SinkTaskContext context) {
    super.initialize(context);
  }

  @Override
  public void start(Map<String, String> props) {
    output = Path.of(props.get("output"));
  }

  @Override
  @SneakyThrows
  public void put(Collection<SinkRecord> records) {
    String value =
        records.stream().map(record -> record.value().toString()).collect(Collectors.joining());
    Files.writeString(output, value);
  }

  @Override
  public void stop() {}
}
