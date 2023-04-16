// Copyright 2023 Tabular Technologies Inc.
package io.tabular.connect.poc.commit;

import static io.tabular.connect.poc.commit.Message.Type.BEGIN_COMMIT;
import static io.tabular.connect.poc.commit.Message.Type.DATA_FILES;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import lombok.extern.log4j.Log4j;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.TopicPartition;

@Log4j
public class Coordinator extends Channel {

  private static final String COMMIT_INTERVAL_MS_PROP = "iceberg.table.commitIntervalMs";
  private static final int COMMIT_INTERVAL_MS_DEFAULT = 60_000;

  private final String bootstrapServers;
  private final Catalog catalog;
  private final TableIdentifier tableIdentifier;
  private final List<Message> commitBuffer;
  private final int commitIntervalMs;
  private long startTime;

  public Coordinator(Catalog catalog, TableIdentifier tableIdentifier, Map<String, String> props) {
    super(props);
    this.bootstrapServers = props.get("bootstrap.servers");
    this.catalog = catalog;
    this.tableIdentifier = tableIdentifier;
    this.commitBuffer = new ArrayList<>();
    this.commitIntervalMs =
        PropertyUtil.propertyAsInt(props, COMMIT_INTERVAL_MS_PROP, COMMIT_INTERVAL_MS_DEFAULT);
  }

  public void process() {
    super.process();

    // send out begin commit
    if (System.currentTimeMillis() - startTime >= commitIntervalMs) {
      send(Message.builder().type(BEGIN_COMMIT).build());
      startTime = System.currentTimeMillis();
    }
  }

  @Override
  protected void receive(Message message) {
    if (message.getType() == DATA_FILES) {
      commitBuffer.add(message);
      commitIfComplete();
    }
  }

  private Map<String, Integer> getNumPartitions(Collection<String> topicNames) {
    try (AdminClient adminClient =
        AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
      Map<String, Integer> result = new HashMap<>();
      adminClient
          .describeTopics(topicNames)
          .topicNameValues()
          .forEach(
              (key, value) -> {
                try {
                  result.put(key, value.get().partitions().size());
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              });
      return result;
    }
  }

  private void commitIfComplete() {
    Map<String, List<TopicPartition>> pending =
        commitBuffer.stream()
            .flatMap(message -> message.getOffsets().keySet().stream())
            .collect(groupingBy(TopicPartition::topic));
    Map<String, Integer> numPartitions = getNumPartitions(pending.keySet());
    for (Entry<String, List<TopicPartition>> entry : pending.entrySet()) {
      if (numPartitions.getOrDefault(entry.getKey(), 0) < entry.getValue().size()) {
        log.info("Commit not ready, waiting for more results");
        return;
      }
    }

    log.info("Commit ready");
    List<DataFile> dataFiles =
        commitBuffer.stream()
            .flatMap(message -> message.getDataFiles().stream())
            .filter(dataFile -> dataFile.recordCount() > 0)
            .collect(toList());
    if (dataFiles.isEmpty()) {
      log.info("Nothing to commit");
      return;
    }

    Table table = catalog.loadTable(tableIdentifier);
    AppendFiles appendOp = table.newAppend();
    dataFiles.forEach(appendOp::appendFile);
    appendOp.commit();
    commitBuffer.clear();
    log.info("Commit complete");

    // TODO: offsets
  }
}
