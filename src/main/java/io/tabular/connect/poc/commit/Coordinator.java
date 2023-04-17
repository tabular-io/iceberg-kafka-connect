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
import org.apache.kafka.common.TopicPartition;

@Log4j
public class Coordinator extends Channel {

  private static final String COMMIT_INTERVAL_MS_PROP = "iceberg.table.commitIntervalMs";
  private static final int COMMIT_INTERVAL_MS_DEFAULT = 60_000;

  private final Table table;
  private final List<Message> commitBuffer;
  private final int commitIntervalMs;
  private final AdminClient adminClient;
  private long startTime;
  private boolean commitInProgress;

  public Coordinator(Catalog catalog, TableIdentifier tableIdentifier, Map<String, String> props) {
    super(props);
    this.table = catalog.loadTable(tableIdentifier);
    this.commitBuffer = new ArrayList<>();
    this.commitIntervalMs =
        PropertyUtil.propertyAsInt(props, COMMIT_INTERVAL_MS_PROP, COMMIT_INTERVAL_MS_DEFAULT);

    Map<String, Object> adminCliProps = new HashMap<>(kafkaProps);
    this.adminClient = AdminClient.create(adminCliProps);
  }

  public void process() {
    // send out begin commit
    if (!commitInProgress && System.currentTimeMillis() - startTime >= commitIntervalMs) {
      commitInProgress = true;
      send(Message.builder().type(BEGIN_COMMIT).build());
      startTime = System.currentTimeMillis();
    }

    super.process();
  }

  @Override
  protected void receive(Message message) {
    if (message.getType() == DATA_FILES) {
      if (!commitInProgress) {
        throw new IllegalStateException("Received data files when no commit in progress");
      }
      commitBuffer.add(message);
      commitIfComplete();
    }
  }

  private Map<String, Integer> getNumPartitions(Collection<String> topicNames) {
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

  private void commitIfComplete() {
    Map<String, List<TopicPartition>> pending =
        commitBuffer.stream()
            .flatMap(message -> message.getOffsets().keySet().stream())
            .collect(groupingBy(TopicPartition::topic));
    // TODO: avoid getting number of partitions so often
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
      commitInProgress = false;
      return;
    }

    table.refresh();
    AppendFiles appendOp = table.newAppend();
    dataFiles.forEach(appendOp::appendFile);
    appendOp.commit();
    commitBuffer.clear();
    commitInProgress = false;

    log.info("Commit complete");

    // TODO: handle offset commit, send offset commit message for workers?
  }

  @Override
  public void stop() {
    super.stop();
    adminClient.close();
  }
}
