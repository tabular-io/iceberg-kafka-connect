// Copyright 2023 Tabular Technologies Inc.
package io.tabular.connect.poc;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;

public class IcebergSinkConnectorTask extends SinkTask {

  private Catalog catalog;
  private String tableName;

  private static final String TABLE_PROP = "iceberg.table";

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
    this.catalog = Util.loadCatalog(props);
    this.tableName = props.get(TABLE_PROP);
  }

  @Override
  @SneakyThrows
  public void put(Collection<SinkRecord> sinkRecords) {
    Table table = catalog.loadTable(TableIdentifier.parse(tableName));
    OutputFile outputFile =
        table.io().newOutputFile(table.location() + "/" + UUID.randomUUID() + ".parquet");

    try (FileAppender<GenericRecord> appender =
        Parquet.write(outputFile)
            .forTable(table)
            .createWriterFunc(GenericParquetWriter::buildWriter)
            .build()) {

      List<GenericRecord> records =
          sinkRecords.stream()
              .map(
                  record -> {
                    GenericRecord rec = GenericRecord.create(table.schema());
                    rec.setField("id", ThreadLocalRandom.current().nextLong());
                    rec.setField("data", record.value().toString());
                    rec.setField("ts", LocalDateTime.now());
                    return rec;
                  })
              .collect(Collectors.toList());

      appender.addAll(records);
    }

    DataFile dataFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath(outputFile.location())
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(1)
            .withRecordCount(1)
            .build();

    table.newAppend().appendFile(dataFile).commit();
  }

  @Override
  public void stop() {}
}
