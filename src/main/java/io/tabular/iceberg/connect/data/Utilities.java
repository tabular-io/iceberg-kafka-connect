// Copyright 2023 Tabular Technologies Inc.
package io.tabular.iceberg.connect.data;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT;

import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.UnpartitionedWriter;
import org.apache.iceberg.util.PropertyUtil;

public class Utilities {

  private static final String CATALOG_PROP = "iceberg.catalog";
  private static final String CATALOG_PROP_PREFIX = "iceberg.catalog.";

  public static Catalog loadCatalog(Map<String, String> props) {
    String catalogImpl = props.get(CATALOG_PROP);
    Map<String, String> catalogProps =
        PropertyUtil.propertiesWithPrefix(props, CATALOG_PROP_PREFIX);
    return CatalogUtil.loadCatalog(catalogImpl, "iceberg", catalogProps, new Configuration());
  }

  public static TaskWriter<Record> createTableWriter(Table table) {
    String formatStr =
        table.properties().getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT);
    FileFormat format = FileFormat.valueOf(formatStr.toUpperCase());

    long targetFileSize =
        PropertyUtil.propertyAsLong(
            table.properties(), WRITE_TARGET_FILE_SIZE_BYTES, WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);

    FileAppenderFactory<Record> appenderFactory =
        new GenericAppenderFactory(table.schema(), table.spec()).setAll(table.properties());

    // (partition ID + task ID + operation ID) must be unique
    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, 1, System.currentTimeMillis())
            .defaultSpec(table.spec())
            .operationId(UUID.randomUUID().toString())
            .format(format)
            .build();

    TaskWriter<Record> writer;
    if (table.spec().isUnpartitioned()) {
      writer =
          new UnpartitionedWriter<>(
              table.spec(), format, appenderFactory, fileFactory, table.io(), targetFileSize);
    } else {
      writer =
          new PartitionedFanoutRecordWriter(
              table.spec(),
              format,
              appenderFactory,
              fileFactory,
              table.io(),
              targetFileSize,
              table.schema());
    }
    return writer;
  }
}
