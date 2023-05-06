// Copyright 2023 Tabular Technologies Inc.
package io.tabular.iceberg.connect.data;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT;

import io.tabular.iceberg.connect.IcebergSinkConfig;
import java.lang.reflect.InvocationTargetException;
import java.util.UUID;
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
import org.apache.iceberg.relocated.com.google.common.primitives.Ints;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utilities {

  private static final Logger LOG = LoggerFactory.getLogger(Utilities.class.getName());

  public static Catalog loadCatalog(IcebergSinkConfig config) {
    return CatalogUtil.loadCatalog(
        config.getCatalogImpl(), "iceberg", config.getCatalogProps(), getHadoopConfig());
  }

  private static Object getHadoopConfig() {
    try {
      Class<?> clazz = Class.forName("org.apache.hadoop.conf.Configuration");
      return clazz.getDeclaredConstructor().newInstance();
    } catch (ClassNotFoundException e) {
      LOG.info("Hadoop not found on classpath, not creating Hadoop config");
    } catch (InstantiationException
        | IllegalAccessException
        | NoSuchMethodException
        | InvocationTargetException e) {
      LOG.warn(
          "Hadoop found on classpath but could not create config, proceeding without config", e);
    }
    return null;
  }

  public static TaskWriter<Record> createTableWriter(Table table, IcebergSinkConfig config) {
    String formatStr =
        table.properties().getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT);
    FileFormat format = FileFormat.valueOf(formatStr.toUpperCase());

    long targetFileSize =
        PropertyUtil.propertyAsLong(
            table.properties(), WRITE_TARGET_FILE_SIZE_BYTES, WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);

    FileAppenderFactory<Record> appenderFactory =
        new GenericAppenderFactory(
                table.schema(),
                table.spec(),
                Ints.toArray(table.schema().identifierFieldIds()),
                table.schema(),
                null)
            .setAll(table.properties());

    // (partition ID + task ID + operation ID) must be unique
    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, 1, System.currentTimeMillis())
            .defaultSpec(table.spec())
            .operationId(UUID.randomUUID().toString())
            .format(format)
            .build();

    TaskWriter<Record> writer;
    if (table.spec().isUnpartitioned()) {
      if (config.getTablesCdcField() == null) {
        writer =
            new UnpartitionedWriter<>(
                table.spec(), format, appenderFactory, fileFactory, table.io(), targetFileSize);
      } else {
        writer =
            new UnpartitionedDeltaWriter(
                table.spec(),
                format,
                appenderFactory,
                fileFactory,
                table.io(),
                targetFileSize,
                table.schema());
      }
    } else {
      if (config.getTablesCdcField() == null) {
        writer =
            new PartitionedAppendWriter(
                table.spec(),
                format,
                appenderFactory,
                fileFactory,
                table.io(),
                targetFileSize,
                table.schema());
      } else {
        writer =
            new PartitionedDeltaWriter(
                table.spec(),
                format,
                appenderFactory,
                fileFactory,
                table.io(),
                targetFileSize,
                table.schema());
      }
    }
    return writer;
  }
}
