/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.tabular.iceberg.connect.data;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT;

import io.tabular.iceberg.connect.IcebergSinkConfig;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.common.DynClasses;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.common.DynMethods.BoundMethod;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.UnpartitionedWriter;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.relocated.com.google.common.primitives.Ints;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utilities {

  private static final Logger LOG = LoggerFactory.getLogger(Utilities.class.getName());

  public static Catalog loadCatalog(IcebergSinkConfig config) {
    return CatalogUtil.buildIcebergCatalog(
        config.getCatalogName(),
        config.getCatalogProps(),
        getHadoopConfig(config.getHadoopProps()));
  }

  private static Object getHadoopConfig(Map<String, String> hadoopProps) {
    Class<?> configClass =
        DynClasses.builder().impl("org.apache.hadoop.hdfs.HdfsConfiguration").orNull().build();
    if (configClass == null) {
      configClass =
          DynClasses.builder().impl("org.apache.hadoop.conf.Configuration").orNull().build();
    }

    if (configClass == null) {
      LOG.info("Hadoop not found on classpath, not creating Hadoop config");
      return null;
    }

    try {
      Object result = configClass.getDeclaredConstructor().newInstance();
      BoundMethod setMethod =
          DynMethods.builder("set").impl(configClass, String.class, String.class).build(result);
      hadoopProps.forEach(setMethod::invoke);
      LOG.info("Hadoop config initialized: {}", configClass.getName());
      return result;
    } catch (InstantiationException
        | IllegalAccessException
        | NoSuchMethodException
        | InvocationTargetException e) {
      LOG.warn(
          "Hadoop found on classpath but could not create config, proceeding without config", e);
    }
    return null;
  }

  public static Object extractFromRecordValue(Object recordValue, String fieldName) {
    if (recordValue instanceof Struct) {
      return ((Struct) recordValue).get(fieldName);
    } else if (recordValue instanceof Map) {
      return ((Map<?, ?>) recordValue).get(fieldName);
    } else {
      throw new UnsupportedOperationException(
          "Cannot extract value from type: " + recordValue.getClass().getName());
    }
  }

  public static TaskWriter<Record> createTableWriter(Table table, IcebergSinkConfig config) {
    String formatStr =
        table.properties().getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT);
    FileFormat format = FileFormat.valueOf(formatStr.toUpperCase());

    long targetFileSize =
        PropertyUtil.propertyAsLong(
            table.properties(), WRITE_TARGET_FILE_SIZE_BYTES, WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);

    Set<Integer> equalityFieldIds = table.schema().identifierFieldIds();

    FileAppenderFactory<Record> appenderFactory;
    if (equalityFieldIds == null || equalityFieldIds.isEmpty()) {
      appenderFactory =
          new GenericAppenderFactory(table.schema(), table.spec(), null, null, null)
              .setAll(table.properties());
    } else {
      appenderFactory =
          new GenericAppenderFactory(
                  table.schema(),
                  table.spec(),
                  Ints.toArray(equalityFieldIds),
                  TypeUtil.select(table.schema(), Sets.newHashSet(equalityFieldIds)),
                  null)
              .setAll(table.properties());
    }

    // (partition ID + task ID + operation ID) must be unique
    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, 1, System.currentTimeMillis())
            .defaultSpec(table.spec())
            .operationId(UUID.randomUUID().toString())
            .format(format)
            .build();

    TaskWriter<Record> writer;
    if (table.spec().isUnpartitioned()) {
      if (config.getTablesCdcField() == null && !config.isUpsertMode()) {
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
                table.schema(),
                config.isUpsertMode());
      }
    } else {
      if (config.getTablesCdcField() == null && !config.isUpsertMode()) {
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
                table.schema(),
                config.isUpsertMode());
      }
    }
    return writer;
  }
}
