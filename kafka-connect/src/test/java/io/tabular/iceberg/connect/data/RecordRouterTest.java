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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import io.tabular.iceberg.connect.IcebergSinkConfig;
import io.tabular.iceberg.connect.exception.DeadLetterUtils;
import io.tabular.iceberg.connect.exception.WriteException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.Pair;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RecordRouterTest {

    static class RecordingWriterManager extends WriterManager {

        private final List<Pair<String, SinkRecord>> log;
        RecordingWriterManager(IcebergWriterFactory factory) {
            super(factory);
            this.log = Lists.newArrayList();
        }

        @Override
        public void write(String tableName, SinkRecord record, boolean ignoreMissingTable) {
            log.add(Pair.of(tableName, record));
        }

    }

    private static final IcebergWriterFactory factory = mock(IcebergWriterFactory.class);
    private static final         SinkTaskContext context = mock(SinkTaskContext.class);


    @Test
    @DisplayName("ConfigRouter should dispatch based on configured tables")
    public void configRouterTest() {
        RecordingWriterManager manager = new RecordingWriterManager(factory);
        IcebergSinkConfig config = mock(IcebergSinkConfig.class);
        SinkRecord record = mock(SinkRecord.class);

        when(config.tables()).thenReturn(Lists.newArrayList("tbl1", "tbl2"));
        when(config.deadLetterTableEnabled()).thenReturn(false);
        when(config.tablesRouteField()).thenReturn(null);
        when(config.dynamicTablesEnabled()).thenReturn(false);

        RecordRouter router = RecordRouter.from(manager, config, this.getClass().getClassLoader(), context);
        // do some assertions here.
        assertThat(router).isInstanceOf(RecordRouter.ConfigRecordRouter.class);
        // test some dispatching here.
        router.write(record);
        List<Pair<String, SinkRecord>> result = manager.log;
        assertThat(result).isEqualTo(Lists.newArrayList(Pair.of("tbl1", record), Pair.of("tbl2", record)));
    }

    @Test
    @DisplayName("Fallback writer should dispatch based on record value and fall back to configured tables otherwise")
    public void fallBackWriterTest() {
        RecordingWriterManager manager = new RecordingWriterManager(factory);
        IcebergSinkConfig config = mock(IcebergSinkConfig.class);

        Schema schemaWithRoute = SchemaBuilder.struct().field("a", Schema.STRING_SCHEMA).field("route_field", Schema.STRING_SCHEMA).build();
        Schema schemaWithoutRoute = SchemaBuilder.struct().field("a", Schema.STRING_SCHEMA);

        Struct structWithRoute = new Struct(schemaWithRoute).put("a", "a").put("route_field", "route_field_table");
        Struct structWithoutRoute = new Struct(schemaWithoutRoute).put("a", "a");

        SinkRecord recordWithRoute = new SinkRecord("topic", 1, null, null, schemaWithRoute, structWithRoute, 100L);
        SinkRecord recordWithoutRoute = new SinkRecord("topic", 1, null, null, schemaWithoutRoute, structWithoutRoute, 101L);

        when(config.tables()).thenReturn(Lists.newArrayList("tbl1", "tbl2"));
        when(config.deadLetterTableEnabled()).thenReturn(false);
        when(config.tablesRouteField()).thenReturn("route_field");
        when(config.dynamicTablesEnabled()).thenReturn(false);

        RecordRouter router = RecordRouter.from(manager, config, this.getClass().getClassLoader(), context);
        assertThat(router).isInstanceOf(RecordRouter.FallbackRecordRouter.class);
        router.write(recordWithRoute);
        router.write(recordWithoutRoute);
        List<Pair<String, SinkRecord>> result = manager.log;
        assertThat(result).isEqualTo(Lists.newArrayList(Pair.of("route_field_table", recordWithRoute), Pair.of("tbl1", recordWithoutRoute), Pair.of("tbl2", recordWithoutRoute)));
    }


    @Test
    @DisplayName("DynamicRecordRouter should dispatch based on the record field")
    public void dynamicRecordRouterTest() {
        RecordingWriterManager manager = new RecordingWriterManager(factory);
        IcebergSinkConfig config = mock(IcebergSinkConfig.class);

        Schema schemaWithRoute = SchemaBuilder.struct().field("a", Schema.STRING_SCHEMA).field("route_field", Schema.STRING_SCHEMA).build();
        Schema schemaWithoutRoute = SchemaBuilder.struct().field("a", Schema.STRING_SCHEMA);

        Struct structWithRoute = new Struct(schemaWithRoute).put("a", "a").put("route_field", "route_field_table");
        Struct structWithoutRoute = new Struct(schemaWithoutRoute).put("a", "a");

        SinkRecord recordWithRoute = new SinkRecord("topic", 1, null, null, schemaWithRoute, structWithRoute, 100L);
        SinkRecord recordWithoutRoute = new SinkRecord("topic", 1, null, null, schemaWithoutRoute, structWithoutRoute, 101L);

        when(config.tables()).thenReturn(Lists.newArrayList());
        when(config.deadLetterTableEnabled()).thenReturn(false);
        when(config.tablesRouteField()).thenReturn("route_field");
        when(config.dynamicTablesEnabled()).thenReturn(true);

        RecordRouter router = RecordRouter.from(manager, config, this.getClass().getClassLoader(), context);
        assertThat(router).isInstanceOf(RecordRouter.DynamicRecordRouter.class);

        router.write(recordWithRoute);
        List<Pair<String, SinkRecord>> result = manager.log;

        assertThat(result).isEqualTo(Lists.newArrayList(Pair.of("route_field_table", recordWithRoute)));
        assertThrows(WriteException.RouteException.class, () -> router.write(recordWithoutRoute));
    }

    @Test
    @DisplayName("RegexRouter should be configured when dynamicTablesEnabled is false and iceberg.tables is null or empty")
    public void regexRouterTest() {
        RecordingWriterManager manager = new RecordingWriterManager(factory);

        IcebergSinkConfig configTablesNull = mock(IcebergSinkConfig.class);
        when(configTablesNull.tables()).thenReturn(null);
        when(configTablesNull.deadLetterTableEnabled()).thenReturn(false);
        when(configTablesNull.tablesRouteField()).thenReturn("route_val");
        when(configTablesNull.dynamicTablesEnabled()).thenReturn(false);

        IcebergSinkConfig configTablesEmpty = mock(IcebergSinkConfig.class);
        when(configTablesEmpty.tables()).thenReturn(Lists.newArrayList());
        when(configTablesEmpty.deadLetterTableEnabled()).thenReturn(false);
        when(configTablesEmpty.tablesRouteField()).thenReturn("route_val");
        when(configTablesEmpty.dynamicTablesEnabled()).thenReturn(false);

        RecordRouter routerNull = RecordRouter.from(manager, configTablesNull, this.getClass().getClassLoader(), context);
        RecordRouter routerEmpty = RecordRouter.from(manager, configTablesEmpty, this.getClass().getClassLoader(), context);

        assertThat(routerNull).isInstanceOf(RecordRouter.RegexRecordRouter.class);
        assertThat(routerEmpty).isInstanceOf(RecordRouter.RegexRecordRouter.class);
    }

    @Test
    @DisplayName("ErrorHandlingRouter should be configured when deadLetterTableEnabled is true")
    public void errorHandlingRouterGetsConfiguredProperly() {
        RecordingWriterManager manager = new RecordingWriterManager(factory);

        Schema schemaWithRoute = SchemaBuilder.struct().field("a", Schema.STRING_SCHEMA).field("route_field", Schema.STRING_SCHEMA).build();
        Schema schemaWithoutRoute = SchemaBuilder.struct().field("a", Schema.STRING_SCHEMA);

        Struct structWithRoute = new Struct(schemaWithRoute).put("a", "a").put("route_field", "route_field_table");
        Struct structWithoutRoute = new Struct(schemaWithoutRoute).put("a", "bad_record_fail");

        SinkRecord recordWithRoute = new SinkRecord("topic", 1, null, null, schemaWithRoute, structWithRoute, 100L);
        SinkRecord recordWithoutRoute = new SinkRecord("topic", 1, null, null, schemaWithoutRoute, structWithoutRoute, 101L);

        // defaultRecordFactory assumes ErrorTransform has been used to put original bytes on the records in the headers
        // since this record will fail, it will go through the configured DefaultFailedRecordFactory and expects these values to be present
        Struct originalRecordStruct = new Struct(DeadLetterUtils.HEADER_STRUCT_SCHEMA);
        originalRecordStruct.put(DeadLetterUtils.VALUE,"test".getBytes(StandardCharsets.UTF_8));
        recordWithoutRoute.headers().add(DeadLetterUtils.ORIGINAL_DATA, new SchemaAndValue(DeadLetterUtils.HEADER_STRUCT_SCHEMA, originalRecordStruct));

        IcebergSinkConfig config = mock(IcebergSinkConfig.class);
        when(config.tables()).thenReturn(Lists.newArrayList("tbl1", "tbl2"));
        when(config.deadLetterTableEnabled()).thenReturn(true);
        when(config.tablesRouteField()).thenReturn("route_field");
        when(config.dynamicTablesEnabled()).thenReturn(true);

        Map<String, String> deadLetterProperties = ImmutableMap.of("failed_record_factory", "io.tabular.iceberg.connect.exception.DefaultFailedRecordFactory","table_name", "dlt.table", "route_field", "route_field");
        when(config.writeExceptionHandlerProperties()).thenReturn(deadLetterProperties);


        when(config.getWriteExceptionHandler()).thenReturn("io.tabular.iceberg.connect.exception.DeadLetterTableWriteExceptionHandler");

        RecordRouter router = RecordRouter.from(manager, config, this.getClass().getClassLoader(), context);
        assertThat(router).isInstanceOf(RecordRouter.ErrorHandlingRecordRouter.class);

        router.write(recordWithRoute);
        router.write(recordWithoutRoute);

        List<Pair<String, SinkRecord>> result = manager.log;
        assertThat(result.stream().map(Pair::first).collect(Collectors.toList())).isEqualTo(Lists.newArrayList("route_field_table", "dlt.table"));
    }

    @Test
    @DisplayName("ErrorHandlingRouter should throw if there is an issue with the failed record conversion")
    public void errorHandlingRouterDoesNotInfiniteLoop() {
        RecordingWriterManager manager = new RecordingWriterManager(factory);

        Schema schemaWithoutRoute = SchemaBuilder.struct().field("a", Schema.STRING_SCHEMA);

        Struct structWithoutRoute = new Struct(schemaWithoutRoute).put("a", "bad_record_fail");

        SinkRecord recordWithoutRoute = new SinkRecord("topic", 1, null, null, schemaWithoutRoute, structWithoutRoute, 101L);

        // defaultRecordFactory assumes ErrorTransform has been used to put original bytes on the records in the headers
        // since this record will fail, it will go through the configured DefaultFailedRecordFactory and expects these values to be present
        Struct originalRecordStruct = new Struct(DeadLetterUtils.HEADER_STRUCT_SCHEMA);
        originalRecordStruct.put(DeadLetterUtils.VALUE,"test".getBytes(StandardCharsets.UTF_8));
        recordWithoutRoute.headers().add(DeadLetterUtils.ORIGINAL_DATA, new SchemaAndValue(DeadLetterUtils.HEADER_STRUCT_SCHEMA, originalRecordStruct));

        IcebergSinkConfig config = mock(IcebergSinkConfig.class);
        when(config.tables()).thenReturn(Lists.newArrayList("tbl1", "tbl2"));
        when(config.deadLetterTableEnabled()).thenReturn(true);
        when(config.tablesRouteField()).thenReturn("route_field");
        when(config.dynamicTablesEnabled()).thenReturn(true);
        Map<String, String> deadLetterProperties = ImmutableMap.of("failed_record_factory", "io.tabular.iceberg.connect.exception.DefaultFailedRecordFactory","table_name", "dlt.table", "route_field", "route_field_bad");
        when(config.writeExceptionHandlerProperties()).thenReturn(deadLetterProperties);
        when(config.getWriteExceptionHandler()).thenReturn("io.tabular.iceberg.connect.exception.DeadLetterTableWriteExceptionHandler");
        // the underlying router is looking for `route_field` but the failed record handler is configured to have
        // the route field on `route_field_bad`
        // this should cause the ErrorHandler to throw an exception
        // since this is a configuration issue, it should kill the connector w/ unhandled exception
        RecordRouter router = RecordRouter.from(manager, config, this.getClass().getClassLoader(), context);
        assertThat(router).isInstanceOf(RecordRouter.ErrorHandlingRecordRouter.class);

        assertThrows(WriteException.RouteException.class, () -> router.write(recordWithoutRoute));
    }
}
