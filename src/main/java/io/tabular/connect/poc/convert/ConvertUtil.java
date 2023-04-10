// Copyright 2023 Tabular Technologies Inc.
package io.tabular.connect.poc.convert;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.ZoneId;
import java.util.Map;
import lombok.SneakyThrows;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types.StructType;
import org.apache.kafka.common.protocol.types.Struct;

public class ConvertUtil {

  private static final RecordConverter MAP_RECORD_CONVERTER = new MapRecordConverter();
  private static final RecordConverter STRUCT_RECORD_CONVERTER = new StructRecordConverter();

  public static final ObjectMapper MAPPER = new ObjectMapper();
  public static final ZoneId DEFAULT_TZ = ZoneId.systemDefault();
  public static final long NANOS_PER_MILLI = 1_000_000L;

  @SneakyThrows
  public static Record convert(Object data, StructType tableSchema) {
    if (data instanceof Struct) {
      return STRUCT_RECORD_CONVERTER.convert(data, tableSchema);
    } else if (data instanceof Map) {
      return MAP_RECORD_CONVERTER.convert(data, tableSchema);
    } else if (data instanceof String) {
      Map<?, ?> map = MAPPER.readValue((String) data, Map.class);
      return MAP_RECORD_CONVERTER.convert(map, tableSchema);
    } else if (data instanceof byte[]) {
      Map<?, ?> map = MAPPER.readValue((byte[]) data, Map.class);
      return MAP_RECORD_CONVERTER.convert(map, tableSchema);
    }
    throw new IllegalArgumentException("Cannot convert type: " + data.getClass().getName());
  }
}
