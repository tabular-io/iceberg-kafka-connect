// Copyright 2023 Tabular Technologies Inc.
package io.tabular.connect.poc;

import static java.util.stream.Collectors.toList;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Streams;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.List;
import lombok.SneakyThrows;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;

public class ConvertUtil {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @SneakyThrows
  public static GenericRecord convert(byte[] json, StructType schema) {
    JsonNode jsonNode = MAPPER.readTree(json);
    return convertRecordValue(jsonNode, schema);
  }

  private static Object convertValue(JsonNode value, Type type) {
    Object result;
    switch (type.typeId()) {
      case STRUCT:
        result = convertRecordValue(value, type.asStructType());
        break;
      case LIST:
        result = convertListValue(value, type.asListType());
        break;
      case INTEGER:
        result = value.asInt();
        break;
      case LONG:
        result = value.asLong();
        break;
      case FLOAT:
      case DOUBLE:
        result = value.asDouble();
        break;
      case BOOLEAN:
        result = value.asBoolean();
        break;
      case STRING:
        result = value.asText();
        break;
      case BINARY:
      case FIXED:
        result = convertBase64Value(value.asText());
        break;
      case DATE:
        result = LocalDate.ofEpochDay(value.asInt());
        break;
      case TIME:
        result = LocalTime.ofSecondOfDay(value.asLong());
        break;
      case TIMESTAMP:
        result = LocalDateTime.ofInstant(Instant.ofEpochMilli(value.asLong()), ZoneOffset.UTC);
        break;
      default:
        // TODO: support more types, better date/time handling
        throw new RuntimeException("Unsupported type: " + type.typeId());
    }
    return result;
  }

  private static GenericRecord convertRecordValue(JsonNode node, StructType schema) {
    GenericRecord record = GenericRecord.create(schema);
    Streams.stream(node.fields())
        .forEach(
            child -> {
              NestedField field = schema.field(child.getKey());
              if (field != null) {
                record.setField(field.name(), convertValue(child.getValue(), field.type()));
              } else {
                // TODO: warn or throw exception if missing?
              }
            });
    return record;
  }

  private static List<Object> convertListValue(JsonNode items, ListType type) {
    return Streams.stream(items.elements())
        .map(item -> convertValue(item, type.elementType()))
        .collect(toList());
  }

  private static byte[] convertBase64Value(String string) {
    return Base64.getDecoder().decode(string);
  }
}
