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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types.BinaryType;
import org.apache.iceberg.types.Types.BooleanType;
import org.apache.iceberg.types.Types.DoubleType;
import org.apache.iceberg.types.Types.FloatType;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.MapType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;
import org.apache.kafka.connect.data.Schema;

public class SchemaUtils {

  public static class AddColumn {
    private final String parentName;
    private final String name;
    private final Type type;

    public AddColumn(String parentName, String name, Type type) {
      this.parentName = parentName;
      this.name = name;
      this.type = type;
    }

    public String parentName() {
      return parentName;
    }

    public String name() {
      return name;
    }

    public Type type() {
      return type;
    }
  }

  public static Type toIcebergType(Schema valueSchema) {
    switch (valueSchema.type()) {
      case BOOLEAN:
        return BooleanType.get();
      case BYTES:
        return BinaryType.get();
      case INT8:
      case INT16:
      case INT32:
        return IntegerType.get();
      case INT64:
        return LongType.get();
      case FLOAT32:
        return FloatType.get();
      case FLOAT64:
        return DoubleType.get();
      case ARRAY:
        Type elementType = toIcebergType(valueSchema.valueSchema());
        return ListType.ofOptional(-1, elementType);
      case MAP:
        Type keyType = toIcebergType(valueSchema.keySchema());
        Type valueType = toIcebergType(valueSchema.valueSchema());
        return MapType.ofOptional(-1, -1, keyType, valueType);
      case STRUCT:
        List<NestedField> structFields =
            valueSchema.fields().stream()
                .map(field -> NestedField.optional(-1, field.name(), toIcebergType(field.schema())))
                .collect(Collectors.toList());
        return StructType.of(structFields);
      case STRING:
      default:
        return StringType.get();
    }
  }

  public static Type inferIcebergType(Object value) {
    if (value == null) {
      return StringType.get();
    } else if (value instanceof Number) {
      Number num = (Number) value;
      Double d = num.doubleValue();
      if (d.equals(Math.floor(d))) {
        return LongType.get();
      } else {
        return DoubleType.get();
      }
    } else if (value instanceof String) {
      return StringType.get();
    } else if (value instanceof Boolean) {
      return BooleanType.get();
    } else if (value.getClass().isArray()) {
      Object[] array = (Object[]) value;
      if (array.length > 0) {
        Type elementType = inferIcebergType(array[0]);
        return ListType.ofOptional(-1, elementType);
      } else {
        return ListType.ofOptional(-1, StringType.get());
      }
    } else if (value instanceof Map) {
      Map<?, ?> map = (Map<?, ?>) value;
      List<NestedField> structFields =
          map.entrySet().stream()
              .map(
                  entry ->
                      NestedField.optional(
                          -1, entry.getKey().toString(), inferIcebergType(entry.getValue())))
              .collect(Collectors.toList());
      return StructType.of(structFields);
    } else {
      return StringType.get();
    }
  }
}
