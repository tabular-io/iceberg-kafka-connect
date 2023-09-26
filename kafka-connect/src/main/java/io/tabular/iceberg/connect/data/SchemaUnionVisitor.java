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
import java.util.function.Consumer;
import java.util.stream.IntStream;
import org.apache.iceberg.Schema;
import org.apache.iceberg.schema.SchemaWithPartnerVisitor;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public class SchemaUnionVisitor extends SchemaWithPartnerVisitor<Integer, Boolean> {

  private final Consumer<AddColumn> addColumnConsumer;
  private final Schema partnerSchema;

  public static void visit(
      Schema existingSchema, Schema newSchema, Consumer<AddColumn> addColumnConsumer) {
    visit(
        newSchema,
        -1,
        new SchemaUnionVisitor(existingSchema, addColumnConsumer),
        new PartnerIdByNameAccessors(existingSchema));
  }

  private SchemaUnionVisitor(Schema partnerSchema, Consumer<AddColumn> addColumnConsumer) {
    this.partnerSchema = partnerSchema;
    this.addColumnConsumer = addColumnConsumer;
  }

  @Override
  public Boolean struct(
      Types.StructType struct, Integer partnerId, List<Boolean> missingPositions) {
    if (partnerId == null) {
      return true;
    }

    List<Types.NestedField> fields = struct.fields();
    IntStream.range(0, missingPositions.size())
        .forEach(
            pos -> {
              Boolean isMissing = missingPositions.get(pos);
              Types.NestedField field = fields.get(pos);
              if (isMissing) {
                addColumn(partnerId, field);
              }
            });

    return false;
  }

  @Override
  public Boolean field(Types.NestedField field, Integer partnerId, Boolean isFieldMissing) {
    return partnerId == null;
  }

  @Override
  public Boolean list(Types.ListType list, Integer partnerId, Boolean isElementMissing) {
    return partnerId == null;
  }

  @Override
  public Boolean map(
      Types.MapType map, Integer partnerId, Boolean isKeyMissing, Boolean isValueMissing) {
    return partnerId == null;
  }

  @Override
  public Boolean primitive(Type.PrimitiveType primitive, Integer partnerId) {
    return partnerId == null;
  }

  private void addColumn(int parentId, Types.NestedField field) {
    String parentName = partnerSchema.findColumnName(parentId);
    addColumnConsumer.accept(new AddColumn(parentName, field.name(), field.type(), field.doc()));
  }

  public static class AddColumn {
    private final String parentName;
    private final String name;
    private final Type type;
    private final String doc;

    public AddColumn(String parentName, String name, Type type, String doc) {
      this.parentName = parentName;
      this.name = name;
      this.type = type;
      this.doc = doc;
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

    public String doc() {
      return doc;
    }
  }

  private static class PartnerIdByNameAccessors implements PartnerAccessors<Integer> {
    private final Schema partnerSchema;

    private PartnerIdByNameAccessors(Schema partnerSchema) {
      this.partnerSchema = partnerSchema;
    }

    @Override
    public Integer fieldPartner(Integer partnerFieldId, int fieldId, String name) {
      Types.StructType struct;
      if (partnerFieldId == -1) {
        struct = partnerSchema.asStruct();
      } else {
        struct = partnerSchema.findField(partnerFieldId).type().asStructType();
      }

      Types.NestedField field = struct.field(name);
      if (field != null) {
        return field.fieldId();
      }

      return null;
    }

    @Override
    public Integer mapKeyPartner(Integer partnerMapId) {
      Types.NestedField mapField = partnerSchema.findField(partnerMapId);
      if (mapField != null) {
        return mapField.type().asMapType().fields().get(0).fieldId();
      }

      return null;
    }

    @Override
    public Integer mapValuePartner(Integer partnerMapId) {
      Types.NestedField mapField = partnerSchema.findField(partnerMapId);
      if (mapField != null) {
        return mapField.type().asMapType().fields().get(1).fieldId();
      }

      return null;
    }

    @Override
    public Integer listElementPartner(Integer partnerListId) {
      Types.NestedField listField = partnerSchema.findField(partnerListId);
      if (listField != null) {
        return listField.type().asListType().fields().get(0).fieldId();
      }

      return null;
    }
  }
}
