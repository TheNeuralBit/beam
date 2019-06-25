/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.core.construction;

import static org.apache.beam.vendor.grpc.v1p21p0.com.google.common.collect.ImmutableMap.toImmutableMap;

import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import org.apache.beam.model.pipeline.v1.SchemaApi;
import org.apache.beam.sdk.schemas.LogicalTypes;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.LogicalType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.vendor.grpc.v1p21p0.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;

/** Utility methods for translating schemas. */
public class SchemaTranslation {

  private static final String URN_BEAM_LOGICAL_DATETIME = "beam:fieldtype:datetime";
  private static final String URN_BEAM_LOGICAL_DECIMAL = "beam:fieldtype:decimal";

  public static SchemaApi.Schema toProto(Schema schema) {
    String uuid = schema.getUUID() != null ? schema.getUUID().toString() : "";
    SchemaApi.Schema.Builder builder = SchemaApi.Schema.newBuilder().setId(uuid);
    for (Field field : schema.getFields()) {
      SchemaApi.Field protoField =
          fieldTypeToProto(
              field,
              schema.indexOf(field.getName()),
              schema.getEncodingPositions().get(field.getName()));
      builder.addFields(protoField);
    }
    return builder.build();
  }

  private static SchemaApi.Field fieldTypeToProto(Field field, int fieldId, int position) {
    return SchemaApi.Field.newBuilder()
        .setName(field.getName())
        .setDescription(field.getDescription())
        .setType(fieldTypeToProto(field.getType()))
        .setId(fieldId)
        .setEncodingPosition(position)
        .build();
  }

  private static SchemaApi.FieldType fieldTypeToProto(FieldType fieldType) {
    SchemaApi.FieldType.Builder builder = SchemaApi.FieldType.newBuilder();
    switch (fieldType.getTypeName()) {
      case ROW:
        builder.setRowType(
            SchemaApi.RowType.newBuilder().setSchema(toProto(fieldType.getRowSchema())));
        break;

      case ARRAY:
        builder.setArrayType(
            SchemaApi.ArrayType.newBuilder()
                .setElementType(fieldTypeToProto(fieldType.getCollectionElementType())));
        break;

      case MAP:
        builder.setMapType(
            SchemaApi.MapType.newBuilder()
                .setKeyType(fieldTypeToProto(fieldType.getMapKeyType()))
                .setValueType(fieldTypeToProto(fieldType.getMapValueType()))
                .build());
        break;

      case LOGICAL_TYPE:
        builder.setLogicalType(LogicalTypeTranslation.toProto(fieldType.getLogicalType()));
        break;
        // Special-case for DATETIME and DECIMAL which are logical types in portable representation,
        // but not yet in Java. (BEAM-7554)
      case DATETIME:
        builder.setLogicalType(
            SchemaApi.LogicalType.newBuilder()
                .setUrn(URN_BEAM_LOGICAL_DATETIME)
                .setRepresentation(fieldTypeToProto(FieldType.INT64))
                .build());
        break;
      case DECIMAL:
        builder.setLogicalType(
            SchemaApi.LogicalType.newBuilder()
                .setUrn(URN_BEAM_LOGICAL_DECIMAL)
                .setRepresentation(fieldTypeToProto(FieldType.BYTES))
                .build());
        break;
      case BYTE:
        builder.setAtomicType(SchemaApi.AtomicType.BYTE);
        break;
      case INT16:
        builder.setAtomicType(SchemaApi.AtomicType.INT16);
        break;
      case INT32:
        builder.setAtomicType(SchemaApi.AtomicType.INT32);
        break;
      case INT64:
        builder.setAtomicType(SchemaApi.AtomicType.INT64);
        break;
      case FLOAT:
        builder.setAtomicType(SchemaApi.AtomicType.FLOAT);
        break;
      case DOUBLE:
        builder.setAtomicType(SchemaApi.AtomicType.DOUBLE);
        break;
      case STRING:
        builder.setAtomicType(SchemaApi.AtomicType.STRING);
        break;
      case BOOLEAN:
        builder.setAtomicType(SchemaApi.AtomicType.BOOLEAN);
        break;
      case BYTES:
        builder.setAtomicType(SchemaApi.AtomicType.BYTES);
        break;
    }
    builder.setNullable(fieldType.getNullable());
    return builder.build();
  }

  public static Schema fromProto(SchemaApi.Schema protoSchema) {
    Schema.Builder builder = Schema.builder();
    Map<String, Integer> encodingLocationMap = Maps.newHashMap();
    for (SchemaApi.Field protoField : protoSchema.getFieldsList()) {
      Field field = fieldFromProto(protoField);
      builder.addField(field);
      encodingLocationMap.put(protoField.getName(), protoField.getEncodingPosition());
    }
    Schema schema = builder.build();
    schema.setEncodingPositions(encodingLocationMap);
    if (!protoSchema.getId().isEmpty()) {
      schema.setUUID(UUID.fromString(protoSchema.getId()));
    }

    return schema;
  }

  private static Field fieldFromProto(SchemaApi.Field protoField) {
    return Field.of(protoField.getName(), fieldTypeFromProto(protoField.getType()))
        .withDescription(protoField.getDescription());
  }

  private static FieldType fieldTypeFromProto(SchemaApi.FieldType protoFieldType) {
    FieldType fieldType = fieldTypeFromProtoWithoutNullable(protoFieldType);

    if (protoFieldType.getNullable()) {
      fieldType = fieldType.withNullable(true);
    }

    return fieldType;
  }

  private static FieldType fieldTypeFromProtoWithoutNullable(SchemaApi.FieldType protoFieldType) {
    switch (protoFieldType.getTypeInfoCase()) {
      case ATOMIC_TYPE:
        switch (protoFieldType.getAtomicType()) {
          case BYTE:
            return FieldType.of(TypeName.BYTE);
          case INT16:
            return FieldType.of(TypeName.INT16);
          case INT32:
            return FieldType.of(TypeName.INT32);
          case INT64:
            return FieldType.of(TypeName.INT64);
          case FLOAT:
            return FieldType.of(TypeName.FLOAT);
          case DOUBLE:
            return FieldType.of(TypeName.DOUBLE);
          case STRING:
            return FieldType.of(TypeName.STRING);
          case BOOLEAN:
            return FieldType.of(TypeName.BOOLEAN);
          case BYTES:
            return FieldType.of(TypeName.BYTES);
          case UNSPECIFIED:
            throw new IllegalArgumentException("Encountered UNSPECIFIED AtomicType");
          default:
            throw new IllegalArgumentException(
                "Encountered unknown AtomicType: " + protoFieldType.getAtomicType());
        }
      case ROW_TYPE:
        return FieldType.row(fromProto(protoFieldType.getRowType().getSchema()));
      case ARRAY_TYPE:
        return FieldType.array(fieldTypeFromProto(protoFieldType.getArrayType().getElementType()));
      case MAP_TYPE:
        return FieldType.map(
            fieldTypeFromProto(protoFieldType.getMapType().getKeyType()),
            fieldTypeFromProto(protoFieldType.getMapType().getValueType()));
      case LOGICAL_TYPE:
        // Special-case for DATETIME and DECIMAL which are logical types in portable representation,
        // but not yet in Java. (BEAM-7554)
        String urn = protoFieldType.getLogicalType().getUrn();
        if (urn.equals(URN_BEAM_LOGICAL_DATETIME)) {
          return FieldType.DATETIME;
        } else if (urn.equals(URN_BEAM_LOGICAL_DECIMAL)) {
          return FieldType.DECIMAL;
        } else {
          return FieldType.logicalType(
              LogicalTypeTranslation.fromProto(protoFieldType.getLogicalType()));
        }
      default:
        throw new IllegalArgumentException(
            "Unexpected type_info: " + protoFieldType.getTypeInfoCase());
    }
  }

  private static class LogicalTypeTranslation {
    private static class LogicalTypeTranslator<T extends LogicalType> {
      private final String urn;
      private final Class<T> clazz;
      private final Function<String, T> typeFromArgs;

      public LogicalTypeTranslator(String urn, Class<T> clazz, Function<String, T> typeFromArgs) {
        this.urn = urn;
        this.clazz = clazz;
        this.typeFromArgs = typeFromArgs;
      }

      public static <T extends LogicalType> LogicalTypeTranslator<T> of(
          String urn, Class<T> clazz, Function<String, T> typeFromArgs) {
        return new LogicalTypeTranslator<>(urn, clazz, typeFromArgs);
      }

      public String getUrn() {
        return urn;
      }

      public Class<T> getClazz() {
        return clazz;
      }

      public Function<String, T> getTypeFromArgs() {
        return typeFromArgs;
      }
    }

    private static final ImmutableList<LogicalTypeTranslator<?>> LOGICAL_TYPE_TRANSLATORS =
        ImmutableList.of(
            LogicalTypeTranslator.of(
                "beam:fieldtype:fixedbytes",
                LogicalTypes.FixedBytes.class,
                (String args) -> LogicalTypes.FixedBytes.of(Integer.parseInt(args))),
            LogicalTypeTranslator.of(
                "beam:fieldtype:millis-instant",
                LogicalTypes.MillisInstant.class,
                (String args) -> LogicalTypes.MillisInstant.of()));

    private static final ImmutableMap<Class<? extends LogicalType>, String> URN_BY_CLASS =
        LOGICAL_TYPE_TRANSLATORS.stream()
            .collect(
                toImmutableMap(LogicalTypeTranslator::getClazz, LogicalTypeTranslator::getUrn));

    private static final ImmutableMap<String, Function<String, ? extends LogicalType>>
        BUILDER_BY_URN =
            LOGICAL_TYPE_TRANSLATORS.stream()
                .collect(
                    toImmutableMap(
                        LogicalTypeTranslator::getUrn, LogicalTypeTranslator::getTypeFromArgs));

    public static SchemaApi.LogicalType toProto(LogicalType logicalTypeInstance) {
      String urn = URN_BY_CLASS.get(logicalTypeInstance.getClass());
      if (urn == null) {
        throw new IllegalArgumentException(
            "Cannot encode unknown LogicalType class, "
                + logicalTypeInstance.getClass()
                + ", to proto.");
      }
      String args = logicalTypeInstance.getArgument();
      return SchemaApi.LogicalType.newBuilder()
          .setArgs(args)
          .setUrn(urn)
          .setRepresentation(fieldTypeToProto(logicalTypeInstance.getBaseType()))
          .build();
    }

    public static LogicalType fromProto(SchemaApi.LogicalType logicalTypeProto) {
      String urn = logicalTypeProto.getUrn();
      String args = logicalTypeProto.getArgs();
      Function<String, ? extends LogicalType> builder = BUILDER_BY_URN.get(urn);
      if (builder == null) {
        throw new IllegalArgumentException("Encountered unsupported URN: \"" + urn + "\"");
      }
      return builder.apply(args);
    }
  }
}
