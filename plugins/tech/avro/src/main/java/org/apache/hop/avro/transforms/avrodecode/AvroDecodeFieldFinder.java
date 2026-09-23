/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.avro.transforms.avrodecode;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.StringUtil;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * Turns pasted JSON into Avro Decode field rows.
 *
 * <p>Accepts a Hop Avro value ({@code schema} and {@code data}), a bare Avro record schema, or one
 * sample record. A sample record only contributes its top-level fields, and a JSON null is typed as
 * String.
 */
final class AvroDecodeFieldFinder {
  private static final String CONST_SCHEMA = "schema";
  private static final String CONST_DATA = "data";
  private static final Set<String> UNION_WRAPPERS =
      Set.of("boolean", "int", "long", "float", "double", "bytes", "string");

  private AvroDecodeFieldFinder() {}

  record FieldRow(
      String sourceField, String sourceAvroType, String targetFieldName, String targetType) {}

  /**
   * @return the record schema described by {@code json}, or {@code null} when {@code json} is blank
   */
  static Schema schemaFromJson(String json) throws HopException {
    if (StringUtils.isBlank(json)) {
      return null;
    }
    String text = json.trim();
    Object parsed;
    try {
      parsed = new JSONParser().parse(text);
    } catch (ParseException e) {
      throw new HopException("Unable to parse Avro JSON", e);
    }
    if (!(parsed instanceof JSONObject object)) {
      throw new HopException("Avro JSON must be an object");
    }
    // The data half is not decoded. Nullable unions in GenericRecord.toString() are not always in
    // the shape JsonDecoder expects, and only the schema is needed to list fields.
    if (object.get(CONST_SCHEMA) instanceof JSONObject schemaObject
        && object.get(CONST_DATA) instanceof JSONObject) {
      try {
        return recordSchema(new Schema.Parser().parse(schemaObject.toJSONString()));
      } catch (RuntimeException e) {
        throw new HopException("Unable to read the Avro schema and data JSON", e);
      }
    }
    try {
      Schema schema = new Schema.Parser().parse(text);
      if (schema.getType() == Schema.Type.RECORD) {
        return schema;
      }
    } catch (RuntimeException e) {
      // Not an Avro schema. Fall through and read it as one sample record.
    }
    return schemaFromSample(object);
  }

  static List<FieldRow> rowsForSchema(Schema schema) throws HopException {
    Schema recordSchema = recordSchema(schema);
    List<Schema.Field> fields = new ArrayList<>(recordSchema.getFields());
    fields.sort(Comparator.comparing(field -> field.name().toLowerCase()));
    List<FieldRow> rows = new ArrayList<>();
    for (Schema.Field field : fields) {
      String typeDesc = StringUtil.initCap(field.schema().getType().name().toLowerCase());
      int hopType = AvroDecode.getStandardHopType(field);
      rows.add(
          new FieldRow(
              field.name(), typeDesc, field.name(), ValueMetaFactory.getValueMetaName(hopType)));
    }
    return rows;
  }

  private static Schema recordSchema(Schema schema) throws HopException {
    if (schema == null || schema.getType() != Schema.Type.RECORD) {
      throw new HopException("Avro JSON must describe a record");
    }
    return schema;
  }

  private static Schema schemaFromSample(JSONObject object) throws HopException {
    SchemaBuilder.FieldAssembler<Schema> fields = SchemaBuilder.record("sample").fields();
    int recordName = 0;
    for (Object keyObject : object.keySet()) {
      String name = String.valueOf(keyObject);
      Object value = object.get(name);
      try {
        Schema fieldSchema = schemaForValue(value, recordName);
        if (value instanceof JSONObject jsonObject && wrapperSchema(jsonObject) == null) {
          recordName++;
        }
        fields = fields.name(name).type(fieldSchema).noDefault();
      } catch (AvroRuntimeException e) {
        throw new HopException("Field '" + name + "' cannot be read from the sample JSON", e);
      }
    }
    return fields.endRecord();
  }

  private static Schema schemaForValue(Object value, int recordName) {
    if (value instanceof JSONObject jsonObject) {
      Schema wrapped = wrapperSchema(jsonObject);
      if (wrapped != null) {
        return wrapped;
      }
      Schema nested = Schema.createRecord("sample_record_" + recordName, null, "hop.sample", false);
      nested.setFields(List.of());
      return nested;
    }
    if (value instanceof JSONArray) {
      return Schema.createArray(Schema.create(Schema.Type.STRING));
    }
    if (value instanceof Boolean) {
      return Schema.create(Schema.Type.BOOLEAN);
    }
    if (value instanceof Number number) {
      if (number instanceof Double || number instanceof Float) {
        return Schema.create(Schema.Type.DOUBLE);
      }
      return Schema.create(Schema.Type.LONG);
    }
    return Schema.create(Schema.Type.STRING);
  }

  // Avro JSON encodes a non-null union value as a one-key object such as {"long": 10}.
  private static Schema wrapperSchema(JSONObject object) {
    if (object.size() != 1) {
      return null;
    }
    Object key = object.keySet().iterator().next();
    if (!(key instanceof String name) || !UNION_WRAPPERS.contains(name)) {
      return null;
    }
    return switch (name) {
      case "boolean" -> Schema.create(Schema.Type.BOOLEAN);
      case "int" -> Schema.create(Schema.Type.INT);
      case "long" -> Schema.create(Schema.Type.LONG);
      case "float" -> Schema.create(Schema.Type.FLOAT);
      case "double" -> Schema.create(Schema.Type.DOUBLE);
      case "bytes" -> Schema.create(Schema.Type.BYTES);
      case "string" -> Schema.create(Schema.Type.STRING);
      default -> null;
    };
  }
}
