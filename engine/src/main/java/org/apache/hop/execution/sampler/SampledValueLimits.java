/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.execution.sampler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.nio.charset.StandardCharsets;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaAvroRecord;
import org.apache.hop.core.row.value.ValueMetaJson;
import org.apache.hop.core.util.JsonUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.execution.profiling.ExecutionDataProfile;

/**
 * Resolved limits for String, JSON, Binary, and Avro values kept by an execution data profile.
 * Empty or non-numeric profile settings mean no limit. Zero stores none of that type.
 */
@Getter
public final class SampledValueLimits {
  public static final String NOT_STORED = "<not stored>";

  private static final SampledValueLimits UNLIMITED =
      new SampledValueLimits(null, null, null, null);
  private static final String CHARACTERS = "characters";

  private final Integer stringLimit;
  private final Integer jsonLimit;
  private final Integer binaryLimit;
  private final Integer avroLimit;

  private SampledValueLimits(
      Integer stringLimit, Integer jsonLimit, Integer binaryLimit, Integer avroLimit) {
    this.stringLimit = stringLimit;
    this.jsonLimit = jsonLimit;
    this.binaryLimit = binaryLimit;
    this.avroLimit = avroLimit;
  }

  public static SampledValueLimits unlimited() {
    return UNLIMITED;
  }

  /**
   * Marker stored in place of a String or JSON value that is over its limit.
   *
   * @param size The measured size that was left out
   * @param unit {@code characters} for String and JSON
   * @return The short replacement text
   */
  public static String notStored(int size, String unit) {
    return "<not stored, " + size + " " + unit + ">";
  }

  /**
   * Resolve the profile fields with {@code variables}. A blank, negative, or non-numeric value is
   * unlimited. Zero stores nothing of that type.
   */
  public static SampledValueLimits from(ExecutionDataProfile profile, IVariables variables) {
    if (profile == null) {
      return unlimited();
    }
    return new SampledValueLimits(
        parseLimit(resolve(variables, profile.getStringValueLimit())),
        parseLimit(resolve(variables, profile.getJsonValueLimit())),
        parseLimit(resolve(variables, profile.getBinaryValueLimit())),
        parseLimit(resolve(variables, profile.getAvroValueLimit())));
  }

  public boolean hasLimits() {
    return stringLimit != null || jsonLimit != null || binaryLimit != null || avroLimit != null;
  }

  /**
   * @return true when this value must not be kept as itself. Nulls are kept as null.
   */
  public boolean omit(IValueMeta valueMeta, Object value) throws HopValueException {
    return decide(valueMeta, value).omit;
  }

  /**
   * A new row array for storage. Cells over a limit are replaced. Cells that stay are cloned when
   * the value type needs its own copy. The pipeline row is left unchanged, and a value that is
   * dropped is not copied first.
   */
  public Object[] copyRow(IRowMeta rowMeta, Object[] row) throws HopValueException {
    if (row == null) {
      return null;
    }
    if (rowMeta == null || !hasLimits()) {
      return rowMeta == null ? row.clone() : rowMeta.cloneRow(row);
    }

    int fields = Math.min(rowMeta.size(), row.length);
    Decision[] decisions = new Decision[fields];
    boolean anyOmitted = false;
    for (int i = 0; i < fields; i++) {
      decisions[i] = decide(rowMeta.getValueMeta(i), row[i]);
      anyOmitted = anyOmitted || decisions[i].omit;
    }
    if (!anyOmitted) {
      return rowMeta.cloneRow(row);
    }

    Object[] copy = row.clone();
    for (int i = 0; i < fields; i++) {
      if (decisions[i].omit) {
        copy[i] = decisions[i].replacement;
      } else {
        copy[i] = copyKept(rowMeta.getValueMeta(i), row[i]);
      }
    }
    return copy;
  }

  private static Object copyKept(IValueMeta valueMeta, Object value) throws HopValueException {
    if (value == null || valueMeta == null) {
      return value;
    }
    return switch (valueMeta.getType()) {
      case IValueMeta.TYPE_STRING,
              IValueMeta.TYPE_JSON,
              IValueMeta.TYPE_BINARY,
              IValueMeta.TYPE_AVRO,
              IValueMeta.TYPE_DATE,
              IValueMeta.TYPE_NUMBER,
              IValueMeta.TYPE_INTEGER,
              IValueMeta.TYPE_BIGNUMBER,
              IValueMeta.TYPE_BOOLEAN ->
          valueMeta.cloneValueData(value);
      default -> value;
    };
  }

  private Decision decide(IValueMeta valueMeta, Object value) throws HopValueException {
    if (valueMeta == null || value == null || valueMeta.isNull(value)) {
      return Decision.keep();
    }
    return switch (valueMeta.getType()) {
      case IValueMeta.TYPE_STRING -> decideString(valueMeta, value);
      case IValueMeta.TYPE_JSON -> decideJson(valueMeta, value);
      case IValueMeta.TYPE_BINARY -> decideBinary(valueMeta, value);
      case IValueMeta.TYPE_AVRO -> decideAvro(value);
      default -> Decision.keep();
    };
  }

  private Decision decideString(IValueMeta valueMeta, Object value) throws HopValueException {
    if (stringLimit == null) {
      return Decision.keep();
    }
    if (stringLimit == 0) {
      return Decision.drop(NOT_STORED);
    }
    int length = stringLength(valueMeta, value);
    if (length > stringLimit) {
      return Decision.drop(notStored(length, CHARACTERS));
    }
    return Decision.keep();
  }

  private Decision decideJson(IValueMeta valueMeta, Object value) throws HopValueException {
    if (jsonLimit == null) {
      return Decision.keep();
    }
    if (jsonLimit == 0) {
      return Decision.drop(TextNode.valueOf(NOT_STORED));
    }
    int length;
    try {
      length = jsonLength(valueMeta, value);
    } catch (Exception e) {
      return Decision.drop(TextNode.valueOf(NOT_STORED));
    }
    if (length > jsonLimit) {
      return Decision.drop(TextNode.valueOf(notStored(length, CHARACTERS)));
    }
    return Decision.keep();
  }

  private Decision decideBinary(IValueMeta valueMeta, Object value) throws HopValueException {
    if (binaryLimit == null) {
      return Decision.keep();
    }
    if (binaryLimit == 0) {
      return Decision.drop(null);
    }
    int size;
    try {
      size = binarySize(valueMeta, value);
    } catch (Exception e) {
      return Decision.drop(null);
    }
    if (size > binaryLimit) {
      return Decision.drop(null);
    }
    return Decision.keep();
  }

  private Decision decideAvro(Object value) {
    if (avroLimit == null) {
      return Decision.keep();
    }
    if (avroLimit == 0) {
      return Decision.drop(null);
    }
    int size;
    try {
      size = ValueMetaAvroRecord.storedPayloadBytes(value);
    } catch (Exception e) {
      return Decision.drop(null);
    }
    if (size > avroLimit) {
      return Decision.drop(null);
    }
    return Decision.keep();
  }

  private static int stringLength(IValueMeta valueMeta, Object value) throws HopValueException {
    if (valueMeta.getStorageType() == IValueMeta.STORAGE_TYPE_BINARY_STRING
        && value instanceof byte[] bytes) {
      String text = valueMeta.getString(value);
      return text == null ? bytes.length : text.length();
    }
    if (value instanceof String text) {
      return text.length();
    }
    String text = valueMeta.getString(value);
    return text == null ? 0 : text.length();
  }

  private static int jsonLength(IValueMeta valueMeta, Object value) throws Exception {
    if (valueMeta.getStorageType() == IValueMeta.STORAGE_TYPE_BINARY_STRING
        && value instanceof byte[] bytes) {
      return new String(bytes, StandardCharsets.UTF_8).length();
    }
    JsonNode node;
    if (value instanceof JsonNode jsonNode) {
      node = jsonNode;
    } else if (valueMeta instanceof ValueMetaJson jsonMeta) {
      node = jsonMeta.getJson(value);
    } else {
      node = valueMeta.getJson(value);
    }
    if (node == null) {
      return 0;
    }
    String text = JsonUtil.mapJsonToString(node, false);
    return text == null ? 0 : text.length();
  }

  private static int binarySize(IValueMeta valueMeta, Object value) throws HopValueException {
    if (value instanceof byte[] bytes) {
      return bytes.length;
    }
    byte[] bytes = valueMeta.getBinary(value);
    return bytes == null ? 0 : bytes.length;
  }

  private static String resolve(IVariables variables, String value) {
    if (value == null || variables == null) {
      return value;
    }
    return variables.resolve(value);
  }

  private static Integer parseLimit(String raw) {
    if (StringUtils.isBlank(raw)) {
      return null;
    }
    try {
      int parsed = Integer.parseInt(raw.trim());
      return parsed < 0 ? null : parsed;
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private record Decision(boolean omit, Object replacement) {
    private static Decision keep() {
      return new Decision(false, null);
    }

    private static Decision drop(Object replacement) {
      return new Decision(true, replacement);
    }
  }
}
