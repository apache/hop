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

package org.apache.hop.core.row.value;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.util.JsonUtil;

@ValueMetaPlugin(id = "11", name = "JSON", description = "JSON object", image = "images/json.svg")
public class ValueMetaJson extends ValueMetaBase {

  /** Do String serialization using pretty printing? */
  private boolean prettyPrinting;

  public ValueMetaJson() {
    this(null);
  }

  public ValueMetaJson(String name) {
    super(name, TYPE_JSON);
    prettyPrinting = true;
  }

  @Override
  public String getString(Object object) throws HopValueException {
    return convertJsonToString(getJson(object));
  }

  @Override
  public String convertJsonToString(JsonNode jsonNode) throws HopValueException {
    try {
      return JsonUtil.mapJsonToString(jsonNode, prettyPrinting);
    } catch (Exception e) {
      throw new HopValueException("Error converting JSON value to String", e);
    }
  }

  @Override
  public Object cloneValueData(Object object) throws HopValueException {
    JsonNode jsonNode = getJson(object);
    if (jsonNode == null) {
      return null;
    }

    try {
      String jsonString = convertJsonToString(jsonNode);
      return convertStringToJson(jsonString);
    } catch (Exception e) {
      throw new HopValueException("Unable to clone JSON value", e);
    }
  }

  @Override
  // used mainly to handle json conversion from Mongo
  public JsonNode getJson(Object object) throws HopValueException {
    if (object == null) {
      return null;
    }

    try {
      if (type == TYPE_JSON) {
        switch (storageType) {
          case STORAGE_TYPE_INDEXED:
            // Resolve the dictionary/indexed value, then fall through to NORMAL handling
            object = index[((Integer) object)];
          case STORAGE_TYPE_NORMAL:
            if (object instanceof JsonNode node) {
              return node;
            }
            // Convert Mongo structures to JsonNode
            return JsonUtil.mapObjectToJson(object);
          case STORAGE_TYPE_BINARY_STRING:
            // Parse UTF-8 JSON bytes directly (faster than building a String first)
            return JsonUtil.parse((byte[]) object);
          default:
            break;
        }
      }
    } catch (IOException e) {
      throw new HopValueException("Error converting bytes to JSON (" + object + ")", e);
    }
    // delegate
    return super.getJson(object);
  }

  /**
   * Returns the immediate field names of a node, sorted alphabetically.
   *
   * <p>- Only the top-level keys of the given node are considered. If a field contains another
   * object, that child’s keys are NOT included in the result. - Keys are sorted in ascending
   * lexicographic order because it is the simplest, deterministic, and generally fastest policy for
   * this use case.
   */
  private String[] getSortedKeys(JsonNode node) {
    final int n = node.size();
    String[] sortedKeys = new String[n];
    int c = 0;

    for (var it = node.fieldNames(); it.hasNext(); ) {
      String key = it.next();

      // insertion sort
      int i = c - 1;
      while (i >= 0 && sortedKeys[i].compareTo(key) > 0) {
        sortedKeys[i + 1] = sortedKeys[i];
        i--;
      }
      sortedKeys[i + 1] = key;
      c++;
    }

    return sortedKeys;
  }

  /**
   * Classifies a {@link JsonNode} into an ordering “kind” used by the comparator. The resulting
   * integer defines the global type precedence for comparisons. Policy follows the one of
   * PostgreSQL's JSONB with the addition of the binary type.
   */
  private static int kind(JsonNode n) throws HopValueException {
    if (n == null) return 0;
    return switch (n.getNodeType()) {
      case NULL -> 0;
      case MISSING -> 1;
      case BINARY -> 2;
      case STRING -> 3;
      case NUMBER -> 4;
      case BOOLEAN -> 5;
      case ARRAY -> 6;
      case OBJECT -> 7;
      default -> throw new HopValueException("Unsupported JsonNode type: " + n.getNodeType());
    };
  }

  /**
   * Compares two JsonNode values. Type precedence is enforced via {@link #kind(JsonNode)}. Nodes of
   * different kinds are ordered by that precedence. Nodes of the same kind are ordered based on
   * their kind.
   *
   * <p>Note, object key order is irrelevant since sorted keys are used.
   */
  protected int jsonCompare(JsonNode a, JsonNode b) throws HopValueException {
    // First, compare by kind
    int kindA = kind(a);
    int kindB = kind(b);
    if (kindA != kindB) {
      return Integer.compare(kindA, kindB);
    }

    return switch (a.getNodeType()) {
      case NULL, MISSING -> 0;
      case BINARY -> jsonCompareBinary(a, b);
      case STRING -> a.textValue().compareTo(b.textValue());
      case NUMBER -> a.decimalValue().compareTo(b.decimalValue());
      case BOOLEAN -> Boolean.compare(a.booleanValue(), b.booleanValue());
      case ARRAY -> jsonCompareArray(a, b);
      case OBJECT -> jsonCompareObject(a, b);
      default -> throw new HopValueException("Unsupported JsonNode type: " + a.getNodeType());
    };
  }

  private int jsonCompareBinary(JsonNode a, JsonNode b) throws HopValueException {
    byte[] binaryA, binaryB;
    try {
      binaryA = a.binaryValue();
      binaryB = b.binaryValue();
    } catch (Exception e) {
      // If a node can't expose binary content, treat as empty
      binaryA = new byte[0];
      binaryB = new byte[0];
    }

    // Unsigned byte comparison
    final int min = Math.min(binaryA.length, binaryB.length);
    for (int i = 0; i < min; i++) {
      int diff = (binaryA[i] & 0xFF) - (binaryB[i] & 0xFF);
      if (diff != 0) return diff;
    }
    return binaryA.length - binaryB.length;
  }

  private int jsonCompareArray(JsonNode a, JsonNode b) throws HopValueException {
    // Compare by length, then element-wise
    int lengthA = a.size();
    int lengthB = b.size();

    // length comparison
    if (lengthA != lengthB) return Integer.compare(lengthA, lengthB);

    // element-wise comparison
    for (int i = 0; i < lengthA; i++) {
      int c = jsonCompare(a.get(i), b.get(i));
      if (c != 0) return c;
    }
    return 0;
  }

  private int jsonCompareObject(JsonNode a, JsonNode b) throws HopValueException {
    // get alphabetically sorted key lists
    String[] keysA = getSortedKeys(a);
    String[] keysB = getSortedKeys(b);

    // object with (n) keys > object with (n - 1) keys
    if (keysA.length != keysB.length) return Integer.compare(keysA.length, keysB.length);

    for (int i = 0; i < keysA.length; i++) {
      // compare the keys
      int keyCompare = keysA[i].compareTo(keysB[i]);
      if (keyCompare != 0) return keyCompare;

      // compare values by the same keys
      int valueCompare = jsonCompare(a.get(keysA[i]), b.get(keysB[i]));
      if (valueCompare != 0) return valueCompare;
    }

    return 0;
  }

  @Override
  protected int typeCompare(Object object1, Object object2) throws HopValueException {
    JsonNode a = getJson(object1);
    JsonNode b = getJson(object2);
    return jsonCompare(a, b);
  }

  @Override
  public void setPreparedStatementValue(
      DatabaseMeta databaseMeta, PreparedStatement preparedStatement, int index, Object data)
      throws HopDatabaseException {
    try {
      JsonNode jn = getJson(data);
      if (jn == null) {
        preparedStatement.setNull(index, Types.OTHER);
        return;
      }

      // This handles both JSON and JSONB if Postgres.
      // other dbs don't accept type OTHER
      if (databaseMeta.getIDatabase().isPostgresVariant()) {
        preparedStatement.setObject(index, jn, Types.OTHER);
        return;
      }

      // generic fallback to String
      preparedStatement.setString(index, this.convertJsonToString(jn));
    } catch (Exception e) {
      throw new HopDatabaseException(
          "Error setting JSON value #" + index + " [" + toStringMeta() + "] on prepared statement",
          e);
    }
  }

  @Override
  public Object getValueFromResultSet(IDatabase iDatabase, ResultSet resultSet, int index)
      throws HopDatabaseException {

    // Try to convert the binary stream directly to JsonNode
    try (InputStream in = resultSet.getBinaryStream(index + 1)) {
      return JsonUtil.parse(in);

    } catch (SQLException | IOException e) {
      // fallback; read object, get string representation, convert to JsonNode
      try {
        Object o = resultSet.getObject(index + 1);
        return JsonUtil.parseTextValue(o);

      } catch (SQLException ex) {
        throw new HopDatabaseException(
            "Unable to get JSON value '"
                + toStringMeta()
                + "' from database resultset, index "
                + index,
            e);
      } catch (Exception exp) {
        throw new HopDatabaseException("Unable to read JSON value", exp);
      }
    }
  }

  @Override
  public Object getNativeDataType(Object object) throws HopValueException {
    return getJson(object);
  }

  @Override
  public Class<?> getNativeDataTypeClass() throws HopValueException {
    return JsonNode.class;
  }

  /**
   * Gets prettyPrinting
   *
   * @return value of prettyPrinting
   */
  public boolean isPrettyPrinting() {
    return prettyPrinting;
  }

  /**
   * @param prettyPrinting The prettyPrinting to set
   */
  public void setPrettyPrinting(boolean prettyPrinting) {
    this.prettyPrinting = prettyPrinting;
  }

  @Override
  public String getDatabaseColumnTypeDefinition(
      IDatabase iDatabase,
      String tk,
      String pk,
      boolean useAutoIncrement,
      boolean addFieldName,
      boolean addCr) {
    final String col = addFieldName ? getName() + " " : "";
    String def = "JSON";

    // Postgres advices non-legacy app to use JSONB instead of JSON
    if (iDatabase.isPostgresVariant()) {
      def = "JSONB";
    }
    return col + def + (addCr ? Const.CR : "");
  }
}
