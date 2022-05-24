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

import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DecimalNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.LongNode;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;

import java.math.BigDecimal;

@ValueMetaPlugin(id = "11", name = "JSON", description = "JSON object", image = "json.svg")
public class ValueMetaJson extends ValueMetaBase implements IValueMeta {

  /** Value type indicating that the value contains a native JSON object */
  public static final int TYPE_JSON = 11;

  /** Do String serialization using pretty printing? */
  private boolean prettyPrinting;

  @Override
  public int compare(Object data1, Object data2) throws HopValueException {
    JsonNode json1 = (JsonNode) data1;
    JsonNode json2 = (JsonNode) data2;

    String string1 = convertJsonToString(json1);
    String string2 = convertJsonToString(json2);

    return string1.compareTo(string2);
  }

  public String convertJsonToString(JsonNode jsonNode) throws HopValueException {
    try {
      ObjectMapper objectMapper = new ObjectMapper();
      if (prettyPrinting) {
        return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(jsonNode);
      } else {
        return objectMapper.writeValueAsString(jsonNode);
      }
    } catch (Exception e) {
      throw new HopValueException("Error converting JSON value to String", e);
    }
  }

  public JsonNode convertStringToJson(String jsonString) throws HopValueException {
    try {
      ObjectMapper objectMapper = JsonMapper.builder().enable(JsonReadFeature.ALLOW_UNQUOTED_FIELD_NAMES).build();      
      return objectMapper.readTree(jsonString);
    } catch (Exception e) {
      throw new HopValueException("Error converting string to JSON value: '" + jsonString + "'", e);
    }
  }

  public ValueMetaJson() {
    this(null);
  }

  public ValueMetaJson(String name) {
    super(name, TYPE_JSON);
    prettyPrinting = true;
  }

  public JsonNode getJson(Object object) throws HopValueException {
    if (object == null) {
      return null;
    }

    switch (type) {
      case TYPE_JSON:
        switch (storageType) {
          case STORAGE_TYPE_NORMAL:
            return (JsonNode) object;
          case STORAGE_TYPE_BINARY_STRING:
            return convertStringToJson(convertBinaryStringToString((byte[]) object));
          case STORAGE_TYPE_INDEXED:
            return (JsonNode) index[((Integer) object)];
          default:
            throw new HopValueException(
                toString() + " : Unknown storage type " + storageType + " specified.");
        }
      case TYPE_STRING:
        switch (storageType) {
          case STORAGE_TYPE_NORMAL:
            return convertStringToJson((String) object);
          case STORAGE_TYPE_BINARY_STRING:
            return convertStringToJson((String) convertBinaryStringToNativeType((byte[]) object));
          case STORAGE_TYPE_INDEXED:
            return convertStringToJson((String) index[(Integer) object]);
          default:
            throw new HopValueException(
                toString() + " : Unknown storage type " + storageType + " specified.");
        }
      case TYPE_NUMBER:
        Double number;
        switch (storageType) {
          case STORAGE_TYPE_NORMAL:
            number = (Double) object;
            break;
          case STORAGE_TYPE_BINARY_STRING:
            number = convertStringToNumber(convertBinaryStringToString((byte[]) object));
            break;
          case STORAGE_TYPE_INDEXED:
            number = (Double) index[(Integer) object];
            break;
          default:
            throw new HopValueException(
                toString() + " : Unknown storage type " + storageType + " specified.");
        }
        return new DoubleNode(number);

      case TYPE_INTEGER:
        Long integer;
        switch (storageType) {
          case STORAGE_TYPE_NORMAL:
            integer = (Long) object;
            break;
          case STORAGE_TYPE_BINARY_STRING:
            integer = (Long) convertBinaryStringToNativeType((byte[]) object);
            break;
          case STORAGE_TYPE_INDEXED:
            integer = (Long) index[(Integer) object];
            break;
          default:
            throw new HopValueException(
                toString() + " : Unknown storage type " + storageType + " specified.");
        }
        return new LongNode(integer);

      case TYPE_BIGNUMBER:
        BigDecimal bigDecimal;
        switch (storageType) {
          case STORAGE_TYPE_NORMAL:
            bigDecimal = (BigDecimal) object;
            break;
          case STORAGE_TYPE_BINARY_STRING:
            bigDecimal = (BigDecimal) convertBinaryStringToNativeType((byte[]) object);
            break;
          case STORAGE_TYPE_INDEXED:
            bigDecimal = (BigDecimal) index[(Integer) object];
            break;
          default:
            throw new HopValueException(
                toString() + " : Unknown storage type " + storageType + " specified.");
        }
        return new DecimalNode(bigDecimal);

      case TYPE_BOOLEAN:
        boolean bool;
        switch (storageType) {
          case STORAGE_TYPE_NORMAL:
            bool = (Boolean) object;
            break;
          case STORAGE_TYPE_BINARY_STRING:
            bool = (Boolean) convertBinaryStringToNativeType((byte[]) object);
            break;
          case STORAGE_TYPE_INDEXED:
            bool = (Boolean) index[(Integer) object];
            break;
          default:
            throw new HopValueException(
                toString() + " : Unknown storage type " + storageType + " specified.");
        }
        return BooleanNode.valueOf(bool);

      case TYPE_DATE:
        throw new HopValueException(
            toString() + " : I don't know how to convert a date to a JSON object.");
      case TYPE_TIMESTAMP:
        throw new HopValueException(
            toString() + " : I don't know how to convert a timestamp to a JSON object.");
      case TYPE_BINARY:
        throw new HopValueException(
            toString() + " : I don't know how to convert a binary value to JSON object.");
      case TYPE_SERIALIZABLE:
        throw new HopValueException(
            toString() + " : I don't know how to convert a serializable value to JSON object.");

      default:
        throw new HopValueException(toString() + " : Unknown type " + type + " specified.");
    }
  }

  @Override
  public String getString(Object object) throws HopValueException {
    return convertJsonToString(getJson(object));
  }

  @Override
  public byte[] getBinaryString(Object object) throws HopValueException {
    if (isStorageBinaryString() && identicalFormat) {
      return (byte[]) object; // shortcut it directly for better performance.
    }
    if (object == null) {
      return null;
    }
    switch (storageType) {
      case STORAGE_TYPE_NORMAL:
        return convertStringToBinaryString(getString(object));
      case STORAGE_TYPE_BINARY_STRING:
        return convertStringToBinaryString(
            getString(convertStringToJson(convertBinaryStringToString((byte[]) object))));
      case STORAGE_TYPE_INDEXED:
        return convertStringToBinaryString(
            convertJsonToString((JsonNode) index[((Integer) object)]));
      default:
        throw new HopValueException(
            toString() + " : Unknown storage type " + storageType + " specified.");
    }
  }

  @Override
  public Object convertDataFromString(
      String pol, IValueMeta convertMeta, String nullIf, String ifNull, int trimType)
      throws HopValueException {
    // null handling and conversion of value to null
    //
    String nullValue = nullIf;
    if (nullValue == null) {
      switch (convertMeta.getType()) {
        case IValueMeta.TYPE_BOOLEAN:
          nullValue = Const.NULL_BOOLEAN;
          break;
        case IValueMeta.TYPE_STRING:
          nullValue = Const.NULL_STRING;
          break;
        case IValueMeta.TYPE_BIGNUMBER:
          nullValue = Const.NULL_BIGNUMBER;
          break;
        case IValueMeta.TYPE_NUMBER:
          nullValue = Const.NULL_NUMBER;
          break;
        case IValueMeta.TYPE_INTEGER:
          nullValue = Const.NULL_INTEGER;
          break;
        case IValueMeta.TYPE_DATE:
          nullValue = Const.NULL_DATE;
          break;
        case IValueMeta.TYPE_BINARY:
          nullValue = Const.NULL_BINARY;
          break;
        default:
          nullValue = Const.NULL_NONE;
          break;
      }
    }

    // See if we need to convert a null value into a String
    // For example, we might want to convert null into "Empty".
    //
    if (!Utils.isEmpty(ifNull)) {
      // Note that you can't pull the pad method up here as a nullComp variable
      // because you could get an NPE since you haven't checked isEmpty(pol)
      // yet!
      if (Utils.isEmpty(pol)
          || pol.equalsIgnoreCase(Const.rightPad(new StringBuilder(nullValue), pol.length()))) {
        pol = ifNull;
      }
    }

    // See if the polled value is empty
    // In that case, we have a null value on our hands...
    //
    if (Utils.isEmpty(pol)) {
      return null;
    } else {
      // if the null_value is specified, we try to match with that.
      //
      if (!Utils.isEmpty(nullValue)) {
        if (nullValue.length() <= pol.length()) {
          // If the polled value is equal to the spaces right-padded null_value,
          // we have a match
          //
          if (pol.equalsIgnoreCase(Const.rightPad(new StringBuilder(nullValue), pol.length()))) {
            return null;
          }
        }
      } else {
        // Verify if there are only spaces in the polled value...
        // We consider that empty as well...
        //
        if (Const.onlySpaces(pol)) {
          return null;
        }
      }
    }

    StringBuilder strpol;
    // Trimming
    switch (trimType) {
      case IValueMeta.TRIM_TYPE_LEFT:
        strpol = new StringBuilder(pol);
        while (strpol.length() > 0 && strpol.charAt(0) == ' ') {
          strpol.deleteCharAt(0);
        }
        pol = strpol.toString();

        break;
      case IValueMeta.TRIM_TYPE_RIGHT:
        strpol = new StringBuilder(pol);
        while (strpol.length() > 0 && strpol.charAt(strpol.length() - 1) == ' ') {
          strpol.deleteCharAt(strpol.length() - 1);
        }
        pol = strpol.toString();

        break;
      case IValueMeta.TRIM_TYPE_BOTH:
        strpol = new StringBuilder(pol);
        while (strpol.length() > 0 && strpol.charAt(0) == ' ') {
          strpol.deleteCharAt(0);
        }
        while (strpol.length() > 0 && strpol.charAt(strpol.length() - 1) == ' ') {
          strpol.deleteCharAt(strpol.length() - 1);
        }
        pol = strpol.toString();
        break;
      default:
        break;
    }

    // On with the regular program...
    // Simply call the ValueMeta routines to do the conversion
    // We need to do some effort here: copy all
    //
    return convertData(convertMeta, pol);
  }

  /**
   * Convert the specified data to the data type specified in this object.
   *
   * @param meta2 the metadata of the object to be converted
   * @param data2 the data of the object to be converted
   * @return the object in the data type of this value metadata object
   * @throws HopValueException in case there is a data conversion error
   */
  @Override
  public Object convertData(IValueMeta meta2, Object data2) throws HopValueException {
    switch (meta2.getType()) {
      case TYPE_STRING:
        return convertStringToJson(meta2.getString(data2));
      default:
        throw new HopValueException(
            meta2.toStringMeta() + " : can't be converted to an Internet Address");
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

  /** @param prettyPrinting The prettyPrinting to set */
  public void setPrettyPrinting(boolean prettyPrinting) {
    this.prettyPrinting = prettyPrinting;
  }
}
