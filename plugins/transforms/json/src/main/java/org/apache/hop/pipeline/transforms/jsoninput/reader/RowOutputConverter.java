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

package org.apache.hop.pipeline.transforms.jsoninput.reader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;
import net.minidev.json.JSONObject;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.pipeline.transforms.jsoninput.JsonInputData;

/** Converts raw reader to output row values */
public class RowOutputConverter {

  private final ILogChannel log;

  public RowOutputConverter(ILogChannel log) {
    this.log = log;
  }

  private Object getValue(IValueMeta targetMeta, IValueMeta strConvertMeta, Object value)
      throws HopValueException {
    if (targetMeta.isNumeric()) {
      Object number = directNumber(targetMeta, value);
      if (number != null) {
        return number;
      }
      try {
        // attempt direct conversion
        return targetMeta.getNativeDataType(value);
      } catch (HopValueException e) {
        if (log.isDebug()) {
          log.logDebug(e.getLocalizedMessage(), e);
        }
      }
    }
    // convert from string
    String strValue = getStringValue(value);
    // A JSON null or missing node is already a Hop null. Do not let the empty-string
    // convention turn that null into "".
    if (strValue == null) {
      return null;
    }
    return targetMeta.convertDataFromString(
        strValue, strConvertMeta, null, null, targetMeta.getTrimType());
  }

  /**
   * A JSON number as the value of a numeric field.
   *
   * <p>A number in JSON is a number, written the one way JSON allows: digits, a dot, an exponent.
   * It is not text in the format of the field, so the field's mask, decimal and grouping symbols
   * are not used to read it: those say how the field is shown and how a JSON <em>string</em> is
   * parsed. Reading the number through them would give 1995 for 199.5 wherever the comma is the
   * decimal symbol.
   *
   * <p>The value is what parsing its text with a dot as decimal symbol gives: a decimal put into an
   * Integer field loses its fraction.
   *
   * @return the value, or null when the value is not a JSON number or has to take the usual route
   */
  private static Object directNumber(IValueMeta targetMeta, Object value) {
    int type = targetMeta.getType();
    if (type != IValueMeta.TYPE_INTEGER
        && type != IValueMeta.TYPE_NUMBER
        && type != IValueMeta.TYPE_BIGNUMBER) {
      return null;
    }

    // Whole numbers that fit a long
    Long whole = null;
    if (value instanceof JsonNode node) {
      if (node.isIntegralNumber() && node.canConvertToLong()) {
        whole = node.longValue();
      }
    } else if (value instanceof Integer
        || value instanceof Long
        || value instanceof Short
        || value instanceof Byte) {
      whole = ((Number) value).longValue();
    }
    if (whole != null) {
      return switch (type) {
        case IValueMeta.TYPE_INTEGER -> whole;
        case IValueMeta.TYPE_NUMBER -> (double) whole;
        default -> BigDecimal.valueOf(whole);
      };
    }

    // A double into a Number field needs no detour
    if (type == IValueMeta.TYPE_NUMBER) {
      if (value instanceof Double number) {
        return Double.isNaN(number) || Double.isInfinite(number) ? null : number;
      }
      if (value instanceof JsonNode node && node.isDouble()) {
        return node.doubleValue();
      }
    }

    // Every other number, by its exact decimal value
    BigDecimal decimal;
    if (value instanceof JsonNode node) {
      if (!node.isNumber()) {
        return null;
      }
      // a float or double node by its text: 0.1f is 0.1, not 0.100000001490116...
      decimal = node.isFloatingPointNumber() ? new BigDecimal(node.asText()) : node.decimalValue();
    } else if (value instanceof Double || value instanceof Float) {
      double number = ((Number) value).doubleValue();
      if (Double.isNaN(number) || Double.isInfinite(number)) {
        return null;
      }
      decimal = new BigDecimal(value.toString());
    } else if (value instanceof BigDecimal bigDecimal) {
      decimal = bigDecimal;
    } else if (value instanceof BigInteger bigInteger) {
      decimal = new BigDecimal(bigInteger);
    } else {
      return null;
    }
    switch (type) {
      case IValueMeta.TYPE_NUMBER:
        return decimal.doubleValue();
      case IValueMeta.TYPE_BIGNUMBER:
        return decimal;
      default:
        // An Integer field: drop the fraction. A value outside the range of a long takes the
        // usual route, which decides what happens to it.
        return decimal.abs().compareTo(LONG_RANGE) < 0 ? (Object) decimal.longValue() : null;
    }
  }

  private static final BigDecimal LONG_RANGE = BigDecimal.valueOf(Long.MAX_VALUE);

  // Package-private and static so RowOutputConverterTest can exercise it directly
  // without instantiating the surrounding transform.
  static String getStringValue(Object jo) {
    if (jo == null) {
      return null;
    }
    // A JSON `null` (e.g. `{"foo": null}`) is parsed by Jackson into a NullNode -
    // a non-null Java reference whose toString() returns the four-character literal
    // "null". Without this branch it flows to jo.toString() at the bottom and the
    // output field ends up containing the string "null" instead of a Hop null.
    // MissingNode covers the JsonPath-returned-missing case for symmetry.
    // See Apache Hop #7990.
    if (jo instanceof JsonNode jsonNode && (jsonNode.isNull() || jsonNode.isMissingNode())) {
      return null;
    }
    if (jo instanceof Map) {
      Map<String, ?> asStrMap = (Map<String, ?>) jo;
      return JSONObject.toJSONString(asStrMap);
    }
    if (jo instanceof TextNode jot) {
      // this avoids returning string enclosed by "" if JsonNode
      return jot.asText();
    }
    return jo.toString();
  }

  public Object[] getRow(Object[] baseOutputRow, Object[] rawPartRow, JsonInputData data)
      throws HopException {
    if (rawPartRow == null) {
      return null;
    }
    for (int i = 0; i < rawPartRow.length; i++) {
      int outIdx = data.totalpreviousfields + i;
      Object val =
          getValue(
              data.outputRowMeta.getValueMeta(outIdx),
              data.convertRowMeta.getValueMeta(outIdx),
              rawPartRow[i]);
      rawPartRow[i] = val;
      if (val == null && data.repeatedFields.get(i) && data.previousRow != null) {
        rawPartRow[i] = data.previousRow[outIdx];
      }
    }
    data.previousRow = RowDataUtil.addRowData(baseOutputRow, data.totalpreviousfields, rawPartRow);
    return data.previousRow;
  }
}
