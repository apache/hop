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

package org.apache.hop.pipeline.transforms.salesforcebulk;

import java.math.BigDecimal;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;

/** Converts Hop values to Bulk API CSV text and CSV text back to Hop values. */
public final class SalesforceBulkValues {
  private static final String[] DATE_PATTERNS = {
    "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
    "yyyy-MM-dd'T'HH:mm:ss'Z'",
    "yyyy-MM-dd'T'HH:mm:ss.SSSX",
    "yyyy-MM-dd'T'HH:mm:ssX",
    "yyyy-MM-dd"
  };

  private SalesforceBulkValues() {}

  public static String format(IValueMeta valueMeta, Object value, boolean nullAsNa)
      throws HopValueException {
    if (valueMeta.isNull(value)) {
      return nullAsNa ? SalesforceBulkCsv.NULL_TOKEN : "";
    }
    return switch (valueMeta.getType()) {
      case IValueMeta.TYPE_BOOLEAN ->
          Boolean.TRUE.equals(valueMeta.getBoolean(value)) ? "true" : "false";
      case IValueMeta.TYPE_INTEGER -> Long.toString(valueMeta.getInteger(value));
      case IValueMeta.TYPE_NUMBER ->
          BigDecimal.valueOf(valueMeta.getNumber(value)).stripTrailingZeros().toPlainString();
      case IValueMeta.TYPE_BIGNUMBER -> valueMeta.getBigNumber(value).toPlainString();
      case IValueMeta.TYPE_DATE, IValueMeta.TYPE_TIMESTAMP -> formatDate(valueMeta.getDate(value));
      default -> valueMeta.getString(value);
    };
  }

  public static Object parse(IValueMeta target, String text) throws HopException {
    if (text == null || text.isEmpty()) {
      return null;
    }
    try {
      return switch (target.getType()) {
        case IValueMeta.TYPE_BOOLEAN -> "true".equalsIgnoreCase(text) || "1".equals(text);
        case IValueMeta.TYPE_INTEGER -> Long.valueOf(text);
        case IValueMeta.TYPE_NUMBER -> Double.valueOf(text);
        case IValueMeta.TYPE_BIGNUMBER -> new BigDecimal(text);
        case IValueMeta.TYPE_DATE, IValueMeta.TYPE_TIMESTAMP -> parseDate(text);
        default -> text;
      };
    } catch (RuntimeException e) {
      throw new HopException("Cannot read '" + text + "' as " + target.getTypeDesc(), e);
    }
  }

  static String formatDate(Date date) {
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    format.setTimeZone(TimeZone.getTimeZone("UTC"));
    return format.format(date);
  }

  static Date parseDate(String text) {
    for (String pattern : DATE_PATTERNS) {
      SimpleDateFormat format = new SimpleDateFormat(pattern);
      format.setTimeZone(TimeZone.getTimeZone("UTC"));
      format.setLenient(false);
      ParsePosition position = new ParsePosition(0);
      Date parsed = format.parse(text, position);
      if (parsed != null && position.getIndex() == text.length()) {
        return parsed;
      }
    }
    throw new IllegalArgumentException(text);
  }
}
