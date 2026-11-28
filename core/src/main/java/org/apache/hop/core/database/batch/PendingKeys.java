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

package org.apache.hop.core.database.batch;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;

/**
 * The keys of the rows that wait in a batch, so that a transform can tell when a later row has the
 * same key as one of them: a lookup of that row can't see the rows that wait, and a statement of
 * another batch would run in the order of the batches, not of the rows.
 *
 * <p>A key is compared the way a database might compare it, never stricter. Two keys the database
 * finds equal must be equal here too, or a key is inserted twice or an update is lost; two keys
 * that are equal here but not in the database only cost an extra flush. So:
 *
 * <ul>
 *   <li>numbers compare by value, whatever their type or scale: an Integer 1, a Long 1 and a
 *       BigDecimal 1.00 are the same key, and so is the text "01";
 *   <li>text compares without case, accents and surrounding spaces, as case- and accent-insensitive
 *       collations and padded CHAR columns do;
 *   <li>dates compare by their instant, to the millisecond;
 *   <li>binary values by their bytes; a null value equals a null value and empty text.
 * </ul>
 */
public final class PendingKeys {

  private static final Pattern MARKS = Pattern.compile("\\p{M}+");

  private PendingKeys() {}

  /**
   * The key of a row: the values of the given fields, each made comparable with {@link
   * #normalize(Object)}.
   *
   * @param rowMeta the layout of the row
   * @param row the row
   * @param fieldIndexes the fields that make up the key, in order
   * @return the key
   * @throws HopValueException when a value in lazy or indexed storage can't be converted
   */
  public static List<Object> key(IRowMeta rowMeta, Object[] row, int[] fieldIndexes)
      throws HopValueException {
    List<Object> key = new ArrayList<>(fieldIndexes.length);
    for (int index : fieldIndexes) {
      key.add(normalize(rowMeta.getValueMeta(index).convertToNormalStorageType(row[index])));
    }
    return key;
  }

  /**
   * @param value a value in normal storage
   * @return a value that equals the normalized value of every value a database might find equal to
   *     it
   */
  public static Object normalize(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof String string) {
      return normalizeText(string);
    }
    if (value instanceof BigDecimal number) {
      return number.signum() == 0 ? BigDecimal.ZERO : number.stripTrailingZeros();
    }
    if (value instanceof Long
        || value instanceof Integer
        || value instanceof Short
        || value instanceof Byte) {
      return normalize(BigDecimal.valueOf(((Number) value).longValue()));
    }
    if (value instanceof BigInteger number) {
      return normalize(new BigDecimal(number));
    }
    if (value instanceof Double || value instanceof Float) {
      double number = ((Number) value).doubleValue();
      return Double.isFinite(number) ? normalize(BigDecimal.valueOf(number)) : number;
    }
    if (value instanceof Date date) {
      return new Instant(date.getTime());
    }
    if (value instanceof byte[] bytes) {
      return ByteBuffer.wrap(bytes.clone());
    }
    if (value instanceof Boolean) {
      return value;
    }
    // A type without a value-based equals would never match another row: compare its text.
    return normalizeText(value.toString());
  }

  /**
   * Text without surrounding spaces, case and accents; text that is a number becomes that number,
   * as a database casts it when the column is numeric; empty text is null, as Oracle stores it.
   */
  private static Object normalizeText(String text) {
    String trimmed = text.trim();
    if (trimmed.isEmpty()) {
      return null;
    }
    if (looksNumeric(trimmed)) {
      try {
        return normalize(new BigDecimal(trimmed));
      } catch (NumberFormatException e) {
        // Not a number after all: compare it as text
      }
    }
    // Upper case first, so that letters such as the German sharp s fold to what they match
    String folded = trimmed.toUpperCase(Locale.ROOT).toLowerCase(Locale.ROOT);
    for (int i = 0; i < folded.length(); i++) {
      if (folded.charAt(i) > 127) {
        return MARKS.matcher(Normalizer.normalize(folded, Normalizer.Form.NFKD)).replaceAll("");
      }
    }
    return folded;
  }

  private static boolean looksNumeric(String text) {
    char first = text.charAt(0);
    return Character.isDigit(first) || first == '-' || first == '+' || first == '.';
  }

  /** A date as the instant it stands for, apart from the numbers. */
  private record Instant(long millis) {}
}
