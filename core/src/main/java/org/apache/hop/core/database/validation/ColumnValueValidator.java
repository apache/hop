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
package org.apache.hop.core.database.validation;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;

/**
 * Applies {@link ColumnValueConstraints} to a stream value. No JDBC: the spec is loaded once at
 * init.
 */
public final class ColumnValueValidator {

  private static final Class<?> PKG = ColumnValueValidator.class;
  private static final int MAX_VALUE_CHARS = 100;
  private static final ObjectMapper JSON = new ObjectMapper();

  private ColumnValueValidator() {
    // Utility class.
  }

  /**
   * Validate one mapped field. Conversion failure stops further checks on this field; other fields
   * of the same row are still checked by the caller.
   */
  public static List<ColumnValueError> validate(
      ColumnValueConstraints spec,
      String streamFieldName,
      IValueMeta streamMeta,
      Object value,
      boolean omitValues) {
    List<ColumnValueError> errors = new ArrayList<>();
    if (spec == null) {
      return errors;
    }
    String field = Const.NVL(streamFieldName, spec.getColumnName());

    if (value == null) {
      if (!spec.isNullable()) {
        errors.add(
            error(
                field,
                spec,
                ColumnValueErrorCode.NULL_NOT_ALLOWED,
                BaseMessages.getString(
                    PKG, "ColumnValueValidator.NullNotAllowed", spec.getColumnName())));
      }
      return errors;
    }

    Object converted = value;
    IValueMeta workingMeta = streamMeta;
    if (spec.getTargetValueMeta() != null && streamMeta != null) {
      try {
        converted = spec.getTargetValueMeta().convertData(streamMeta, value);
        workingMeta = spec.getTargetValueMeta();
      } catch (HopValueException e) {
        errors.add(
            error(
                field,
                spec,
                ColumnValueErrorCode.CONVERSION,
                BaseMessages.getString(
                    PKG,
                    "ColumnValueValidator.Conversion",
                    spec.getColumnName(),
                    typeLabel(spec),
                    Const.NVL(e.getMessage(), ""),
                    preview(streamMeta, value, omitValues))));
        return errors;
      }
    }

    if (converted == null) {
      if (!spec.isNullable()) {
        errors.add(
            error(
                field,
                spec,
                ColumnValueErrorCode.NULL_NOT_ALLOWED,
                BaseMessages.getString(
                    PKG, "ColumnValueValidator.NullNotAllowed", spec.getColumnName())));
      }
      return errors;
    }

    checkString(spec, field, workingMeta, converted, omitValues, errors);
    checkNumeric(spec, field, workingMeta, converted, omitValues, errors);
    checkIntegerRange(spec, field, workingMeta, converted, omitValues, errors);
    checkUuid(spec, field, workingMeta, converted, omitValues, errors);
    checkJson(spec, field, workingMeta, converted, omitValues, errors);
    return errors;
  }

  private static void checkString(
      ColumnValueConstraints spec,
      String field,
      IValueMeta workingMeta,
      Object converted,
      boolean omitValues,
      List<ColumnValueError> errors) {
    boolean lengthLimited = hasLimitedLength(spec.getStringMaxLength());
    int hopType = spec.getHopType();
    boolean stringColumn =
        hopType == IValueMeta.TYPE_STRING
            || hopType == IValueMeta.TYPE_NONE
            || lengthLimited
            || spec.isRejectNulChar();
    if (!stringColumn) {
      return;
    }
    if (workingMeta != null && workingMeta.isBinary()) {
      return;
    }
    String string;
    try {
      string = workingMeta != null ? workingMeta.getString(converted) : String.valueOf(converted);
    } catch (HopValueException e) {
      return;
    }
    if (string == null) {
      return;
    }

    String preview = previewString(string, omitValues);

    if (spec.isRejectNulChar() && string.indexOf('\0') >= 0) {
      errors.add(
          error(
              field,
              spec,
              ColumnValueErrorCode.INVALID_ENCODING,
              BaseMessages.getString(
                  PKG, "ColumnValueValidator.NulChar", spec.getColumnName(), preview)));
    }

    Charset charset = charsetOf(spec.getCharacterSet());
    ByteBuffer encoded = null;
    try {
      CharsetEncoder encoder =
          charset
              .newEncoder()
              .onMalformedInput(CodingErrorAction.REPORT)
              .onUnmappableCharacter(CodingErrorAction.REPORT);
      encoded = encoder.encode(CharBuffer.wrap(string));
    } catch (CharacterCodingException e) {
      errors.add(
          error(
              field,
              spec,
              ColumnValueErrorCode.INVALID_ENCODING,
              BaseMessages.getString(
                  PKG,
                  "ColumnValueValidator.InvalidEncoding",
                  spec.getColumnName(),
                  charset.displayName(),
                  preview)));
    }

    if (lengthLimited) {
      int actual;
      String unitLabel;
      if (spec.getLengthUnit() == StringLengthUnit.BYTES) {
        if (encoded == null) {
          return;
        }
        actual = encoded.remaining();
        unitLabel = "byte";
      } else {
        actual = string.codePointCount(0, string.length());
        unitLabel = nativeOr("varchar", spec.getNativeTypeName());
      }
      if (actual > spec.getStringMaxLength()) {
        errors.add(
            error(
                field,
                spec,
                ColumnValueErrorCode.STRING_TOO_LONG,
                BaseMessages.getString(
                    PKG,
                    "ColumnValueValidator.StringTooLong",
                    spec.getColumnName(),
                    Integer.toString(actual),
                    unitLabel,
                    Integer.toString(spec.getStringMaxLength()),
                    preview)));
      }
    }
  }

  private static void checkNumeric(
      ColumnValueConstraints spec,
      String field,
      IValueMeta workingMeta,
      Object converted,
      boolean omitValues,
      List<ColumnValueError> errors) {
    if (spec.getNumericPrecision() <= 0) {
      return;
    }
    BigDecimal number;
    try {
      number =
          workingMeta != null
              ? workingMeta.getBigNumber(converted)
              : new BigDecimal(converted.toString());
    } catch (Exception e) {
      return;
    }
    if (number == null) {
      return;
    }
    int scale = Math.max(spec.getNumericScale(), 0);
    BigDecimal rounded = number.setScale(scale, RoundingMode.HALF_UP);
    BigDecimal normalized = rounded.stripTrailingZeros();
    int valueScale = normalized.scale();
    int valuePrecision = normalized.precision();
    int integerDigits = valuePrecision - valueScale;
    int maxIntegerDigits = spec.getNumericPrecision() - scale;
    if (integerDigits > maxIntegerDigits) {
      errors.add(
          error(
              field,
              spec,
              ColumnValueErrorCode.NUMERIC_OVERFLOW,
              BaseMessages.getString(
                  PKG,
                  "ColumnValueValidator.NumericOverflow",
                  spec.getColumnName(),
                  nativeOr("numeric", spec.getNativeTypeName()),
                  Integer.toString(spec.getNumericPrecision()),
                  Integer.toString(scale),
                  preview(workingMeta, converted, omitValues))));
    }
  }

  private static void checkIntegerRange(
      ColumnValueConstraints spec,
      String field,
      IValueMeta workingMeta,
      Object converted,
      boolean omitValues,
      List<ColumnValueError> errors) {
    if (spec.getIntegerMin() == null || spec.getIntegerMax() == null) {
      return;
    }
    Long number;
    try {
      number =
          workingMeta != null
              ? workingMeta.getInteger(converted)
              : Long.valueOf(converted.toString());
    } catch (Exception e) {
      return;
    }
    if (number == null) {
      return;
    }
    if (number < spec.getIntegerMin() || number > spec.getIntegerMax()) {
      errors.add(
          error(
              field,
              spec,
              ColumnValueErrorCode.INTEGER_RANGE,
              BaseMessages.getString(
                  PKG,
                  "ColumnValueValidator.IntegerRange",
                  spec.getColumnName(),
                  Long.toString(number),
                  nativeOr("integer", spec.getNativeTypeName()),
                  Long.toString(spec.getIntegerMin()),
                  Long.toString(spec.getIntegerMax()),
                  preview(workingMeta, converted, omitValues))));
    }
  }

  private static void checkUuid(
      ColumnValueConstraints spec,
      String field,
      IValueMeta workingMeta,
      Object converted,
      boolean omitValues,
      List<ColumnValueError> errors) {
    if (!spec.isUuid()) {
      return;
    }
    if (converted instanceof UUID) {
      return;
    }
    String string;
    try {
      string = workingMeta != null ? workingMeta.getString(converted) : String.valueOf(converted);
    } catch (HopValueException e) {
      return;
    }
    if (string == null) {
      return;
    }
    try {
      UUID.fromString(string);
    } catch (IllegalArgumentException e) {
      errors.add(
          error(
              field,
              spec,
              ColumnValueErrorCode.INVALID_UUID,
              BaseMessages.getString(
                  PKG,
                  "ColumnValueValidator.InvalidUuid",
                  spec.getColumnName(),
                  previewString(string, omitValues))));
    }
  }

  private static void checkJson(
      ColumnValueConstraints spec,
      String field,
      IValueMeta workingMeta,
      Object converted,
      boolean omitValues,
      List<ColumnValueError> errors) {
    if (!spec.isJson()) {
      return;
    }
    if (converted != null
        && converted.getClass().getName().startsWith("com.fasterxml.jackson.databind")) {
      return;
    }
    String string;
    try {
      string = workingMeta != null ? workingMeta.getString(converted) : String.valueOf(converted);
    } catch (HopValueException e) {
      return;
    }
    if (string == null) {
      return;
    }
    try (JsonParser parser = JSON.getFactory().createParser(string)) {
      JSON.readTree(parser);
      if (parser.nextToken() != null) {
        throw new IllegalArgumentException("trailing data");
      }
    } catch (Exception e) {
      errors.add(
          error(
              field,
              spec,
              ColumnValueErrorCode.INVALID_JSON,
              BaseMessages.getString(
                  PKG,
                  "ColumnValueValidator.InvalidJson",
                  spec.getColumnName(),
                  previewString(string, omitValues))));
    }
  }

  public static boolean hasLimitedLength(int length) {
    return length > 0 && length < DatabaseMeta.CLOB_LENGTH;
  }

  public static Charset charsetOf(String name) {
    if (Utils.isEmpty(name) || "UTF8".equalsIgnoreCase(name) || "UTF-8".equalsIgnoreCase(name)) {
      return StandardCharsets.UTF_8;
    }
    try {
      return Charset.forName(name);
    } catch (Exception e) {
      return StandardCharsets.UTF_8;
    }
  }

  private static String nativeOr(String fallback, String nativeTypeName) {
    return Utils.isEmpty(nativeTypeName) ? fallback : nativeTypeName;
  }

  private static String typeLabel(ColumnValueConstraints spec) {
    if (!Utils.isEmpty(spec.getNativeTypeName())) {
      return spec.getNativeTypeName();
    }
    if (spec.getTargetValueMeta() != null) {
      return spec.getTargetValueMeta().getTypeDesc();
    }
    return "column type";
  }

  private static String preview(IValueMeta meta, Object value, boolean omitValues) {
    if (omitValues) {
      return BaseMessages.getString(PKG, "ColumnValueValidator.OmittedValue");
    }
    try {
      String string = meta != null ? meta.getString(value) : String.valueOf(value);
      return quotePreview(string);
    } catch (Exception e) {
      return quotePreview(String.valueOf(value));
    }
  }

  private static String previewString(String value, boolean omitValues) {
    if (omitValues) {
      return BaseMessages.getString(PKG, "ColumnValueValidator.OmittedValue");
    }
    return quotePreview(value);
  }

  private static String quotePreview(String value) {
    if (value == null) {
      return "'null'";
    }
    String sanitized = value.replace("\0", "\\0");
    if (sanitized.length() > MAX_VALUE_CHARS) {
      sanitized = sanitized.substring(0, MAX_VALUE_CHARS) + "...";
    }
    return "'" + sanitized + "'";
  }

  private static ColumnValueError error(
      String field, ColumnValueConstraints spec, ColumnValueErrorCode code, String message) {
    return new ColumnValueError(field, spec.getColumnName(), code, message);
  }
}
