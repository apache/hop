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

package org.apache.hop.openformula.typing;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;
import org.apache.hop.openformula.error.FormulaErrorValue;

/** An immutable typed value container, pairing a {@link DataType} with a raw value object. */
public record TypedValue(DataType type, Object value) implements Serializable {
  public static final TypedValue NULL = new TypedValue(DataType.ANY, null);
  public static final TypedValue TRUE = new TypedValue(DataType.LOGICAL, Boolean.TRUE);
  public static final TypedValue FALSE = new TypedValue(DataType.LOGICAL, Boolean.FALSE);

  public static TypedValue of(DataType type, Object value) {
    if (value == null) {
      return NULL;
    }
    return new TypedValue(type, value);
  }

  public static TypedValue ofText(String text) {
    return text == null ? NULL : new TypedValue(DataType.TEXT, text);
  }

  public static TypedValue ofNumber(BigDecimal number) {
    return number == null ? NULL : new TypedValue(DataType.NUMBER, number);
  }

  public static TypedValue ofNumber(Number number) {
    if (number == null) return NULL;
    if (number instanceof BigDecimal bd) return ofNumber(bd);
    return ofNumber(new BigDecimal(number.toString()));
  }

  public static TypedValue ofLogical(Boolean logical) {
    if (logical == null) return NULL;
    return logical ? TRUE : FALSE;
  }

  public static TypedValue ofDate(Date date) {
    return date == null ? NULL : new TypedValue(DataType.DATETIME, date);
  }

  public static TypedValue ofArray(ArrayCallback callback) {
    return callback == null ? NULL : new TypedValue(DataType.ARRAY, callback);
  }

  public static TypedValue ofError(FormulaErrorValue error) {
    return new TypedValue(DataType.ERROR, error);
  }

  public static TypedValue ofError(int errorCode) {
    return ofError(new FormulaErrorValue(errorCode));
  }

  public DataType getType() {
    return type;
  }

  public Object getValue() {
    return value;
  }

  public boolean isError() {
    return type == DataType.ERROR || value instanceof FormulaErrorValue;
  }

  public boolean isNull() {
    return value == null;
  }
}
