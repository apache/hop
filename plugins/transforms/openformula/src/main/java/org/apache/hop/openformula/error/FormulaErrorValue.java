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

package org.apache.hop.openformula.error;

import java.io.Serializable;
import java.util.Locale;
import java.util.Objects;

/** Standard OpenFormula error values and error codes. */
public class FormulaErrorValue implements Serializable {
  private static final long serialVersionUID = 1L;

  public static final int ERROR_UNEXPECTED = 0;
  public static final int ERROR_ARGUMENTS = 1;
  public static final int ERROR_REFERENCE_NOT_RESOLVABLE = 499;
  public static final int ERROR_INVALID_CHARACTER = 501;
  public static final int ERROR_INVALID_ARGUMENT = 502;
  public static final int ERROR_ARITHMETIC = 503;
  public static final int ERROR_NOT_FOUND = 504;
  public static final int ERROR_INVALID_FUNCTION = 505;
  public static final int ERROR_MISSING_VARIABLE = 511;
  public static final int ERROR_NA = 522;
  public static final int ERROR_INVALID_AUTO_ARGUMENT = 666;
  public static final int ERROR_ILLEGAL_ARRAY = 667;

  public static final FormulaErrorValue ERROR_UNEXPECTED_VALUE =
      new FormulaErrorValue(ERROR_UNEXPECTED);
  public static final FormulaErrorValue ERROR_ARGUMENTS_VALUE =
      new FormulaErrorValue(ERROR_ARGUMENTS);
  public static final FormulaErrorValue ERROR_REFERENCE_NOT_RESOLVABLE_VALUE =
      new FormulaErrorValue(ERROR_REFERENCE_NOT_RESOLVABLE);
  public static final FormulaErrorValue ERROR_INVALID_CHARACTER_VALUE =
      new FormulaErrorValue(ERROR_INVALID_CHARACTER);
  public static final FormulaErrorValue ERROR_INVALID_ARGUMENT_VALUE =
      new FormulaErrorValue(ERROR_INVALID_ARGUMENT);
  public static final FormulaErrorValue ERROR_ARITHMETIC_VALUE =
      new FormulaErrorValue(ERROR_ARITHMETIC);
  public static final FormulaErrorValue ERROR_NOT_FOUND_VALUE =
      new FormulaErrorValue(ERROR_NOT_FOUND);
  public static final FormulaErrorValue ERROR_INVALID_FUNCTION_VALUE =
      new FormulaErrorValue(ERROR_INVALID_FUNCTION);
  public static final FormulaErrorValue ERROR_MISSING_VARIABLE_VALUE =
      new FormulaErrorValue(ERROR_MISSING_VARIABLE);
  public static final FormulaErrorValue ERROR_NA_VALUE = new FormulaErrorValue(ERROR_NA);
  public static final FormulaErrorValue ERROR_INVALID_AUTO_ARGUMENT_VALUE =
      new FormulaErrorValue(ERROR_INVALID_AUTO_ARGUMENT);
  public static final FormulaErrorValue ERROR_ILLEGAL_ARRAY_VALUE =
      new FormulaErrorValue(ERROR_ILLEGAL_ARRAY);

  private final int errorCode;

  public FormulaErrorValue(int errorCode) {
    this.errorCode = errorCode;
  }

  public int getErrorCode() {
    return errorCode;
  }

  public String getErrorMessage(Locale locale) {
    return switch (errorCode) {
      case ERROR_UNEXPECTED -> "Unexpected error";
      case ERROR_ARGUMENTS -> "Invalid number of arguments";
      case ERROR_REFERENCE_NOT_RESOLVABLE -> "Reference not resolvable";
      case ERROR_INVALID_CHARACTER -> "Invalid character";
      case ERROR_INVALID_ARGUMENT -> "Invalid argument";
      case ERROR_ARITHMETIC -> "Arithmetic error (e.g. division by zero)";
      case ERROR_NOT_FOUND -> "Value not found";
      case ERROR_INVALID_FUNCTION -> "Invalid function name";
      case ERROR_MISSING_VARIABLE -> "Missing variable / argument";
      case ERROR_NA -> "Value Not Available (#N/A)";
      case ERROR_INVALID_AUTO_ARGUMENT -> "Invalid auto argument";
      case ERROR_ILLEGAL_ARRAY -> "Illegal array dimensions or format";
      default -> "Error code " + errorCode;
    };
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof FormulaErrorValue other)) return false;
    return errorCode == other.errorCode;
  }

  @Override
  public int hashCode() {
    return Objects.hash(errorCode);
  }

  @Override
  public String toString() {
    return "FormulaErrorValue{errorCode="
        + errorCode
        + ", errorMessage="
        + getErrorMessage(Locale.getDefault())
        + "}";
  }
}
