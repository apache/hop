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
 *
 */

package org.apache.hop.pipeline.transforms.validator;

import java.io.Serial;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopValueException;

public class HopValidatorException extends HopValueException {

  public static final int ERROR_NONE = 0;
  public static final int ERROR_NULL_VALUE_NOT_ALLOWED = 1;
  public static final int ERROR_LONGER_THAN_MAXIMUM_LENGTH = 2;
  public static final int ERROR_SHORTER_THAN_MINIMUM_LENGTH = 3;
  public static final int ERROR_UNEXPECTED_DATA_TYPE = 4;
  public static final int ERROR_LOWER_THAN_ALLOWED_MINIMUM = 5;
  public static final int ERROR_HIGHER_THAN_ALLOWED_MAXIMUM = 6;
  public static final int ERROR_VALUE_NOT_IN_LIST = 7;
  public static final int ERROR_NON_NUMERIC_DATA = 8;
  public static final int ERROR_DOES_NOT_START_WITH_STRING = 9;
  public static final int ERROR_DOES_NOT_END_WITH_STRING = 10;
  public static final int ERROR_STARTS_WITH_STRING = 11;
  public static final int ERROR_ENDS_WITH_STRING = 12;
  public static final int ERROR_MATCHING_REGULAR_EXPRESSION_EXPECTED = 13;
  public static final int ERROR_MATCHING_REGULAR_EXPRESSION_NOT_ALLOWED = 14;
  public static final int ERROR_ONLY_NULL_VALUE_ALLOWED = 15;

  private static final String[] errorCode =
      new String[] {
        "KVD000", "KVD001", "KVD002", "KVD003", "KVD004", "KVD005", "KVD006", "KVD007", "KVD008",
        "KVD009", "KVD010", "KVD011", "KVD012", "KVD013", "KVD014", "KVD015",
      };

  private final Validator validator;
  private final Validation validatorField;
  private final int code;
  private final String fieldName;
  @Serial private static final long serialVersionUID = -212228277329271284L;

  /**
   * Constructs a new Throwable with the specified detail message.
   *
   * @param validator - the instance of Validator that this object will resolve variable
   *     substitution. The class is probably where this object is being created.
   * @param validatorField - the Validation in which the failure happened and this exception is to
   *     be created for.
   * @param code - the error code, see the static members of this class.
   * @param message - the detail message. The detail message is saved for later retrieval by the
   *     getMessage() method.
   * @param fieldName - the name of the field that failed Validation.
   */
  public HopValidatorException(
      Validator validator, Validation validatorField, int code, String message, String fieldName) {
    super(message);
    this.validator = validator;
    this.validatorField = validatorField;
    this.code = code;
    this.fieldName = fieldName;
  }

  /**
   * @return the code
   */
  public int getCode() {
    return code;
  }

  /**
   * @return the code in string format
   */
  public String getCodeDesc() {
    if (!StringUtils.isEmpty(validatorField.getErrorCode())) {
      return validator.resolve(validatorField.getErrorCode());
    }
    return errorCode[code];
  }

  @Override
  public String getMessage() {
    if (!StringUtils.isEmpty(validatorField.getErrorDescription())) {
      return validator.resolve(validatorField.getErrorDescription());
    }

    return super.getMessage();
  }

  /**
   * @return the fieldName
   */
  public String getFieldName() {
    return fieldName;
  }

  /**
   * @return the validatorField
   */
  public Validation getValidatorField() {
    return validatorField;
  }
}
