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

import lombok.Getter;

/** Stable error codes reported on the error hop of Database value validation. */
@Getter
public enum ColumnValueErrorCode {
  NULL_NOT_ALLOWED("DBV001"),
  STRING_TOO_LONG("DBV002"),
  INVALID_ENCODING("DBV003"),
  NUMERIC_OVERFLOW("DBV004"),
  CONVERSION("DBV005"),
  INTEGER_RANGE("DBV006"),
  INVALID_UUID("DBV007"),
  INVALID_JSON("DBV008");

  private final String code;

  ColumnValueErrorCode(String code) {
    this.code = code;
  }
}
