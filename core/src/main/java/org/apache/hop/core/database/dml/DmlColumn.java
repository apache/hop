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

package org.apache.hop.core.database.dml;

/**
 * A column of the target table that a DML statement writes or matches on.
 *
 * @param column the column name as it is in the table, unquoted
 * @param nativeType the column's type as the database names it, used for parameter casts; may be
 *     null when the dialect doesn't cast parameters
 * @param comparable whether the database can compare two values of the column with {@code <>},
 *     which "update only changed rows" needs; false for LOB, XML and similar types, see {@link
 *     org.apache.hop.core.database.IDatabase#isDmlComparable}
 */
public record DmlColumn(String column, String nativeType, boolean comparable) {

  /** A column with a native type, comparable. */
  public DmlColumn(String column, String nativeType) {
    this(column, nativeType, true);
  }

  /** A column without a native type, for dialects that don't cast their parameters. */
  public static DmlColumn of(String column) {
    return new DmlColumn(column, null, true);
  }
}
