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

import lombok.Getter;

/**
 * The forms chosen to write one kind of DML statement on one connection and one {@link DmlSpec}:
 * the upsert form of an UPSERT ({@link DmlSupport#plan}), or the number of rows per INSERT ({@link
 * DmlSupport#planStatements}), or why the statement can't be written.
 */
@Getter
public final class DmlPlan {

  /** What the statements do. */
  private final DmlOperation operation;

  /**
   * The dialect's declaration; null when it declares nothing, which allows one row per INSERT only.
   */
  private final DmlSyntax syntax;

  /** The chosen upsert form; null for an INSERT or when the connection can't write the upsert. */
  private final DmlSyntax.Upsert upsert;

  /** How many rows one statement carries, at least 1. */
  private final int rowsPerStatement;

  /**
   * The most bytes one statement of several rows may take on the connection, values included; 0
   * when the database has no limit that matters, see {@link
   * org.apache.hop.core.database.IDatabase#getMaxDmlStatementBytes}.
   */
  private final long maxStatementBytes;

  /** Why the statement can't be written on this connection; null when it can. */
  private final String unsupportedReason;

  /**
   * Why fewer rows per statement are used than were asked for, for the log; null when the request
   * was granted.
   */
  private final String note;

  private DmlPlan(
      DmlOperation operation,
      DmlSyntax syntax,
      DmlSyntax.Upsert upsert,
      int rowsPerStatement,
      long maxStatementBytes,
      String unsupportedReason,
      String note) {
    this.operation = operation;
    this.syntax = syntax;
    this.upsert = upsert;
    this.rowsPerStatement = Math.max(1, rowsPerStatement);
    this.maxStatementBytes = Math.max(0, maxStatementBytes);
    this.unsupportedReason = unsupportedReason;
    this.note = note;
  }

  /** A plan that writes the given upsert form of the syntax, one row per statement. */
  public static DmlPlan of(DmlSyntax syntax, DmlSyntax.Upsert upsert) {
    if (syntax == null || upsert == null) {
      throw new IllegalArgumentException("A supported plan needs a syntax and an upsert form");
    }
    return new DmlPlan(DmlOperation.UPSERT, syntax, upsert, 1, 0, null, null);
  }

  /** A plan that says why the upsert can't be written. */
  public static DmlPlan unsupported(DmlSyntax syntax, String reason) {
    return new DmlPlan(DmlOperation.UPSERT, syntax, null, 1, 0, reason, null);
  }

  /** A plan that says why the operation can't be written. */
  public static DmlPlan unsupported(DmlOperation operation, DmlSyntax syntax, String reason) {
    return new DmlPlan(operation, syntax, null, 1, 0, reason, null);
  }

  /**
   * A plan of the operation with one row per statement.
   *
   * @param note why one row per statement is written although more were asked for; null for none
   */
  public static DmlPlan singleRow(DmlOperation operation, DmlSyntax syntax, String note) {
    return new DmlPlan(operation, syntax, null, 1, 0, null, note);
  }

  /**
   * A plan of the operation with several rows per statement.
   *
   * @param rowsPerStatement the rows per statement
   * @param note why fewer rows per statement are used than were asked for; null for none
   */
  public static DmlPlan multiRow(
      DmlOperation operation, DmlSyntax syntax, int rowsPerStatement, String note) {
    return multiRow(operation, syntax, rowsPerStatement, 0, note);
  }

  /**
   * A plan of the operation with several rows per statement, each statement kept within a size.
   *
   * @param rowsPerStatement the rows per statement
   * @param maxStatementBytes the most bytes one statement may take, 0 for no limit
   * @param note why fewer rows per statement are used than were asked for; null for none
   */
  public static DmlPlan multiRow(
      DmlOperation operation,
      DmlSyntax syntax,
      int rowsPerStatement,
      long maxStatementBytes,
      String note) {
    return new DmlPlan(operation, syntax, null, rowsPerStatement, maxStatementBytes, null, note);
  }

  public boolean isSupported() {
    return unsupportedReason == null;
  }
}
