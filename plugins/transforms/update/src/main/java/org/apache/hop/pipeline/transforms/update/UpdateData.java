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

package org.apache.hop.pipeline.transforms.update;

import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

@SuppressWarnings("java:S1104")
public class UpdateData extends BaseTransformData implements ITransformData {
  public Database db;

  /**
   * Whether each row runs behind a savepoint: with an error hop on a database that rejects every
   * statement after a failed one until the transaction is rolled back (PostgreSQL).
   */
  public boolean useSavepoints;

  /**
   * Whether the statements go out in JDBC batches: asked for, no savepoint per row, a commit size
   * and a database that runs batches on this connection.
   */
  public boolean batchMode;

  /**
   * A row whose UPDATE is in the current JDBC batch. It stays here until the batch has run, so it
   * only goes to the output or the error hop once the database has said which it is.
   *
   * @param inputRow the row as it came in, for the error hop
   * @param outputRow the row for the output
   * @param bindValues the UPDATE parameters, to send the row again when the database never tried it
   */
  public record BatchedRow(Object[] inputRow, Object[] outputRow, Object[] bindValues) {}

  /** The rows in the current JDBC batch, in the order they were added to it. */
  public final List<BatchedRow> batchBuffer = new ArrayList<>();

  /** The lookup of the current row found no row to update, with an error hop to send it to. */
  public boolean keyNotFound;

  /** Rows written since the last commit, when the transform commits itself (with savepoints). */
  public int rowsSinceCommit;

  public int commitSize;

  public int[] keynrs; // nr of keylookup -value in row...
  public int[] keynrs2; // nr of keylookup2-value in row...
  public int[] valuenrs; // Stream valuename nrs to prevent searches.

  public String stringErrorKeyNotFound;

  public String stringFieldnames;

  public IRowMeta outputRowMeta;

  public String schemaTable;

  public PreparedStatement prepStatementLookup;
  public PreparedStatement prepStatementUpdate;

  public IRowMeta lookupParameterRowMeta;
  public IRowMeta lookupReturnRowMeta;
  public IRowMeta updateParameterRowMeta;

  public UpdateData() {
    super();

    db = null;
  }
}
