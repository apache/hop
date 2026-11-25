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
import lombok.Getter;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.batch.BatchSupport;
import org.apache.hop.core.database.batch.RecoverableBatch;
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
   * Whether the statements go out in JDBC batches the way Update always ran them: asked for, no
   * savepoint per row, a commit size and a database that runs batches on this connection. False
   * when {@link #recoveredBatches} is on.
   */
  public boolean batchMode;

  /** How a failed batch is handled on this connection, see {@link BatchSupport}. */
  public BatchSupport batchSupport;

  /**
   * True when the connection's batch failure contract has been validated: the statements go out in
   * {@link #batch}, which works out the rows of a failed batch by that contract. False keeps the
   * handling Update always had, with {@link #batchMode} and {@link #useSavepoints}.
   */
  public boolean recoveredBatches;

  /**
   * Row by row, with a commit size and auto-commit off: a row whose UPDATE ran waits in {@link
   * #batchBuffer}, and the rows that came in after it in {@link #pendingRows}, until the commit
   * that includes it; they are passed on after that commit, or dropped when the transaction is
   * rolled back. False on a connection shared by a transactional pipeline and with
   * HOP_DATABASE_BATCH_RECOVERY=LEGACY: every row is passed on right after its update.
   */
  public boolean holdUntilCommit;

  /**
   * With {@link #holdUntilCommit} and an error hop, on a database whose validated batch failure
   * contract says a failure loses the work of the transaction ({@code LOSES_WHOLE_BATCH}) and that
   * runs no savepoint per row (DuckDB): after a failed row the transaction is rolled back and the
   * updates of the rows that wait for the commit run again, so that only the failed row is lost.
   */
  public boolean runAgainAfterFailure;

  /**
   * With {@link #holdUntilCommit}: the row whose UPDATE the current row just ran, to queue once its
   * savepoint section (if any) is over; null when the row has no update.
   */
  public PendingRow updatedRow;

  /** The batch of {@link #recoveredBatches}, created with the UPDATE statement on the first row. */
  public RecoverableBatch batch;

  /** The UPDATE statement, to prepare it again when the driver closed it after a failed batch. */
  public String updateSql;

  /** The lookup SELECT, to prepare it again when the driver closed it after a failed lookup. */
  public String lookupSql;

  /**
   * How many rows of {@link #batchBuffer} the recovered batch has given a verdict while it runs.
   */
  public int settledRows;

  /**
   * A row that waits for the current JDBC batch, or row by row for the next commit, before it is
   * passed on.
   *
   * <p>A row with bind values is in the batch, or its update ran in the open transaction: it goes
   * to the output or the error hop once the database has said which. The other rows (nothing to
   * update, key not found, an error of their own) already know where they go, but wait as well, so
   * that rows leave in the order they came in.
   */
  @Getter
  public static final class PendingRow {
    /** The row as it came in, for the error hop. */
    private final Object[] inputRow;

    /** The row for the output, or null when it goes to the error hop. */
    private final Object[] outputRow;

    /** The UPDATE parameters of a row in the batch, to send it again; null for the other rows. */
    private final Object[] bindValues;

    private String errorMessage;
    private String errorFields;

    /** Set when the row goes to the error hop. */
    private String errorCode;

    private PendingRow(Object[] inputRow, Object[] outputRow, Object[] bindValues) {
      this.inputRow = inputRow;
      this.outputRow = outputRow;
      this.bindValues = bindValues;
    }

    /** A row whose UPDATE is in the batch. */
    public static PendingRow batched(Object[] inputRow, Object[] outputRow, Object[] bindValues) {
      return new PendingRow(inputRow, outputRow, bindValues);
    }

    /** A row for the output that isn't in the batch. */
    public static PendingRow output(Object[] outputRow) {
      return new PendingRow(null, outputRow, null);
    }

    /** A row for the error hop that isn't in the batch. */
    public static PendingRow error(
        Object[] inputRow, String errorMessage, String errorFields, String errorCode) {
      PendingRow row = new PendingRow(inputRow, null, null);
      row.fail(errorMessage, errorFields, errorCode);
      return row;
    }

    public boolean isBatched() {
      return bindValues != null;
    }

    public boolean isFailed() {
      return errorCode != null;
    }

    /** Sends the row to the error hop instead of the output. */
    public void fail(String errorMessage, String errorFields, String errorCode) {
      this.errorMessage = errorMessage;
      this.errorFields = errorFields;
      this.errorCode = errorCode;
    }
  }

  /**
   * The rows in the current JDBC batch, or row by row the rows whose update waits for the next
   * commit, in the order they were added.
   */
  public final List<PendingRow> batchBuffer = new ArrayList<>();

  /**
   * Every row that waits for the current batch or commit, in the order the rows came in: the rows
   * of {@link #batchBuffer} and the rows that arrived between them. Empty when nothing is pending.
   */
  public final List<PendingRow> pendingRows = new ArrayList<>();

  /** The lookup of the current row found no row to update, with an error hop to send it to. */
  public boolean keyNotFound;

  /** Rows written since the last commit, when the transform commits itself (with savepoints). */
  public int rowsSinceCommit;

  public int commitSize;

  public int[] keynrs; // nr of keylookup -value in row...
  public int[] keynrs2; // nr of keylookup2-value in row...
  public int[] valuenrs; // Stream valuename nrs to prevent searches.

  /** The error message of the current row when its key is not found, naming that key. */
  public String stringErrorKeyNotFound;

  /** The names of the lookup fields, for the error row of a key that is not found. */
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
