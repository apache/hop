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

package org.apache.hop.pipeline.transforms.delete;

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
public class DeleteData extends BaseTransformData implements ITransformData {
  public Database db;

  /**
   * Whether each row runs behind a savepoint: with an error hop on a database that rejects every
   * statement after a failed one until the transaction is rolled back (PostgreSQL).
   */
  public boolean useSavepoints;

  /**
   * Whether the statements go out in JDBC batches the way Delete always ran them: asked for, and no
   * savepoint per row. False when {@link #recoveredBatches} is on.
   */
  public boolean batchMode;

  /** How a failed batch is handled on this connection, see {@link BatchSupport}. */
  public BatchSupport batchSupport;

  /**
   * True when the connection's batch failure contract has been validated: the statements go out in
   * {@link #batch}, which works out the rows of a failed batch by that contract. False keeps the
   * handling Delete always had, with {@link #batchMode} and {@link #useSavepoints}.
   */
  public boolean recoveredBatches;

  /** The batch of {@link #recoveredBatches}, created with the DELETE statement on the first row. */
  public RecoverableBatch batch;

  /** The DELETE statement, to prepare it again when the driver closed it after a failed batch. */
  public String deleteSql;

  /**
   * How many rows of {@link #batchBuffer} the recovered batch has given a verdict while it runs.
   */
  public int settledRows;

  /**
   * A row that waits for the current recovered batch before it is passed on.
   *
   * <p>A batched row is in the batch: it goes to the output or the error hop once the database has
   * said which. A row with an error of its own (a key that could not be bound) already knows where
   * it goes, but waits as well, so that rows leave in the order they came in.
   */
  @Getter
  public static final class PendingRow {
    /** The row as it came in: Delete passes it on unchanged, to the output or the error hop. */
    private final Object[] row;

    /** Whether the row's DELETE is in the batch. */
    private final boolean batched;

    private String errorMessage;
    private String errorFields;

    /** Set when the row goes to the error hop. */
    private String errorCode;

    private PendingRow(Object[] row, boolean batched) {
      this.row = row;
      this.batched = batched;
    }

    /** A row whose DELETE is in the batch. */
    public static PendingRow batched(Object[] row) {
      return new PendingRow(row, true);
    }

    /** A row for the error hop that isn't in the batch. */
    public static PendingRow error(
        Object[] row, String errorMessage, String errorFields, String errorCode) {
      PendingRow pending = new PendingRow(row, false);
      pending.fail(errorMessage, errorFields, errorCode);
      return pending;
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

  /** The rows in the current recovered batch, in the order they were added. */
  public final List<PendingRow> batchBuffer = new ArrayList<>();

  /**
   * Every row that waits for the current recovered batch, in the order the rows came in: the rows
   * of {@link #batchBuffer} and the rows that arrived between them. Empty when nothing is pending.
   */
  public final List<PendingRow> pendingRows = new ArrayList<>();

  /** Rows written since the last commit, when the transform commits itself (with savepoints). */
  public int rowsSinceCommit;

  public int commitSize;

  public int[] keynrs; // nr of keylookup -value in row...
  public int[] keynrs2; // nr of keylookup2-value in row...

  public IRowMeta outputRowMeta;

  public String schemaTable;

  public IRowMeta deleteParameterRowMeta;

  public PreparedStatement prepStatementDelete;

  public DeleteData() {
    super();

    db = null;
  }
}
