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

package org.apache.hop.pipeline.transforms.insertupdate;

import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.Getter;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.batch.BatchSupport;
import org.apache.hop.core.database.batch.PendingKeys;
import org.apache.hop.core.database.batch.RecoverableBatch;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

/** Stores data for the Insert/Update transform. */
@SuppressWarnings("java:S1104")
public class InsertUpdateData extends BaseTransformData implements ITransformData {
  public Database db;

  /**
   * Whether each row runs behind a savepoint: with an error hop on a database that rejects every
   * statement after a failed one until the transaction is rolled back (PostgreSQL).
   */
  public boolean useSavepoints;

  /** Rows written since the last commit, when the transform commits itself (with savepoints). */
  public int rowsSinceCommit;

  public int commitSize;

  public int[] keynrs; // nr of keylookup -value in row...
  public int[] keynrs2; // nr of keylookup2-value in row...
  public int[] valuenrs; // Stream valuename nrs to prevent searches.

  public IRowMeta outputRowMeta;

  public String schemaTable;

  public PreparedStatement prepStatementLookup;
  public PreparedStatement prepStatementUpdate;

  public IRowMeta updateParameterRowMeta;
  public IRowMeta lookupParameterRowMeta;
  public IRowMeta lookupReturnRowMeta;
  public IRowMeta insertRowMeta;

  /** How a failed batch is handled on this connection, see {@link BatchSupport}. */
  public BatchSupport batchSupport;

  /**
   * True when "Use batch updates" is on, every key condition is an equality and the connection's
   * batch failure contract has been validated: the inserts go out in {@link #insertBatch} and the
   * updates in {@link #updateBatch}, which work out the rows of a failed batch by that contract.
   * False keeps the row by row handling Insert/Update always had, with {@link #useSavepoints}.
   */
  public boolean recoveredBatches;

  /** The INSERT batch of {@link #recoveredBatches}, created on the first row. */
  public RecoverableBatch insertBatch;

  /**
   * The UPDATE batch of {@link #recoveredBatches}, created on the first row; null when updates are
   * bypassed.
   */
  public RecoverableBatch updateBatch;

  /** The INSERT statement, to prepare it again when the driver closed it after a failed batch. */
  public String insertSql;

  /** The UPDATE statement, to prepare it again when the driver closed it after a failed batch. */
  public String updateSql;

  /** The lookup SELECT, to prepare it again when the driver closed it after a failed lookup. */
  public String lookupSql;

  /**
   * The fields of the input row whose values the key conditions compare, for the conditions that
   * compare a value (=, = ~NULL): the key a row is looked up by, see {@link PendingKeys}.
   */
  public int[] keyFieldIndexes;

  /**
   * For each of {@link #keyFieldIndexes}, the field the INSERT writes into that key column; null
   * when the INSERT leaves one of the key columns to the database.
   */
  public int[] insertKeyIndexes;

  /**
   * For each of {@link #keyFieldIndexes}, the field the UPDATE sets that key column to, or the key
   * field itself when the UPDATE leaves the column as it is.
   */
  public int[] updateKeyIndexes;

  /** The key of the current row, see {@link PendingKeys}. */
  public List<Object> rowKey;

  /** Whether the current row went into a batch, or is passed on now. */
  public boolean rowInBatch;

  /**
   * The keys the rows in the batches have before or after their write. A row with one of these keys
   * can't be looked up before the batches are written: the lookup can't see them.
   */
  public final Set<List<Object>> pendingKeys = new HashSet<>();

  /**
   * True when an INSERT waits in the batch whose key the transform can't tell, because the INSERT
   * leaves a key column to the database: every row then waits for the batches to be written.
   */
  public boolean pendingKeyUnknown;

  /**
   * A row that waits for the current batches before it is passed on.
   *
   * <p>A row of a batch goes to the output or the error hop once the database has said which. The
   * other rows (nothing to update, an error of their own) already know where they go, but wait as
   * well, so that rows leave in the order they came in.
   */
  @Getter
  public static final class PendingRow {

    /** What the row does to the table. */
    public enum Kind {
      INSERT,
      UPDATE,
      /** Not in a batch: the row needs no write, or failed on its own. */
      NONE
    }

    private final Kind kind;

    /**
     * The row as it came in: Insert/Update passes it on unchanged, or sends it to the error hop.
     */
    private final Object[] row;

    /** Set when the database committed the row's write. */
    private boolean written;

    private String errorMessage;
    private String errorFields;

    /** Set when the row goes to the error hop. */
    private String errorCode;

    private PendingRow(Kind kind, Object[] row) {
      this.kind = kind;
      this.row = row;
    }

    /** A row whose INSERT or UPDATE is in a batch. */
    public static PendingRow batched(Kind kind, Object[] row) {
      return new PendingRow(kind, row);
    }

    /** A row for the output that isn't in a batch. */
    public static PendingRow output(Object[] row) {
      return new PendingRow(Kind.NONE, row);
    }

    /** A row for the error hop that isn't in a batch. */
    public static PendingRow error(
        Object[] row, String errorMessage, String errorFields, String errorCode) {
      PendingRow pending = new PendingRow(Kind.NONE, row);
      pending.fail(errorMessage, errorFields, errorCode);
      return pending;
    }

    public boolean isBatched() {
      return kind != Kind.NONE;
    }

    public boolean isFailed() {
      return errorCode != null;
    }

    /** The database committed the row's write. */
    public void written() {
      written = true;
    }

    /** Sends the row to the error hop instead of the output. */
    public void fail(String errorMessage, String errorFields, String errorCode) {
      this.errorMessage = errorMessage;
      this.errorFields = errorFields;
      this.errorCode = errorCode;
    }
  }

  /** The rows in {@link #insertBatch}, in the order they were added. */
  public final List<PendingRow> insertBuffer = new ArrayList<>();

  /** The rows in {@link #updateBatch}, in the order they were added. */
  public final List<PendingRow> updateBuffer = new ArrayList<>();

  /**
   * Every row that waits for the current batches, in the order the rows came in: the rows of both
   * batches and the rows that arrived between them. Empty when nothing is pending.
   */
  public final List<PendingRow> pendingRows = new ArrayList<>();

  /** Default constructor. */
  public InsertUpdateData() {
    super();

    db = null;
  }
}
