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

package org.apache.hop.pipeline.transforms.synchronizeaftermerge;

import java.sql.PreparedStatement;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Getter;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.batch.BatchSupport;
import org.apache.hop.core.database.batch.PendingKeys;
import org.apache.hop.core.database.batch.RecoverableBatch;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

/** Performs an insert/update/delete depending on the value of a field. */
@SuppressWarnings("java:S1104")
public class SynchronizeAfterMergeData extends BaseTransformData implements ITransformData {
  public Database db;

  public int[] keyIndexes; // nr of keylookup -value in row...
  public int[] keyIndexes2; // nr of keylookup2-value in row...
  public int[] valueIndexes; // Stream valuename nrs to prevent searches.
  public int indexOfTableNameField;

  public int indexOfOperationOrderField;

  /**
   * Mapping between the SQL and the actual prepared statement. Normally this is only one, but in
   * case we have more than one, it's convenient to have this.
   */
  public Map<String, PreparedStatement> preparedStatements;

  public String realTableName;
  public String realSchemaName;
  public String realSchemaTable;

  /** Use batch mode or not? */
  public boolean batchMode;

  PreparedStatement insertStatement;
  PreparedStatement lookupStatement;
  PreparedStatement updateStatement;
  PreparedStatement deleteStatement;

  public String insertValue;
  public String updateValue;
  public String deleteValue;

  public String stringErrorKeyNotFound;

  public String stringFieldNames;

  public boolean lookupFailure;

  public IRowMeta outputRowMeta;
  public IRowMeta inputRowMeta;

  public IRowMeta deleteParameterRowMeta;
  public IRowMeta updateParameterRowMeta;
  public IRowMeta lookupParameterRowMeta;
  public IRowMeta lookupReturnRowMeta;
  public IRowMeta insertRowMeta;

  public Map<String, Integer> commitCounterMap;
  public int commitSize;
  public DatabaseMeta databaseMeta;
  public boolean specialErrorHandling;

  /**
   * Whether every row runs behind a savepoint: with an error hop on a database where a failed
   * statement aborts the transaction (PostgreSQL).
   */
  public boolean useSavepoints;

  public List<Object[]> batchBuffer;

  /** How a failed batch is handled on this connection, see {@link BatchSupport}. */
  public BatchSupport batchSupport;

  /**
   * True when the connection's batch failure contract has been validated: every INSERT, UPDATE and
   * DELETE goes out in a {@link RecoverableBatch} of its own, which works out the rows of a failed
   * batch by that contract. The batches run at the same moments, and so in the same order, as the
   * JDBC batches of {@link #batchMode}. False keeps the handling the transform always had, with
   * {@link #batchMode} and {@link #useSavepoints}.
   */
  public boolean recoveredBatches;

  /** The SQL of every prepared statement, by the key of {@link #preparedStatements}. */
  public final Map<String, String> statementSql = new HashMap<>();

  /**
   * The recovered batch of every INSERT, UPDATE and DELETE, by the key of {@link
   * #preparedStatements}: the table and the statement.
   */
  public final Map<String, RecoveredStatement> recoveredStatements = new HashMap<>();

  /**
   * Every row that waits to be passed on, in input order: the rows in a recovered batch, and the
   * rows behind them that already know where they go. Empty when nothing waits.
   */
  public final Deque<PendingRow> pendingRows = new ArrayDeque<>();

  /** How many rows of {@link #pendingRows} know where they go, but wait for a row in front. */
  public int settledPendingRows;

  /**
   * The rows added to any recovered batch since the database last ran one by its count: the count
   * {@link Database#insertRow(PreparedStatement, boolean)} keeps in batch mode, at which it runs
   * the batch of the row that makes it.
   */
  public int rowsAddedToBatches;

  /** The key of the recovered batch to run after the current row, or null. */
  public String batchToRun;

  /** The message of the current row's key that was not found, naming that key. */
  public String keyNotFoundMessage;

  /**
   * The fields of the input row whose values the = conditions compare: the key a row is looked up,
   * updated or deleted by, see {@link PendingKeys}. IS NULL and IS NOT NULL compare no value.
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

  /** Whether the UPDATE sets one of the key columns, and so gives a row another key. */
  public boolean updateWritesKey;

  /**
   * Whether every key condition is =, = ~NULL, IS NULL or IS NOT NULL. Otherwise (a range, LIKE,
   * BETWEEN) a statement can touch any row a batch of another statement waits to write.
   */
  public boolean keysAreEqualities;

  /** The INSERT, UPDATE or DELETE of one table, with its recovered batch. */
  public static final class RecoveredStatement {
    /** The key of {@link SynchronizeAfterMergeData#preparedStatements}. */
    final String key;

    /** The table, quoted as in the statement. */
    final String schemaTable;

    /** insert, update or delete. */
    final String statementName;

    final RecoverableBatch batch;

    /** The rows in the batch, in the order they were added. */
    final Deque<PendingRow> rows = new ArrayDeque<>();

    /**
     * The keys of the rows in the batch, see {@link PendingKeys}: the key a row is updated or
     * deleted by, the key an UPDATE gives it, the key an INSERT writes.
     */
    final Set<List<Object>> waitingKeys = new HashSet<>();

    /** True when an INSERT in the batch leaves a key column to the database: its key is unknown. */
    boolean unknownKey;

    RecoveredStatement(
        String key, String schemaTable, String statementName, RecoverableBatch batch) {
      this.key = key;
      this.schemaTable = schemaTable;
      this.statementName = statementName;
      this.batch = batch;
    }
  }

  /**
   * A row that waits before it is passed on, so that rows leave in the order they came in.
   *
   * <p>A row in a recovered batch goes to the output or to the error hop once the database has
   * committed or refused it. The other rows (skipped, or failed on their own) already know where
   * they go, but wait for the rows in front of them.
   */
  @Getter
  public static final class PendingRow {
    private final Object[] row;

    /**
     * The statement of a row in a batch, see SynchronizeAfterMerge.CONST_INSERT; null otherwise.
     */
    private final String statement;

    private boolean settled;
    private String errorMessage;
    private String errorFields;

    /** Set when the row goes to the error hop. */
    private String errorCode;

    private PendingRow(Object[] row, String statement) {
      this.row = row;
      this.statement = statement;
    }

    /** A row whose statement is in a batch. */
    public static PendingRow batched(Object[] row, String statement) {
      return new PendingRow(row, statement);
    }

    /** A row for the output that isn't in a batch. */
    public static PendingRow output(Object[] row) {
      PendingRow pending = new PendingRow(row, null);
      pending.settled = true;
      return pending;
    }

    /** A row for the error hop that isn't in a batch. */
    public static PendingRow error(
        Object[] row, String errorMessage, String errorFields, String errorCode) {
      PendingRow pending = new PendingRow(row, null);
      pending.fail(errorMessage, errorFields, errorCode);
      return pending;
    }

    public boolean isBatched() {
      return statement != null;
    }

    public boolean isFailed() {
      return errorCode != null;
    }

    /** The database committed the statement of the row. */
    public void written() {
      settled = true;
    }

    /** Sends the row to the error hop instead of the output. */
    public void fail(String errorMessage, String errorFields, String errorCode) {
      this.errorMessage = errorMessage;
      this.errorFields = errorFields;
      this.errorCode = errorCode;
      settled = true;
    }
  }

  /** Default constructor. */
  public SynchronizeAfterMergeData() {
    super();
    insertStatement = null;
    lookupStatement = null;
    updateStatement = null;
    deleteStatement = null;

    indexOfTableNameField = -1;

    db = null;
    preparedStatements = new Hashtable<>();
    realTableName = null;
    realSchemaName = null;
    batchMode = false;
    insertValue = null;
    updateValue = null;
    deleteValue = null;
    indexOfOperationOrderField = -1;
    lookupFailure = false;
    realSchemaTable = null;
    commitCounterMap = new HashMap<>();
    batchBuffer = new ArrayList<>();
  }
}
