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
package org.apache.hop.pgvector.transforms.upsert;

import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.Getter;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.batch.BatchSupport;
import org.apache.hop.core.database.batch.RecoverableBatch;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pgvector.util.PgVectorTableColumn;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

public class PgVectorUpsertData extends BaseTransformData implements ITransformData {

  public IRowMeta inputRowMeta;
  public Database database;
  public PreparedStatement insertStatement;
  public PreparedStatement deleteStatement;

  /**
   * Rows added to the current JDBC batch, in batch order. They are only passed downstream once the
   * batch has been executed and committed, so downstream transforms never see a row as written
   * before it actually is.
   */
  public final List<Object[]> batchRows = new ArrayList<>();

  /** Resolved commit size, and whether JDBC batching is safe given the error-handling setting. */
  public int commitSize;

  public boolean batchMode;

  /**
   * PostgreSQL aborts the whole transaction on a failed statement, so a row can only be diverted to
   * an error hop if the transform can roll back to just before it. Table Output takes the same
   * route.
   */
  public boolean useSafePoints;

  /** How a failed batch is handled on this connection, see {@link BatchSupport}. */
  public BatchSupport batchSupport;

  /**
   * True with an error hop when the connection's batch failure contract has been validated: the
   * upserts go out in {@link #batch}, which works out the rows of a failed batch by that contract,
   * and no row runs behind a savepoint. False keeps the handling this transform always had, with
   * {@link #batchMode} and {@link #useSafePoints}.
   */
  public boolean recoveredBatches;

  /** The batch of {@link #recoveredBatches}, created with the upsert statement. */
  public RecoverableBatch batch;

  /**
   * How many rows of {@link #batchBuffer} the recovered batch has given a verdict while it runs.
   */
  public int settledRows;

  /**
   * The rows in the current recovered batch, in the order they were added. Each also waits in
   * {@link #pendingRows}.
   */
  public final List<PendingRow> batchBuffer = new ArrayList<>();

  /**
   * Every row that waits for the current recovered batch, in the order the rows came in: the rows
   * of {@link #batchBuffer} and the rows that arrived between them. Empty when nothing is pending.
   */
  public final List<PendingRow> pendingRows = new ArrayList<>();

  /**
   * A row that waits for the current recovered batch before it is passed on. A row in the batch
   * goes to the output or the error hop once the database has said which. The other rows (nothing
   * to store, a failure before the database) already know where they go, but wait as well, so that
   * rows leave in the order they came in.
   */
  @Getter
  public static final class PendingRow {
    /** The row as it came in: the output row and the error row alike. */
    private final Object[] row;

    private String errorMessage;

    /** Set when the row goes to the error hop. */
    private String errorCode;

    private PendingRow(Object[] row) {
      this.row = row;
    }

    /**
     * A row whose upsert is in the batch, or a row for the output that isn't: both go to the output
     * unless the batch {@link #fail fails} the row.
     */
    public static PendingRow output(Object[] row) {
      return new PendingRow(row);
    }

    /** A row for the error hop that isn't in the batch. */
    public static PendingRow error(Object[] row, String errorMessage, String errorCode) {
      PendingRow pending = new PendingRow(row);
      pending.fail(errorMessage, errorCode);
      return pending;
    }

    public boolean isFailed() {
      return errorCode != null;
    }

    /** Sends the row to the error hop instead of the output. */
    public void fail(String errorMessage, String errorCode) {
      this.errorMessage = errorMessage;
      this.errorCode = errorCode;
    }
  }

  /** Rows written since the last commit, used to honour the commit size outside batch mode. */
  public int rowsSinceCommit;

  public int idFieldIndex = -1;
  public int documentIdFieldIndex = -1;
  public int chunkIndexFieldIndex = -1;
  public int contentFieldIndex = -1;
  public int embeddingFieldIndex = -1;
  public List<PgVectorTableColumn> tableColumns = new ArrayList<>();
  public List<MappingBinding> mappingBindings = new ArrayList<>();

  /** Documents already deleted in this run, so the delete runs once per document. */
  public final Set<String> deletedDocuments = new HashSet<>();

  public static final class MappingBinding {
    public final int streamFieldIndex;
    public final int statementIndex;
    public final PgVectorTableColumn column;

    public MappingBinding(int streamFieldIndex, int statementIndex, PgVectorTableColumn column) {
      this.streamFieldIndex = streamFieldIndex;
      this.statementIndex = statementIndex;
      this.column = column;
    }
  }

  public PgVectorUpsertData() {
    super();
  }
}
