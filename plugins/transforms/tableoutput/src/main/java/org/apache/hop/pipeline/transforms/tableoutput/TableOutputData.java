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

package org.apache.hop.pipeline.transforms.tableoutput;

import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.batch.BatchSupport;
import org.apache.hop.core.database.batch.IRowBatch;
import org.apache.hop.core.database.batch.RecoverableBatch;
import org.apache.hop.core.database.dml.DmlPlan;
import org.apache.hop.core.database.dml.DmlSpec;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

/** Storage class for table output transform. */
@SuppressWarnings("java:S1104")
public class TableOutputData extends BaseTransformData implements ITransformData {
  public Database db;
  public int warnings;
  public String tableName;
  public int[] valuenrs; // Stream valuename nrs to prevent searches.

  /**
   * Mapping between the SQL and the actual prepared statement. Normally this is only one, but in
   * case we have more then one, it's convenient to have this.
   */
  public Map<String, PreparedStatement> preparedStatements;

  public int indexOfPartitioningField;

  /** Cache of the data formatter object */
  public SimpleDateFormat dateFormater;

  /** Use batch mode or not? */
  public boolean batchMode;

  public int indexOfTableNameField;

  /**
   * Distinct table names seen in the per-row dynamic-table-name mode, emitted as lineage on
   * dispose. Only populated when lineage is enabled, and capped — see {@code
   * TableOutput.recordDynamicLineageTarget}.
   */
  public Set<String> dynamicTablesWritten = new LinkedHashSet<>();

  /** Whether the lineage target set hit its cap, so the warning is logged only once. */
  public boolean dynamicLineageTruncated;

  public List<Object[]> batchBuffer;

  /**
   * True while several rows per INSERT may still be used: recovered batches, more than one row per
   * statement asked for, and nothing at init that rules it out. Each table is then planned on its
   * first batched row, see {@link #insertPlans}.
   */
  public boolean multiRowCandidate;

  /**
   * The INSERT plan of each table, made on its first batched row: several rows per statement, or
   * null for a table that gets one row per statement.
   */
  public Map<String, DmlPlan> insertPlans = new HashMap<>();

  /** The INSERT of each table of the multi-row batches, made on its first row. */
  public Map<String, DmlSpec> insertSpecs = new HashMap<>();

  /**
   * The values bound to the prepared statement for each row in {@link #batchBuffer}, in the same
   * order. It is not the same array: with "specify database fields" the bound row is a projection
   * of the stream row, and with the table name in a field it is a copy with that column removed. A
   * batch that has to be re-driven after a failure needs the values that were actually bound.
   */
  public List<Object[]> batchBindBuffer;

  public boolean sendToErrorRow;
  public IRowMeta outputRowMeta;
  public IRowMeta insertRowMeta;
  public boolean useSafePoints;

  public DatabaseMeta databaseMeta;

  public Map<String, Integer> commitCounterMap;

  public int commitSize;

  /** How a failed batch is handled on this connection, see {@link BatchSupport}. */
  public BatchSupport batchSupport;

  /**
   * True when the connection's batch failure contract has been validated and batches are written
   * through {@link RecoverableBatch}; false keeps the batch handling Table output always had.
   */
  public boolean recoveredBatches;

  /**
   * A row on its way through a recovered batch: what happened to it once its batch has run, so it
   * can be passed on in input order. Rows of several tables (table name in a field, partitioning)
   * sit in different batches that are committed at different times.
   */
  public static final class PendingRow {
    public final Object[] row;
    public final IRowMeta rowMeta;
    public boolean settled;
    public boolean written;
    public String message;
    public String errorCode;

    public PendingRow(Object[] row, IRowMeta rowMeta) {
      this.row = row;
      this.rowMeta = rowMeta;
    }
  }

  /** Every row of the recovered batches, in input order, until it is passed on. */
  public final Deque<PendingRow> pendingRows = new ArrayDeque<>();

  /** The pending row of each output row in a recovered batch, by identity. */
  public final Map<Object[], PendingRow> pendingByRow = new IdentityHashMap<>();

  /** The recovered batch of each table, by table name, when {@link #recoveredBatches} is on. */
  public Map<String, IRowBatch> batches = new LinkedHashMap<>();

  public TableOutputData() {
    super();

    db = null;
    warnings = 0;
    tableName = null;

    preparedStatements = new Hashtable<>();

    indexOfPartitioningField = -1;
    indexOfTableNameField = -1;

    batchBuffer = new ArrayList<>();
    batchBindBuffer = new ArrayList<>();
    commitCounterMap = new HashMap<>();
  }
}
