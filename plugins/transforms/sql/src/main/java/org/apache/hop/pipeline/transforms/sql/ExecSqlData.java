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

package org.apache.hop.pipeline.transforms.sql;

import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Result;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.SqlQueryClassifier;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

@SuppressWarnings("java:S1104")
public class ExecSqlData extends BaseTransformData implements ITransformData {
  public Database db;
  public Result result;
  public int[] argumentIndexes;
  public List<Integer> markerPositions;
  public IRowMeta outputRowMeta;
  public String sql;
  public boolean isCanceled;
  public IRowMeta paramsMeta;

  /**
   * The statement kept open while the SQL text stays the same from one row to the next (single
   * statement, executed for each row).
   */
  public PreparedStatement preparedStatement;

  /** The SQL text, as generated for the row, that {@link #preparedStatement} was prepared for. */
  public String preparedSql;

  /** The prepared SQL without comments, used in error messages. */
  public String preparedCleanSql;

  /** Which counter the update count of {@link #preparedStatement} goes to. */
  public SqlQueryClassifier.AffectedRows preparedCountTarget;

  /** Whether {@link #preparedStatement} changes the layout of a table or view. */
  public boolean preparedSchemaChange;

  /** The SQL text executed for the previous row, without bind parameters. */
  public String previousSql;

  /** Rows per transaction when executed for each row; 0 means auto-commit, a row at a time. */
  public int commitSize;

  /** Rows executed in the current transaction. */
  public int rowsSinceCommit;

  /**
   * Output rows of the current transaction. They are passed on once it is committed, so a row is
   * never reported as done while it can still be rolled back.
   */
  public final List<Object[]> pendingRows = new ArrayList<>();

  /** The counts of the statements in the current transaction, added to the metrics on commit. */
  public long pendingLinesRead;

  public long pendingLinesOutput;
  public long pendingLinesUpdated;

  /**
   * Set a savepoint around each row, so a failing row that goes to the error hop does not abort the
   * whole transaction on databases like PostgreSQL.
   */
  public boolean useSavepoints;

  public ExecSqlData() {
    super();

    db = null;
    result = null;
    argumentIndexes = null;
    markerPositions = null;
    paramsMeta = null;
  }
}
