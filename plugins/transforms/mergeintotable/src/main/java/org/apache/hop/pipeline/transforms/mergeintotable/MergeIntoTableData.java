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

package org.apache.hop.pipeline.transforms.mergeintotable;

import java.sql.PreparedStatement;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.batch.BatchSupport;
import org.apache.hop.core.database.batch.RecoverableBatch;
import org.apache.hop.core.database.dml.DmlPlan;
import org.apache.hop.core.database.dml.DmlStatement;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

public class MergeIntoTableData extends BaseTransformData implements ITransformData {
  public Database db;

  public IRowMeta outputRowMeta;

  /** The values of one row in the order of the statement's parameters. */
  public IRowMeta bindRowMeta;

  /** For each parameter of the statement, the index of the stream field that supplies it. */
  public int[] bindIndexes;

  /** The chosen upsert form. */
  public DmlPlan plan;

  /** The upsert statement. */
  public DmlStatement statement;

  public PreparedStatement prepStatement;

  /** How a failed batch is recovered on this connection. */
  public BatchSupport batchSupport;

  /** True when the rows go through {@link #batch}. */
  public boolean batchMode;

  /** The pending rows in batch mode, created on the first row; null otherwise. */
  public RecoverableBatch batch;

  public int commitSize;

  /**
   * Row by row with an error hop on a database where a failed statement aborts the whole
   * transaction: a savepoint per row.
   */
  public boolean useSavepoints;

  /**
   * Row by row with an error hop on a connection without savepoints: every row is committed on its
   * own, so a failed row can be rolled back alone.
   */
  public boolean commitEveryRow;

  public int rowsSinceCommit;
}
