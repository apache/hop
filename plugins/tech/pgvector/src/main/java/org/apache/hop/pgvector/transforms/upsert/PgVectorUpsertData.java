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
import java.sql.Savepoint;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hop.core.database.Database;
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

  public boolean releaseSavepoint;

  public Savepoint savepoint;

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
