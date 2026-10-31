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

package org.apache.hop.pipeline.transforms.execsqlrow;

import java.sql.Savepoint;
import org.apache.hop.core.Result;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

@SuppressWarnings("java:S1104")
public class ExecSqlRowData extends BaseTransformData implements ITransformData {
  public Database db;
  public Result result;
  public int indexOfSqlFieldname;
  public IRowMeta outputRowMeta;

  /**
   * Whether each row's SQL runs behind a savepoint: with an error hop on a database that rejects
   * every statement after a failed one until the transaction is rolled back (PostgreSQL).
   */
  public boolean useSavepoints;

  public boolean releaseSavepoint;
  public Savepoint savepoint;

  public ExecSqlRowData() {
    super();
    db = null;
    result = null;
    indexOfSqlFieldname = -1;
  }
}
