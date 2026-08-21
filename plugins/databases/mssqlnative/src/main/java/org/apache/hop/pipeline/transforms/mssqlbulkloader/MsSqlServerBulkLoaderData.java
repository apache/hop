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

package org.apache.hop.pipeline.transforms.mssqlbulkloader;

import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

public class MsSqlServerBulkLoaderData extends BaseTransformData implements ITransformData {

  protected Database db;
  protected DatabaseMeta databaseMeta;

  /** The driver connection behind {@link #db}, which is what the bulk copy runs on. */
  protected Connection connection;

  protected SQLServerBulkCopy bulkCopy;

  protected IRowMeta outputRowMeta;

  /** Rows converted and waiting for the next {@code writeToServer} call. */
  protected List<Object[]> buffer;

  protected int batchSize;

  /** For source ordinal i (0-based here), the index of the field to read from the input row. */
  protected int[] streamIndexes;

  /** The value metadata of those input fields, cached so the row loop does no lookups. */
  protected IValueMeta[] streamValueMeta;

  /** The target columns, in the same order as {@link #streamIndexes}. */
  protected RowBufferBulkData.Column[] targetColumns;

  /** The quoted "[schema].[table]" the bulk copy writes into. */
  protected String schemaTable;

  public MsSqlServerBulkLoaderData() {
    super();
    buffer = new ArrayList<>();
  }
}
