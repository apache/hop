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

package org.apache.hop.pipeline.transforms.mysqlbulkloader;

import java.io.OutputStream;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.StreamLogger;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

public class MySqlBulkLoaderData extends BaseTransformData implements ITransformData {
  public Database db;

  public int[] keynrs; // nr of keylookup -value in row...

  public StreamLogger errorLogger;

  public StreamLogger outputLogger;

  public byte[] quote;
  public byte[] separator;
  public byte[] newline;

  public IValueMeta bulkTimestampMeta;
  public IValueMeta bulkDateMeta;
  public IValueMeta bulkNumberMeta;
  protected String dbDescription;

  public String schemaTable;

  public String fifoFilename;

  public OutputStream fifoStream;

  public MySqlBulkLoader.SqlRunner sqlRunner;

  public IValueMeta[] bulkFormatMeta;

  public long bulkSize;

  /** Default constructor. */
  public MySqlBulkLoaderData() {
    super();

    db = null;
  }
}
