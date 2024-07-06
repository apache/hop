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

package org.apache.hop.pipeline.transforms.cratedbbulkloader;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

@SuppressWarnings("java:S1104")
public class CrateDBBulkLoaderData extends BaseTransformData implements ITransformData {
  protected Database db;
  protected DatabaseMeta databaseMeta;

  protected int[] selectedRowFieldIndices;

  protected IRowMeta outputRowMeta;
  protected IRowMeta insertRowMeta;
  protected IRowMeta convertedRowMeta;

  protected boolean convertedRowMetaReady = false;

  // A list of table fields mapped to their data type.  String[0] is the field name, String[1] is
  // the CrateDB
  // data type
  public ArrayList<String[]> dbFields;

  // Maps table fields to the location of the corresponding field on the input stream.
  public Map<String, Integer> fieldnrs;

  protected OutputStream writer;

  protected List<Object[]> httpBulkArgs = new ArrayList<>();
  // Byte arrays for constant characters put into output files.
  public byte[] binarySeparator;
  public byte[] binaryEnclosure;
  public byte[] escapeCharacters;
  public byte[] binaryNewline;
  public byte[] binaryNullValue;

  protected PipedInputStream pipedInputStream;

  protected volatile Thread workerThread;

  public CrateDBBulkLoaderData() {
    super();

    db = null;
  }

  public IRowMeta getInsertRowMeta() {
    return insertRowMeta;
  }

  public void setDatabaseMeta(DatabaseMeta databaseMeta) {
    this.databaseMeta = databaseMeta;
  }

  public void close() throws IOException {
    // Do nothing
  }
}
