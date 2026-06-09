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

package org.apache.hop.pipeline.transforms.vertica.bulkloader;

import java.io.IOException;
import java.io.PipedInputStream;
import java.util.List;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transforms.vertica.bulkloader.nativebinary.ColumnSpec;
import org.apache.hop.pipeline.transforms.vertica.bulkloader.nativebinary.StreamEncoder;

public class VerticaBulkLoaderData extends BaseTransformData implements ITransformData {
  protected Database db;
  protected DatabaseMeta databaseMeta;

  protected StreamEncoder encoder;

  protected int[] selectedRowFieldIndices;

  protected IRowMeta outputRowMeta;
  protected IRowMeta insertRowMeta;

  protected PipedInputStream pipedInputStream;

  protected volatile Thread workerThread;

  protected List<ColumnSpec> colSpecs;

  public VerticaBulkLoaderData() {
    super();

    db = null;
  }

  public IRowMeta getInsertRowMeta() {
    return insertRowMeta;
  }

  public void close() throws IOException {

    if (encoder != null) {
      encoder.close();
    }
  }
}
