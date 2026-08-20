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

package org.apache.hop.pipeline.transforms.tableinput;

import java.sql.ResultSet;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.stream.IStream;

@SuppressWarnings("java:S1104")
public class TableInputData extends BaseTransformData implements ITransformData {
  public Object[] nextRow;
  public Object[] thisRow;
  public Database db;
  public ResultSet rs;
  public String lookupTransform;
  public IRowMeta rowMeta;
  public IRowSet rowSet;
  public boolean isCanceled;
  public IStream infoStream;

  /** JDBC result metadata before specified-field mapping, or null when specify-fields is off. */
  public IRowMeta jdbcRowMeta;

  /** JDBC column index for each specified output field, or null when specify-fields is off. */
  public int[] specifiedMapping;

  public TableInputData() {
    super();

    db = null;
    thisRow = null;
    nextRow = null;
    rs = null;
    lookupTransform = null;
  }
}
