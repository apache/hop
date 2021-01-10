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

import org.apache.hop.core.IRowSet;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.errorhandling.IStream;

import java.sql.ResultSet;

/**
 * @author Matt
 * @since 20-jan-2005
 */
public class TableInputData extends BaseTransformData implements ITransformData {
  public Object[] nextrow;
  public Object[] thisrow;
  public Database db;
  public ResultSet rs;
  public String lookupTransform;
  public IRowMeta rowMeta;
  public IRowSet rowSet;
  public boolean isCanceled;
  public IStream infoStream;

  public TableInputData() {
    super();

    db = null;
    thisrow = null;
    nextrow = null;
    rs = null;
    lookupTransform = null;
  }

}
