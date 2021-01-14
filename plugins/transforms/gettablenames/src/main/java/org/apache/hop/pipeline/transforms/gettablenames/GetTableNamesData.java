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

package org.apache.hop.pipeline.transforms.gettablenames;

import org.apache.hop.core.database.Database;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

/**
 * @author Samatar
 * @since 03-Juin-2008
 */
public class GetTableNamesData extends BaseTransformData implements ITransformData {
  public Database db;
  public String realTableNameFieldName;
  public String realObjectTypeFieldName;
  public String realIsSystemObjectFieldName;
  public String realSqlCreationFieldName;
  public String realSchemaName;

  public IRowMeta outputRowMeta;
  public long rownr;
  public IRowMeta inputRowMeta;
  public int totalpreviousfields;
  public int indexOfSchemaField;

  public Object[] readrow;

  public GetTableNamesData() {
    super();
    db = null;
    realTableNameFieldName = null;
    realObjectTypeFieldName = null;
    realIsSystemObjectFieldName = null;
    realSqlCreationFieldName = null;
    rownr = 0;
    realSchemaName = null;
    totalpreviousfields = 0;
    readrow = null;
    indexOfSchemaField = -1;
  }

}
