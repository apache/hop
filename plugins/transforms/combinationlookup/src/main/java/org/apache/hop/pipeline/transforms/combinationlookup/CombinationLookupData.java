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

package org.apache.hop.pipeline.transforms.combinationlookup;

import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

import java.sql.PreparedStatement;
import java.util.Map;

/**
 * @author Matt
 * @since 24-jan-2005
 */
public class CombinationLookupData extends BaseTransformData implements ITransformData {
  public Database db;
  public int[] keynrs; // nrs in row of the keys

  public Map<RowMetaAndData, Long> cache;

  public IRowMeta outputRowMeta;
  public IRowMeta lookupRowMeta;
  public IRowMeta insertRowMeta;
  public IRowMeta hashRowMeta;
  public String realTableName;
  public String realSchemaName;
  public boolean[] removeField;

  public String schemaTable;

  public PreparedStatement prepStatementLookup;
  public PreparedStatement prepStatementInsert;
  public long smallestCacheKey;

  /**
   * Default Constructor
   */
  public CombinationLookupData() {
    super();
    db = null;
    realTableName = null;
    realSchemaName = null;
  }
}
