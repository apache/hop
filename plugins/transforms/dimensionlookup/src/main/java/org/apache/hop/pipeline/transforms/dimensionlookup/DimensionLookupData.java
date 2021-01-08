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

package org.apache.hop.pipeline.transforms.dimensionlookup;

import org.apache.hop.core.database.Database;
import org.apache.hop.core.hash.ByteArrayHashMap;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

import java.sql.PreparedStatement;
import java.util.Date;
import java.util.List;

/**
 * @author Matt
 * @since 24-jan-2005
 */
public class DimensionLookupData extends BaseTransformData implements ITransformData {
  public Date valueDateNow;

  public Database db;

  public Date minDate;
  public Date maxDate;

  public int[] keynrs; // nrs in row of the keys
  public int[] fieldnrs; // nrs in row of the fields
  public int datefieldnr; // Nr of datefield field in row

  public ByteArrayHashMap cache;

  public long smallestCacheKey;

  public Long notFoundTk;

  public IRowMeta outputRowMeta;

  public IRowMeta lookupRowMeta;
  public IRowMeta returnRowMeta;

  public PreparedStatement prepStatementLookup;
  public PreparedStatement prepStatementInsert;
  public PreparedStatement prepStatementUpdate;
  public PreparedStatement prepStatementDimensionUpdate;
  public PreparedStatement prepStatementPunchThrough;

  public IRowMeta insertRowMeta;
  public IRowMeta updateRowMeta;
  public IRowMeta dimensionUpdateRowMeta;
  public IRowMeta punchThroughRowMeta;

  public IRowMeta cacheKeyRowMeta;
  public IRowMeta cacheValueRowMeta;

  public String schemaTable;

  public String realTableName;
  public String realSchemaName;

  public int startDateChoice;

  public int startDateFieldIndex;

  public int[] preloadKeyIndexes;

  public int preloadFromDateIndex;
  public int preloadToDateIndex;

  public DimensionCache preloadCache;

  public List<Integer> preloadIndexes;

  public List<Integer> lazyList;

  /**
   * The input row metadata, but converted to normal storage type
   */
  public IRowMeta inputRowMeta;

  public DimensionLookupData() {
    super();

    db = null;
    valueDateNow = null;
    smallestCacheKey = -1;
    realTableName = null;
    realSchemaName = null;
  }

}
