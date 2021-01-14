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

package org.apache.hop.pipeline.transforms.databaselookup;

import org.apache.hop.core.database.Database;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

/**
 * @author Matt
 * @since 24-jan-2005
 */
public class DatabaseLookupData extends BaseTransformData implements ITransformData {
  public ICache cache;
  public Database db;

  public Object[] nullif; // Not found: default values...
  public int[] keynrs; // nr of keylookup -value in row...
  public int[] keynrs2; // nr of keylookup2-value in row...
  public int[] keytypes; // Types of the desired database values

  public IRowMeta outputRowMeta;
  public IRowMeta lookupMeta;
  public IRowMeta returnMeta;
  public boolean isCanceled;
  public boolean allEquals;
  public int[] conditions;
  public boolean hasDBCondition;

  public DatabaseLookupData() {
    super();

    db = null;
  }

  /**
   * ICache for {@code DatabaseLookup} transform.
   */
  public interface ICache {
    /**
     * Returns the very first data row that matches all conditions or {@code null} if none has been found.
     * Note, cache should keep the order in which elements were put into it.
     *
     * @param lookupMeta meta object for dealing with {@code lookupRow}
     * @param lookupRow  tuple containing values for comparison
     * @return first matching data row or {@code null}
     * @throws HopException
     */
    Object[] getRowFromCache( IRowMeta lookupMeta, Object[] lookupRow) throws HopException;

    /**
     * Saved {@code add} as data row and {@code lookupRow} as a key for searching it.
     *
     * @param meta       transform's meta
     * @param lookupMeta {@code lookupRow}'s meta
     * @param lookupRow  tuple of keys
     * @param add        tuple of data
     */
    void storeRowInCache( DatabaseLookupMeta meta, IRowMeta lookupMeta, Object[] lookupRow, Object[] add);
  }
}
