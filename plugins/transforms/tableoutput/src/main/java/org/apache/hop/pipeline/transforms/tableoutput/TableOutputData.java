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

package org.apache.hop.pipeline.transforms.tableoutput;

import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

import java.sql.PreparedStatement;
import java.sql.Savepoint;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

/**
 * Storage class for table output transform.
 *
 * @author Matt
 * @since 24-jan-2005
 */
public class TableOutputData extends BaseTransformData implements ITransformData {
  public Database db;
  public int warnings;
  public String tableName;
  public int[] valuenrs; // Stream valuename nrs to prevent searches.

  /**
   * Mapping between the SQL and the actual prepared statement. Normally this is only one, but in case we have more then
   * one, it's convenient to have this.
   */
  public Map<String, PreparedStatement> preparedStatements;

  public int indexOfPartitioningField;

  /**
   * Cache of the data formatter object
   */
  public SimpleDateFormat dateFormater;

  /**
   * Use batch mode or not?
   */
  public boolean batchMode;
  public int indexOfTableNameField;

  public List<Object[]> batchBuffer;
  public boolean sendToErrorRow;
  public IRowMeta outputRowMeta;
  public IRowMeta insertRowMeta;
  public boolean useSafePoints;
  public Savepoint savepoint;
  public boolean releaseSavepoint;

  public DatabaseMeta databaseMeta;

  public Map<String, Integer> commitCounterMap;

  public int commitSize;

  public TableOutputData() {
    super();

    db = null;
    warnings = 0;
    tableName = null;

    preparedStatements = new Hashtable<>();

    indexOfPartitioningField = -1;
    indexOfTableNameField = -1;

    batchBuffer = new ArrayList<>();
    commitCounterMap = new HashMap<>();

    releaseSavepoint = true;
  }
}
