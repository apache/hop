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

package org.apache.hop.pipeline.transforms.pgbulkloader;

import org.apache.hop.core.database.Database;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.util.StreamLogger;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;
import org.postgresql.PGConnection;

import java.io.OutputStream;

/**
 * Stores data for the GPBulkLoader transform.
 *
 * @author Sven Boden
 * @since 20-feb-2005
 */
public class PGBulkLoaderData extends BaseTransformData implements ITransformData {
  public Database db;

  public int[] keynrs; // nr of keylookup -value in row...

  public StreamLogger errorLogger;

  public Process psqlProcess;

  public StreamLogger outputLogger;

  public OutputStream pgOutputStream;

  public byte[] quote;
  public byte[] separator;
  public byte[] newline;

  public PGConnection pgdb;

  public int[] dateFormatChoices;

  public IValueMeta dateMeta;
  public IValueMeta dateTimeMeta;

  /**
   * Default constructor.
   */
  public PGBulkLoaderData() {
    super();

    db = null;

    // Let's use ISO 8601 format. This in unambiguous with PostgreSQL
    dateMeta = new ValueMetaDate( "date" );
    dateMeta.setConversionMask( "yyyy-MM-dd" );

    dateTimeMeta = new ValueMetaDate( "date" );
    // Let's keep milliseconds. Didn't find a way to keep microseconds (max resolution with PG)
    dateTimeMeta.setConversionMask( "yyyy-MM-dd HH:mm:ss.SSS" );
  }
}
