/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.pipeline.transforms.mysqlbulkloader;

import org.apache.hop.core.database.Database;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.StreamLogger;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

import java.io.OutputStream;

/**
 * Stores data for the MySQL bulk load transform.
 *
 * @author Matt
 * @since 14-apr-2009
 */
public class MySQLBulkLoaderData extends BaseTransformData implements ITransformData {
  public Database db;

  public int[] keynrs; // nr of keylookup -value in row...

  public StreamLogger errorLogger;

  public StreamLogger outputLogger;

  public byte[] quote;
  public byte[] separator;
  public byte[] newline;

  public IValueMeta bulkTimestampMeta;
  public IValueMeta bulkDateMeta;
  public IValueMeta bulkNumberMeta;
  protected String dbDescription;

  public String schemaTable;

  public String fifoFilename;

  public OutputStream fifoStream;

  public MySQLBulkLoader.SqlRunner sqlRunner;

  public IValueMeta[] bulkFormatMeta;

  public long bulkSize;

  /**
   * Default constructor.
   */
  public MySQLBulkLoaderData() {
    super();

    db = null;
  }
}
