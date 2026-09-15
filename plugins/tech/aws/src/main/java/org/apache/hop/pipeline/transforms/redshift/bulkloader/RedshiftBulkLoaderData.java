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

package org.apache.hop.pipeline.transforms.redshift.bulkloader;

import java.io.OutputStream;
import java.util.ArrayList;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

@SuppressWarnings("java:S1104")
public class RedshiftBulkLoaderData extends BaseTransformData implements ITransformData {
  protected Database db;
  protected DatabaseMeta databaseMeta;

  protected IRowMeta outputRowMeta;
  protected IRowMeta insertRowMeta;

  // A list of table fields mapped to their data type.  String[0] is the field name, String[1] is
  // the Redshift
  // data type
  public ArrayList<String[]> dbFields;

  /**
   * Per selected column: the index of the corresponding field on the input row, or -1 when the
   * field is not present on the stream. Resolved once, when the first row comes in.
   */
  protected int[] streamFieldIndexes;

  /** Per selected column: the value meta used to render the value in the CSV file. */
  protected IValueMeta[] writeValueMeta;

  /**
   * Per selected column: the value meta of the field on the input row, but only when the value
   * needs to be converted before it is written. {@code null} means the value can be written as is.
   */
  protected IValueMeta[] sourceValueMeta;

  /**
   * The names of the columns the CSV file actually holds, in the order they were written. Only set
   * when this transform wrote the file itself.
   */
  protected String[] columnNames;

  /** Set as soon as a first row is read, so we know whether the stream was empty. */
  protected boolean rowsReceived;

  protected OutputStream writer;
  // Byte arrays for constant characters put into output files.
  public byte[] binarySeparator;
  public byte[] binaryEnclosure;
  public byte[] escapeCharacters;
  public byte[] binaryNewline;
  public byte[] binaryNullValue;

  public RedshiftBulkLoaderData() {
    super();

    db = null;
  }

  public IRowMeta getInsertRowMeta() {
    return insertRowMeta;
  }
}
