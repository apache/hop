/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.snowflake.bulkloader;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.compress.CompressionOutputStream;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

@SuppressWarnings("java:S1104")
public class SnowflakeBulkLoaderData extends BaseTransformData implements ITransformData {

  // When the meta.splitSize is exceeded the file being written is closed and a new file is created.
  //  These new files
  // are called splits.  Every time a new file is created this is incremented so it will contain the
  // latest split number
  public int splitnr;

  // Maps table fields to the location of the corresponding field on the input stream.
  public Map<String, Integer> fieldnrs;

  // The database being used
  public Database db;
  public DatabaseMeta databaseMeta;

  // A list of table fields mapped to their data type.  String[0] is the field name, String[1] is
  // the Snowflake
  // data type
  public ArrayList<String[]> dbFields;

  // The number of rows output to temp files.  Incremented every time a new row is written.
  public int outputCount;

  // The output stream being used to write files
  public CompressionOutputStream out;

  public OutputStream writer;

  public OutputStream fos;

  // The metadata about the output row
  public IRowMeta outputRowMeta;

  // Byte arrays for constant characters put into output files.
  public byte[] binarySeparator;
  public byte[] binaryEnclosure;
  public byte[] escapeCharacters;
  public byte[] binaryNewline;

  public byte[] binaryNullValue;

  // Indicates that at least one file has been opened by the transform
  public boolean oneFileOpened;

  // A list of files that have been previous created by the transform
  public List<String> previouslyOpenedFiles;

  /** Sets the default values */
  public SnowflakeBulkLoaderData() {
    super();

    previouslyOpenedFiles = new ArrayList<>();

    oneFileOpened = false;
    outputCount = 0;

    dbFields = null;
    db = null;
  }

  List<String> getPreviouslyOpenedFiles() {
    return previouslyOpenedFiles;
  }
}
