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

package org.apache.hop.pipeline.transforms.excelinput;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.playlist.IFilePlayList;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.spreadsheet.IKSheet;
import org.apache.hop.core.spreadsheet.IKWorkbook;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.errorhandling.IFileErrorHandler;

import java.util.Date;

/**
 * @author Matt
 * @since 24-jan-2005
 */
public class ExcelInputData extends BaseTransformData implements ITransformData {
  /**
   * The previous row in case we want to repeat values...
   */
  public Object[] previousRow;

  /**
   * The maximum length of all filenames...
   */
  public int maxfilelength;

  /**
   * The maximum length of all sheets...
   */
  public int maxsheetlength;

  /**
   * The Excel files to read
   */
  public FileInputList files;

  /**
   * The file number that's being handled...
   */
  public int filenr;

  public String filename;

  public FileObject file;

  /**
   * The openFile that's being processed...
   */
  public IKWorkbook workbook;

  /**
   * The sheet number that's being processed...
   */
  public int sheetnr;

  /**
   * The sheet that's being processed...
   */
  public IKSheet sheet;

  /**
   * The row where we left off the previous time...
   */
  public int rownr;

  /**
   * The column where we left off previous time...
   */
  public int colnr;

  /**
   * The error handler when processing of a row fails.
   */
  public IFileErrorHandler errorHandler;

  public IFilePlayList filePlayList;

  public IRowMeta outputRowMeta;

  IValueMeta valueMetaString;
  IValueMeta valueMetaNumber;
  IValueMeta valueMetaDate;
  IValueMeta valueMetaBoolean;

  public IRowMeta conversionRowMeta;

  public String[] sheetNames;
  public int[] startColumn;
  public int[] startRow;

  public int defaultStartColumn;
  public int defaultStartRow;

  public String shortFilename;
  public String path;
  public String extension;
  public boolean hidden;
  public Date lastModificationDateTime;
  public String uriName;
  public String rootUriName;
  public long size;

  public ExcelInputData() {
    super();
    workbook = null;
    filenr = 0;
    sheetnr = 0;
    rownr = -1;
    colnr = -1;

    valueMetaString = new ValueMetaString( "v" );
    valueMetaNumber = new ValueMetaNumber( "v" );
    valueMetaDate = new ValueMetaDate( "v" );
    valueMetaBoolean = new ValueMetaBoolean( "v" );
  }
}
