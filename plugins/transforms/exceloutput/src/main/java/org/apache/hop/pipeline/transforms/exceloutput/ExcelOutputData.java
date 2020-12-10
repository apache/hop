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

package org.apache.hop.pipeline.transforms.exceloutput;

import jxl.WorkbookSettings;
import jxl.format.Colour;
import jxl.write.WritableCellFormat;
import jxl.write.WritableFont;
import jxl.write.WritableImage;
import jxl.write.WritableSheet;
import jxl.write.WritableWorkbook;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

import java.io.OutputStream;
import java.util.Hashtable;
import java.util.Map;

/**
 * @author Matt
 * @since 7-sep-2006
 */
public class ExcelOutputData extends BaseTransformData implements ITransformData {
  public int splitnr;

  public IRowMeta previousMeta;
  public IRowMeta outputMeta;
  public int[] fieldnrs;

  public WritableWorkbook workbook;

  public WritableSheet sheet;

  public int templateColumns; // initial number of columns in the template

  public WritableCellFormat writableCellFormat;

  public Map<String, WritableCellFormat> formats;

  public int positionX;

  public int positionY;

  public WritableFont headerFont;

  public OutputStream outputStream;

  public FileObject file;

  public boolean oneFileOpened;

  public String realSheetname;

  int[] fieldsWidth;

  public boolean headerWrote;

  public int Headerrowheight;

  public String realHeaderImage;

  public Colour rowFontBackgoundColour;

  public WritableCellFormat headerCellFormat;

  public WritableImage headerImage;

  public double headerImageHeight;
  public double headerImageWidth;
  public WritableFont writableFont;

  public String realFilename;

  public WorkbookSettings ws;

  public ExcelOutputData() {
    super();

    formats = new Hashtable<>();
    oneFileOpened = false;
    file = null;
    realSheetname = null;
    headerWrote = false;
  }

}
