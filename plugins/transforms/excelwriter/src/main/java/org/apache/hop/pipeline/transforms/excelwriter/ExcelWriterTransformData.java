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

package org.apache.hop.pipeline.transforms.excelwriter;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

public class ExcelWriterTransformData extends BaseTransformData implements ITransformData {

  public IRowMeta outputRowMeta;
  public int splitnr;
  public int datalines;
  public String realSheetname;
  public String realTemplateSheetName;
  public boolean firstFileOpened;
  public FileObject file;
  public int posX;
  public int posY;
  public Sheet sheet;
  public Workbook wb;
  public int[] fieldnrs;
  public IRowMeta inputRowMeta;
  public int[] commentfieldnrs;
  public int[] commentauthorfieldnrs;
  public int startingCol = 0;
  public int startingRow = 0;
  public boolean shiftExistingCells = false;
  public boolean createNewFile = false;
  public boolean createNewSheet = false;
  public String realTemplateFileName;
  public String realStartingCell;
  public String realPassword;
  public String realProtectedBy;
  public int[] linkfieldnrs;
  private CellStyle[] cellStyleCache;
  private CellStyle[] cellLinkStyleCache;

  public ExcelWriterTransformData() {
    super();
  }

  public void clearStyleCache( int nrFields ) {
    cellStyleCache = new CellStyle[ nrFields ];
    cellLinkStyleCache = new CellStyle[ nrFields ];
  }

  public void cacheStyle( int fieldNr, CellStyle style ) {
    cellStyleCache[ fieldNr ] = style;
  }

  public void cacheLinkStyle( int fieldNr, CellStyle style ) {
    cellLinkStyleCache[ fieldNr ] = style;
  }

  public CellStyle getCachedStyle( int fieldNr ) {
    return cellStyleCache[ fieldNr ];
  }

  public CellStyle getCachedLinkStyle( int fieldNr ) {
    return cellLinkStyleCache[ fieldNr ];
  }

}
