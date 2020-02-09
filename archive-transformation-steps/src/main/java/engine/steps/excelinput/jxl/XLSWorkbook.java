/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.trans.steps.excelinput.jxl;

import jxl.Sheet;
import jxl.Workbook;
import jxl.WorkbookSettings;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.spreadsheet.KSheet;
import org.apache.hop.core.spreadsheet.KWorkbook;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVFS;

import java.io.InputStream;

public class XLSWorkbook implements KWorkbook {

  private Workbook workbook;
  private String filename;
  private String encoding;

  public XLSWorkbook( String filename, String encoding ) throws HopException {
    this.filename = filename;
    this.encoding = encoding;

    WorkbookSettings ws = new WorkbookSettings();
    if ( !Utils.isEmpty( encoding ) ) {
      ws.setEncoding( encoding );
    }
    try {
      workbook = Workbook.getWorkbook( HopVFS.getInputStream( filename ), ws );
    } catch ( Exception e ) {
      throw new HopException( e );
    }
  }

  public XLSWorkbook( InputStream inputStream, String encoding ) throws HopException {
    this.encoding = encoding;

    WorkbookSettings ws = new WorkbookSettings();
    if ( !Utils.isEmpty( encoding ) ) {
      ws.setEncoding( encoding );
    }
    try {
      workbook = Workbook.getWorkbook( inputStream, ws );
    } catch ( Exception e ) {
      throw new HopException( e );
    }
  }

  public void close() {
    if ( workbook != null ) {
      workbook.close();
    }
  }

  @Override
  public KSheet getSheet( String sheetName ) {
    Sheet sheet = workbook.getSheet( sheetName );
    if ( sheet == null ) {
      return null;
    }
    return new XLSSheet( sheet );
  }

  public String[] getSheetNames() {
    return workbook.getSheetNames();
  }

  public String getFilename() {
    return filename;
  }

  public String getEncoding() {
    return encoding;
  }

  public int getNumberOfSheets() {
    return workbook.getNumberOfSheets();
  }

  public KSheet getSheet( int sheetNr ) {
    Sheet sheet = workbook.getSheet( sheetNr );
    if ( sheet == null ) {
      return null;
    }
    return new XLSSheet( sheet );
  }

  public String getSheetName( int sheetNr ) {
    Sheet sheet = workbook.getSheet( sheetNr );
    if ( sheet == null ) {
      return null;
    }
    return sheet.getName();
  }
}
