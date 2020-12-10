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

package org.apache.hop.pipeline.transforms.excelinput.ods;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.spreadsheet.IKSheet;
import org.apache.hop.core.spreadsheet.IKWorkbook;
import org.apache.hop.core.vfs.HopVfs;
import org.odftoolkit.odfdom.doc.OdfDocument;
import org.odftoolkit.odfdom.doc.OdfSpreadsheetDocument;
import org.odftoolkit.odfdom.doc.table.OdfTable;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OdfWorkbook implements IKWorkbook {

  private String filename;
  private String encoding;
  private OdfDocument document;
  private Map<String, OdfSheet> openSheetsMap = new HashMap<>();

  public OdfWorkbook( String filename, String encoding ) throws HopException {
    this.filename = filename;
    this.encoding = encoding;

    try {
      document = OdfSpreadsheetDocument.loadDocument( HopVfs.getInputStream( filename ) );
    } catch ( Exception e ) {
      throw new HopException( e );
    }
  }

  public OdfWorkbook( InputStream inputStream, String encoding ) throws HopException {
    this.encoding = encoding;

    try {
      document = OdfSpreadsheetDocument.loadDocument( inputStream );
    } catch ( Exception e ) {
      throw new HopException( e );
    }
  }

  public void close() {
    if ( document != null ) {
      document.close();
    }
  }

  @Override
  public IKSheet getSheet( String sheetName ) {
    OdfSheet sheet = openSheetsMap.get( sheetName );
    if ( sheet == null ) {
      OdfTable table = document.getTableByName( sheetName );
      if ( table == null ) {
        return null;
      } else {
        sheet = new OdfSheet( table );
        openSheetsMap.put( sheetName, sheet );
      }
    }
    return sheet;
  }

  public String[] getSheetNames() {
    List<OdfTable> list = document.getTableList();
    int nrSheets = list.size();
    String[] names = new String[ nrSheets ];
    for ( int i = 0; i < nrSheets; i++ ) {
      names[ i ] = list.get( i ).getTableName();
    }
    return names;
  }

  public String getFilename() {
    return filename;
  }

  public String getEncoding() {
    return encoding;
  }

  public int getNumberOfSheets() {
    return document.getTableList().size();
  }

  public IKSheet getSheet( int sheetNr ) {
    OdfTable table = document.getTableList().get( sheetNr );
    if ( table == null ) {
      return null;
    }
    return new OdfSheet( table );
  }

  public String getSheetName( int sheetNr ) {
    OdfTable table = document.getTableList().get( sheetNr );
    if ( table == null ) {
      return null;
    }
    return table.getTableName();
  }
}
