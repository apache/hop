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

/**
 * Author = Shailesh Ahuja
 */

package org.apache.hop.pipeline.transforms.excelinput.staxpoi;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.spreadsheet.IKSheet;
import org.apache.hop.core.spreadsheet.IKWorkbook;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.xssf.eventusermodel.XSSFReader;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Streaming reader for XLSX files.<br>
 * Does not open XLS.
 */
public class StaxPoiWorkbook implements IKWorkbook {

  private static final String RELATION_NS_URI = "http://schemas.openxmlformats.org/officeDocument/2006/relationships";

  private ILogChannel log;

  private XSSFReader reader;

  // maintain the mapping of the sheet name to its ID
  private Map<String, String> sheetNameIDMap;
  // sheet names in order
  private String[] sheetNames;

  // mapping of the sheet object with its ID/Name
  private Map<String, StaxPoiSheet> openSheetsMap;

  private OPCPackage opcpkg;

  protected StaxPoiWorkbook() {
    openSheetsMap = new HashMap<>();
    this.log = HopLogStore.getLogChannelFactory().create( this );
  }

  public StaxPoiWorkbook( String filename, String encoding ) throws HopException {
    this();
    try {
      opcpkg = OPCPackage.open( filename );
      openFile( opcpkg, encoding );
    } catch ( Exception e ) {
      throw new HopException( e );
    }
  }

  public StaxPoiWorkbook( InputStream inputStream, String encoding ) throws HopException {
    this();
    try {
      opcpkg = OPCPackage.open( inputStream );
      openFile( opcpkg, encoding );
    } catch ( Exception e ) {
      throw new HopException( e );
    }
  }

  private void openFile( OPCPackage pkg, String encoding ) throws HopException, IOException, XMLStreamException {
    InputStream workbookData = null;
    XMLStreamReader workbookReader = null;
    try {
      reader = new XSSFReader( pkg );
      sheetNameIDMap = new LinkedHashMap<>();
      workbookData = reader.getWorkbookData();
      XMLInputFactory factory = StaxUtil.safeXMLInputFactory();
      workbookReader = factory.createXMLStreamReader( workbookData );
      while ( workbookReader.hasNext() ) {
        if ( workbookReader.next() == XMLStreamConstants.START_ELEMENT
          && workbookReader.getLocalName().equals( "sheet" ) ) {
          String sheetName = workbookReader.getAttributeValue( null, "name" );
          String sheetID = workbookReader.getAttributeValue( RELATION_NS_URI, "id" );
          sheetNameIDMap.put( sheetName, sheetID );
        }
      }
      sheetNames = new String[ sheetNameIDMap.size() ];
      int i = 0;
      for ( String sheetName : sheetNameIDMap.keySet() ) {
        sheetNames[ i++ ] = sheetName;
      }
    } catch ( Exception e ) {
      throw new HopException( e );
    } finally {
      if ( workbookReader != null ) {
          workbookReader.close();
      }
      if ( workbookData != null ) {
          workbookData.close();
      }
    }
  }

  @Override
  /**
   * return the same sheet if it already is created otherwise instantiate a new one
   */
  public IKSheet getSheet( String sheetName ) {
    String sheetID = sheetNameIDMap.get( sheetName );
    if ( sheetID == null ) {
      return null;
    }
    StaxPoiSheet sheet = openSheetsMap.get( sheetID );

    if ( sheet == null ) {
      try {
        sheet = new StaxPoiSheet( reader, sheetName, sheetID );
        openSheetsMap.put( sheetID, sheet );
      } catch ( Exception e ) {
        log.logError( sheetName, e );
      }
    }
    return sheet;
  }

  @Override
  public String[] getSheetNames() {
    String[] sheets = new String[ sheetNameIDMap.size() ];
    return sheetNameIDMap.keySet().toArray( sheets );
  }

  @Override
  public void close() {
    // close all the sheets
    for ( StaxPoiSheet sheet : openSheetsMap.values() ) {
      try {
        sheet.close();
      } catch ( IOException e ) {
        log.logError( "Could not close workbook", e );
      } catch ( XMLStreamException e ) {
        log.logError( "Could not close xmlstream", e );
      }
    }
    if ( opcpkg != null ) {
      //We should not save change in xlsx because it is input transform.
      opcpkg.revert();
    }
  }

  @Override
  public int getNumberOfSheets() {
    return sheetNameIDMap.size();
  }

  @Override
  public IKSheet getSheet( int sheetNr ) {
    if ( sheetNr >= 0 && sheetNr < sheetNames.length ) {
      return getSheet( sheetNames[ sheetNr ] );
    }
    return null;
  }

  @Override
  public String getSheetName( int sheetNr ) {
    if ( sheetNr >= 0 && sheetNr < sheetNames.length ) {
      return sheetNames[ sheetNr ];
    }
    return null;
  }

}
