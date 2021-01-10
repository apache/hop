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

package org.apache.hop.pipeline.transforms.excelinput.poi;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.provider.local.LocalFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.spreadsheet.IKSheet;
import org.apache.hop.core.spreadsheet.IKWorkbook;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

public class PoiWorkbook implements IKWorkbook {

  private ILogChannel log;

  private Workbook workbook;
  private String filename;
  private String encoding;
  // for PDI-10251 we need direct access to streams
  private InputStream internalIS;
  private POIFSFileSystem poifs;
  private OPCPackage opcpkg;

  public PoiWorkbook( String filename, String encoding ) throws HopException {
    this.filename = filename;
    this.encoding = encoding;
    this.log = HopLogStore.getLogChannelFactory().create( this );
    try {
      FileObject fileObject = HopVfs.getFileObject( filename );
      if ( fileObject instanceof LocalFile ) {
        // This supposedly shaves off a little bit of memory usage by allowing POI to randomly access data in the file
        //
        String localFilename = HopVfs.getFilename( fileObject );
        File excelFile = new File( localFilename );
        try {
          poifs = new POIFSFileSystem( excelFile );
          workbook = org.apache.poi.ss.usermodel.WorkbookFactory.create( poifs );
        } catch ( Exception ofe ) {
          try {
            opcpkg = OPCPackage.open( excelFile );
            workbook = org.apache.poi.ss.usermodel.WorkbookFactory.create( opcpkg );
          } catch ( Exception ex ) {
            workbook = org.apache.poi.ss.usermodel.WorkbookFactory.create( excelFile );
          }
        }
      } else {
        internalIS = HopVfs.getInputStream( filename );
        workbook = org.apache.poi.ss.usermodel.WorkbookFactory.create( internalIS );
      }
    } catch ( Exception e ) {
      throw new HopException( e );
    }
  }

  public PoiWorkbook( InputStream inputStream, String encoding ) throws HopException {
    this.encoding = encoding;

    try {
      workbook = org.apache.poi.ss.usermodel.WorkbookFactory.create( inputStream );
    } catch ( Exception e ) {
      throw new HopException( e );
    }
  }

  public void close() {
    try {
      if ( internalIS != null ) {
        internalIS.close();
      }
      if ( poifs != null ) {
        poifs.close();
      }
      if ( opcpkg != null ) {
        //We should not save change in xls because it is input transform.
        opcpkg.revert();
      }
    } catch ( IOException ex ) {
      log.logError( "Could not close workbook", ex );
    }
  }

  @Override
  public IKSheet getSheet( String sheetName ) {
    Sheet sheet = workbook.getSheet( sheetName );
    if ( sheet == null ) {
      return null;
    }
    return new PoiSheet( sheet );
  }

  public String[] getSheetNames() {
    int nrSheets = workbook.getNumberOfSheets();
    String[] names = new String[ nrSheets ];
    for ( int i = 0; i < nrSheets; i++ ) {
      names[ i ] = workbook.getSheetName( i );
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
    return workbook.getNumberOfSheets();
  }

  public IKSheet getSheet( int sheetNr ) {
    Sheet sheet = workbook.getSheetAt( sheetNr );
    if ( sheet == null ) {
      return null;
    }
    return new PoiSheet( sheet );
  }

  public String getSheetName( int sheetNr ) {
    Sheet sheet = (Sheet) getSheet( sheetNr );
    if ( sheet == null ) {
      return null;
    }
    return sheet.getSheetName();
  }
}
