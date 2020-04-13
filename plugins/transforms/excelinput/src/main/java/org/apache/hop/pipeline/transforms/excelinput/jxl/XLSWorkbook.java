/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
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

package org.apache.hop.pipeline.transforms.excelinput.jxl;

import jxl.Sheet;
import jxl.Workbook;
import jxl.WorkbookSettings;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.spreadsheet.IKSheet;
import org.apache.hop.core.spreadsheet.IKWorkbook;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;

import java.io.InputStream;

public class XLSWorkbook implements IKWorkbook {

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
      workbook = Workbook.getWorkbook( HopVfs.getInputStream( filename ), ws );
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
  public IKSheet getSheet( String sheetName ) {
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

  public IKSheet getSheet( int sheetNr ) {
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
