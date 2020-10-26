/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

import jxl.Cell;
import jxl.Sheet;
import org.apache.hop.core.spreadsheet.IKCell;
import org.apache.hop.core.spreadsheet.IKSheet;

public class XLSSheet implements IKSheet {
  private Sheet sheet;

  public XLSSheet( Sheet sheet ) {
    this.sheet = sheet;
  }

  public String getName() {
    return sheet.getName();
  }

  public IKCell[] getRow( int rownr ) {
    Cell[] cells = sheet.getRow( rownr );
    XLSCell[] xlsCells = new XLSCell[ cells.length ];
    for ( int i = 0; i < cells.length; i++ ) {
      if ( cells[ i ] != null ) {
        xlsCells[ i ] = new XLSCell( cells[ i ] );
      }
    }
    return xlsCells;
  }

  public int getRows() {
    return sheet.getRows();
  }

  public IKCell getCell( int colnr, int rownr ) {
    Cell cell = sheet.getCell( colnr, rownr );
    if ( cell == null ) {
      return null;
    }
    return new XLSCell( cell );
  }
}
