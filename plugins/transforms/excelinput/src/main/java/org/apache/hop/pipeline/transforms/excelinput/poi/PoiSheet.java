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

import org.apache.hop.core.spreadsheet.IKCell;
import org.apache.hop.core.spreadsheet.IKSheet;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;

public class PoiSheet implements IKSheet {
  private Sheet sheet;

  public PoiSheet( Sheet sheet ) {
    this.sheet = sheet;
  }

  public String getName() {
    return sheet.getSheetName();
  }

  public IKCell[] getRow( int rownr ) {
    if ( rownr < sheet.getFirstRowNum() ) {
      return new IKCell[] {};
    } else if ( rownr > sheet.getLastRowNum() ) {
      throw new ArrayIndexOutOfBoundsException( "Read beyond last row: " + rownr );
    }
    Row row = sheet.getRow( rownr );
    if ( row == null ) { // read an empty row
      return new IKCell[] {};
    }
    int cols = row.getLastCellNum();
    if ( cols < 0 ) { // this happens if a row has no cells, POI returns -1 then
      return new IKCell[] {};
    }
    PoiCell[] xlsCells = new PoiCell[ cols ];
    for ( int i = 0; i < cols; i++ ) {
      Cell cell = row.getCell( i );
      if ( cell != null ) {
        xlsCells[ i ] = new PoiCell( cell );
      }
    }
    return xlsCells;
  }

  public int getRows() {
    return sheet.getLastRowNum() + 1;
  }

  public IKCell getCell( int colnr, int rownr ) {
    Row row = sheet.getRow( rownr );
    if ( row == null ) {
      return null;
    }
    Cell cell = row.getCell( colnr );
    if ( cell == null ) {
      return null;
    }
    return new PoiCell( cell );
  }
}
