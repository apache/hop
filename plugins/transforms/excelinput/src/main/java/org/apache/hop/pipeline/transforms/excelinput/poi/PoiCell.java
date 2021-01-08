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
import org.apache.hop.core.spreadsheet.KCellType;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;

import java.sql.Date;
import java.util.TimeZone;

public class PoiCell implements IKCell {

  private Cell cell;

  public PoiCell( Cell cell ) {
    this.cell = cell;
  }

  public KCellType getType() {
    // For POI version 4.1.2
    // switch ( cell.getCellType() ) {
      
    switch ( cell.getCellTypeEnum() ) {
      case BOOLEAN:
        return KCellType.BOOLEAN;
        
      case NUMERIC:
        if ( DateUtil.isCellDateFormatted( cell ) ) {
          return KCellType.DATE;
        }
        return KCellType.NUMBER;
              
      case STRING:
        return KCellType.LABEL;
        
      case BLANK:
      case ERROR:
        return KCellType.EMPTY;
      
      case FORMULA: 
       // For POI version 4.1.2
       // switch ( cell.getCachedFormulaResultType() ) {
        switch ( cell.getCachedFormulaResultTypeEnum() ) {
          case BLANK:
          case ERROR:
            return KCellType.EMPTY;
          case BOOLEAN:
            return KCellType.BOOLEAN_FORMULA;
          case STRING:
            return KCellType.STRING_FORMULA;
          case NUMERIC:
            if ( DateUtil.isCellDateFormatted( cell ) ) {
              return KCellType.DATE_FORMULA;
            } else {
              return KCellType.NUMBER_FORMULA;
            }
          default:
            break;
        }
      default:
        return null;
    }
        
  }

  public Object getValue() {
    try {
      switch ( getType() ) {
        case BOOLEAN_FORMULA:
        case BOOLEAN:
          return Boolean.valueOf( cell.getBooleanCellValue() );
        case DATE_FORMULA:
        case DATE:
          // Timezone conversion needed since POI doesn't support this apparently
          //
          long time = cell.getDateCellValue().getTime();
          long tzOffset = TimeZone.getDefault().getOffset( time );

          return new Date( time + tzOffset );
        case NUMBER_FORMULA:
        case NUMBER:
          return Double.valueOf( cell.getNumericCellValue() );
        case STRING_FORMULA:
        case LABEL:
          return cell.getStringCellValue();
        case EMPTY:
        default:
          return null;
      }
    } catch ( Exception e ) {
      throw new RuntimeException( "Unable to get value of cell ("
        + cell.getColumnIndex() + ", " + cell.getRowIndex() + ")", e );
    }
  }

  public String getContents() {
    try {
      Object value = getValue();
      if ( value == null ) {
        return null;
      }
      return value.toString();
    } catch ( Exception e ) {
      throw new RuntimeException( "Unable to get string content of cell ("
        + cell.getColumnIndex() + ", " + cell.getRowIndex() + ")", e );
    }
  }

  public int getRow() {
    Row row = cell.getRow();
    return row.getRowNum();
  }
}
