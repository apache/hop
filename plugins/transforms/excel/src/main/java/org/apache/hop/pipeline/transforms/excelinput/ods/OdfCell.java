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

import java.sql.Date;
import java.util.TimeZone;
import org.apache.hop.core.spreadsheet.IKCell;
import org.apache.hop.core.spreadsheet.KCellType;
import org.apache.hop.core.util.Utils;
import org.odftoolkit.odfdom.doc.table.OdfTableCell;

public class OdfCell implements IKCell {

  public static final String TYPE_BOOLEAN = "boolean";
  public static final String TYPE_CURRENCY = "currency";
  public static final String TYPE_DATE = "date";
  public static final String TYPE_FLOAT = "float";
  public static final String TYPE_PERCENTAGE = "percentage";
  public static final String TYPE_STRING = "string";
  public static final String TYPE_TIME = "time";

  private OdfTableCell cell;

  public OdfCell(OdfTableCell cell) {
    this.cell = cell;
  }

  @Override
  public KCellType getType() {

    String type = cell.getValueType();
    if (Utils.isEmpty(type)) {
      return KCellType.EMPTY;
    }

    switch (type) {
      case TYPE_BOOLEAN -> {
        if (Utils.isEmpty(cell.getFormula())) {
          return KCellType.BOOLEAN;
        } else {
          return KCellType.BOOLEAN_FORMULA;
        }
      }
      case TYPE_CURRENCY, TYPE_FLOAT, TYPE_PERCENTAGE -> {
        if (Utils.isEmpty(cell.getFormula())) {
          return KCellType.NUMBER;
        } else {
          return KCellType.NUMBER_FORMULA;
        }
      }
      case TYPE_DATE, TYPE_TIME -> {
        if (Utils.isEmpty(cell.getFormula())) {
          return KCellType.DATE;
        } else {
          return KCellType.DATE_FORMULA;
        } // Validate!
      }
      case TYPE_STRING -> {
        if (Utils.isEmpty(cell.getFormula())) {
          return KCellType.LABEL;
        } else {
          return KCellType.STRING_FORMULA;
        }
      }
    }

    // TODO: check what to do with a formula! Is the result cached or not with this format?

    return null; // unknown type!
  }

  @Override
  public Object getValue() {
    try {
      switch (getType()) {
        case BOOLEAN_FORMULA, BOOLEAN:
          return cell.getBooleanValue();
        case DATE_FORMULA, DATE:
          // Timezone conversion needed since POI doesn't support this apparently
          //
          long time = cell.getDateValue().getTime().getTime();
          long tzOffset = TimeZone.getDefault().getOffset(time);

          return new Date(time + tzOffset);
        case NUMBER_FORMULA, NUMBER:
          return Double.valueOf(cell.getDoubleValue());
        case STRING_FORMULA, LABEL:
          return cell.getStringValue();
        case EMPTY:
        default:
          return null;
      }
    } catch (Exception e) {
      throw new RuntimeException(
          "Unable to get value of cell (" + cell.getColumnIndex() + ", " + cell.getRowIndex() + ")",
          e);
    }
  }

  @Override
  public String getContents() {
    try {
      Object value = getValue();
      if (value == null) {
        return null;
      }
      return value.toString();
    } catch (Exception e) {
      throw new RuntimeException(
          "Unable to get string content of cell ("
              + cell.getColumnIndex()
              + ", "
              + cell.getRowIndex()
              + ")",
          e);
    }
  }

  @Override
  public int getRow() {
    return cell.getRowIndex();
  }
}
