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
package org.apache.hop.databases.doris;

import org.apache.hop.core.Const;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabaseMetaPlugin;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.databases.mysql.MySqlDatabaseMeta;

@DatabaseMetaPlugin(
    type = "DORIS",
    typeDescription = "Apache Doris",
    image = "doris.svg",
    documentationUrl = "/database/databases/doris.html")
@GuiPlugin(id = "GUI-DorisDatabaseMeta")
public class DorisDatabaseMeta extends MySqlDatabaseMeta {

  @Override
  public String getFieldDefinition(
      IValueMeta v, String tk, String pk, boolean useAutoinc, boolean addFieldName, boolean addCR) {
    String retval = "";

    String fieldname = v.getName();
    if (v.getLength() == DatabaseMeta.CLOB_LENGTH) {
      v.setLength(getMaxTextFieldLength());
    }
    int length = v.getLength();
    int precision = v.getPrecision();

    if (addFieldName) {
      retval += fieldname + " ";
    }

    int type = v.getType();
    switch (type) {
      case IValueMeta.TYPE_TIMESTAMP, IValueMeta.TYPE_DATE:
        retval += "DATETIME";
        break;
      case IValueMeta.TYPE_BOOLEAN:
        if (isSupportsBooleanDataType()) {
          retval += "BOOLEAN";
        } else {
          retval += "CHAR(1)";
        }
        break;

      case IValueMeta.TYPE_NUMBER, IValueMeta.TYPE_INTEGER, IValueMeta.TYPE_BIGNUMBER:
        if (fieldname.equalsIgnoreCase(tk)
            || // Technical key
            fieldname.equalsIgnoreCase(pk) // Primary key
        ) {
          if (useAutoinc) {
            retval += "BIGINT AUTO_INCREMENT NOT NULL PRIMARY KEY";
          } else {
            retval += "BIGINT NOT NULL PRIMARY KEY";
          }
        } else {
          if (type == IValueMeta.TYPE_INTEGER) {
            // Integer values...
            if (length < 3) {
              retval += "TINYINT";
            } else if (length < 5) {
              retval += "SMALLINT";
            } else if (length < 10) {
              retval += "INT";
            } else if (length < 20) {
              retval += "BIGINT";
            } else {
              retval += "DECIMAL(" + length + ")";
            }
          } else if (type == IValueMeta.TYPE_BIGNUMBER) {
            // Fixed point value...
            if (length
                < 1) { // user configured no value for length. Use 16 digits, which is comparable to
              // mantissa 2^53 of IEEE 754 binary64 "double".
              length = 16;
            }
            if (precision
                < 1) { // user configured no value for precision. Use 16 digits, which is comparable
              // to IEEE 754 binary64 "double".
              precision = 16;
            }
            retval += "DECIMAL(" + length + "," + precision + ")";
          } else {
            // Floating point value with double precision...
            retval += "DOUBLE";
          }
        }
        break;
      case IValueMeta.TYPE_STRING:
        if (length > 0) {
          if (length == 1) {
            retval += "CHAR(1)";
          } else if (length < 65533) {
            retval += "VARCHAR(" + length + ")";
          } else {
            retval += "STRING";
          }
        } else {
          retval += "STRING";
        }
        break;
      case IValueMeta.TYPE_BINARY:
        retval += "BITMAP";
        break;
      default:
        retval += " UNKNOWN";
        break;
    }

    if (addCR) {
      retval += Const.CR;
    }

    return retval;
  }
}
