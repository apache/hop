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

package org.apache.hop.databases.impala;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.BaseDatabaseMeta;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabaseMetaPlugin;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.metadata.api.HopMetadataProperty;

/** Contains Cloudera Impala specific information through static final members */
@DatabaseMetaPlugin(
    type = "CLOUDERA-IMPALA",
    typeDescription = "Cloudera Impala",
    image = "impala.svg",
    documentationUrl = "/database/databases/cloudera-impala")
@GuiPlugin(id = "GUI-ClouderaImpalaDatabaseMeta")
public class ImpalaDatabaseMeta extends BaseDatabaseMeta implements IDatabase {
  public static final String CONST_ALTER_TABLE = "ALTER TABLE ";
  private static final int VARCHAR_LIMIT = 65_535;

  @GuiWidgetElement(
      id = "impalaPrincipal",
      order = "10",
      parentId = DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::ImpalaDatabaseMeta.label.PrincipalName",
      toolTip = "i18n::ImpalaDatabaseMeta.toolTip.PrincipalName")
  @HopMetadataProperty(key = "principal")
  private String principalName;

  @Override
  public String getDriverClass() {
    return "com.cloudera.impala.jdbc.Driver";
  }

  @Override
  public int getDefaultDatabasePort() {
    return 21050;
  }

  public int[] getAccessTypeList() {
    return new int[] {DatabaseMeta.TYPE_ACCESS_NATIVE};
  }

  @Override
  public boolean isSupportsAutoInc() {
    return false;
  }

  @Override
  public int getMaxTextFieldLength() {
    return VARCHAR_LIMIT;
  }

  @Override
  public int getMaxVARCHARLength() {
    return VARCHAR_LIMIT;
  }

  @Override
  public boolean isRequiresName() {
    return false;
  }

  @Override
  public String getURL(String hostname, String port, String databaseName) {
    String url = "jdbc:impala://" + hostname + ":" + port + "/";
    if (StringUtils.isNotEmpty(databaseName)) {
      url += databaseName + "/";
    }

    if (StringUtils.isNotEmpty(principalName)) {
      // Kerberos auth: add host and port and the principal in the options
      //
      url += ";principal=" + principalName;
    }

    return url;
  }

  @Override
  public String getAddColumnStatement(
      String tableName,
      IValueMeta v,
      String tk,
      boolean useAutoIncrement,
      String pk,
      boolean semicolon) {
    return CONST_ALTER_TABLE
        + tableName
        + " ADD COLUMN "
        + getFieldDefinition(v, tk, pk, useAutoIncrement, true, false);
  }

  @Override
  public String getModifyColumnStatement(
      String tableName,
      IValueMeta v,
      String tk,
      boolean useAutoIncrement,
      String pk,
      boolean semicolon) {
    return CONST_ALTER_TABLE
        + tableName
        + " MODIFY "
        + getFieldDefinition(v, tk, pk, useAutoIncrement, true, false);
  }

  @Override
  public String getDropColumnStatement(
      String tableName,
      IValueMeta v,
      String tk,
      boolean useAutoIncrement,
      String pk,
      boolean semicolon) {
    return CONST_ALTER_TABLE + tableName + " DROP COLUMN " + v.getName();
  }

  @Override
  public String getFieldDefinition(
      IValueMeta v,
      String tk,
      String pk,
      boolean useAutoIncrement,
      boolean addFieldName,
      boolean addCR) {
    String fieldClause = "";

    String fieldName = v.getName();
    if (v.getLength() == DatabaseMeta.CLOB_LENGTH) {
      v.setLength(getMaxTextFieldLength());
    }
    int length = v.getLength();
    int precision = v.getPrecision();

    if (addFieldName) {
      fieldClause += fieldName + " ";
    }

    int type = v.getType();
    switch (type) {
      case IValueMeta.TYPE_DATE:
      case IValueMeta.TYPE_TIMESTAMP:
        fieldClause += "TIMESTAMP";
        break;
      case IValueMeta.TYPE_BOOLEAN:
        if (isSupportsBooleanDataType()) {
          fieldClause += "BOOLEAN";
        } else {
          fieldClause += "CHAR(1)";
        }
        break;

      case IValueMeta.TYPE_NUMBER, IValueMeta.TYPE_INTEGER, IValueMeta.TYPE_BIGNUMBER:
        if (fieldName.equalsIgnoreCase(tk)
            || // Technical key
            fieldName.equalsIgnoreCase(pk) // Primary key
        ) {
          fieldClause += "BIGINT NOT NULL PRIMARY KEY";
        } else {
          // Integer values...
          if (precision == 0) {
            if (length > 9) {
              if (length < 19) {
                // can hold signed values between -9223372036854775808 and 9223372036854775807
                // 18 significant digits
                fieldClause += "BIGINT";
              } else {
                fieldClause += "DECIMAL(" + length + ")";
              }
            } else {
              fieldClause += "INT";
            }
          } else {
            // Floating point values...
            if (length > 15) {
              fieldClause += "DECIMAL(" + length;
              if (precision > 0) {
                fieldClause += ", " + precision;
              }
              fieldClause += ")";
            } else {
              // A double-precision floating-point number is accurate to approximately 15 decimal
              // places.
              // The values are stored in 8 bytes, using IEEE 754 Double Precision Binary Floating
              // Point format
              //
              fieldClause += "DOUBLE";
            }
          }
        }
        break;
      case IValueMeta.TYPE_STRING:
        if (length > 0) {
          if (length == 1) {
            fieldClause += "CHAR(1)";
          } else if (length < 65536) {
            fieldClause += "VARCHAR(" + length + ")";
          } else {
            fieldClause += "VARCHAR(65535)";
          }
        } else {
          fieldClause += "VARCHAR(65535)";
        }
        break;
      case IValueMeta.TYPE_BINARY:
        fieldClause += "!! BINARY DATA TYPE ARE NOT SUPPORTED ON IMPALA !!";
        break;
      default:
        fieldClause += " !! UNKNOWN DATA TYPE FOR IMPALA !!";
        break;
    }

    if (addCR) {
      fieldClause += Const.CR;
    }

    return fieldClause;
  }

  public String getPrincipalName() {
    return principalName;
  }

  public void setPrincipalName(String principalName) {
    this.principalName = principalName;
  }

  @Override
  public void addDefaultOptions() {
    setSupportsBooleanDataType(true);
  }
}
