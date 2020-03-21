/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.databases.mssqlnative;

import org.apache.hop.core.Const;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.plugins.DatabaseMetaPlugin;
import org.apache.hop.core.util.Utils;
import org.apache.hop.databases.mssql.MSSQLServerDatabaseMeta;

@DatabaseMetaPlugin(
  type = "MSSQLNATIVE",
  typeDescription = "MS SQL Server (Native)"
)
@GuiPlugin( id = "GUI-MSSQLServerNativeDatabaseMeta" )
public class MSSQLServerNativeDatabaseMeta extends MSSQLServerDatabaseMeta {

  public static final String ATTRIBUTE_USE_INTEGRATED_SECURITY = "MSSQLUseIntegratedSecurity";

  @GuiWidgetElement(
    id = "usingIntegratedSecurity",
    order = "20",
    parentId = DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
    type = GuiElementType.CHECKBOX,
    i18nPackage = "org.apache.hop.ui.core.database",
    label = "DatabaseDialog.label.UseIntegratedSecurity"
  )
  private boolean usingIntegratedSecurity;

  @GuiWidgetElement(
    id = "usingDoubleDigit",
    order = "20",
    parentId = DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
    type = GuiElementType.CHECKBOX,
    i18nPackage = "org.apache.hop.ui.core.database",
    label = "DatabaseDialog.label.UseDoubleDecimalSeparator"
  )
  private boolean usingDoubleDigit;

  /**
   * Gets usingIntegratedSecurity
   *
   * @return value of usingIntegratedSecurity
   */
  public boolean isUsingIntegratedSecurity() {
    String flag = getAttributes().getProperty( ATTRIBUTE_USE_INTEGRATED_SECURITY );
    return "Y".equalsIgnoreCase( flag );
  }

  /**
   * @param usingIntegratedSecurity The usingIntegratedSecurity to set
   */
  public void setUsingIntegratedSecurity( boolean usingIntegratedSecurity ) {
    getAttributes().setProperty( ATTRIBUTE_USE_INTEGRATED_SECURITY, usingIntegratedSecurity ? "Y" : "N" );
  }

  /**
   * Gets usingDoubleDigit
   *
   * @return value of usingDoubleDigit
   */
  @Override
  public boolean isUsingDoubleDigit() {
    String flag = getAttributes().getProperty( ATTRIBUTE_MSSQL_DOUBLE_DECIMAL_SEPARATOR );
    return "Y".equalsIgnoreCase( flag );
  }

  /**
   * @param usingDoubleDigit The usingDoubleDigit to set
   */
  @Override
  public void setUsingDoubleDigit( boolean usingDoubleDigit ) {
    getAttributes().setProperty( ATTRIBUTE_MSSQL_DOUBLE_DECIMAL_SEPARATOR, usingDoubleDigit ? "Y" : "N" );
  }

  @Override
  public String getDriverClass() {
    return "com.microsoft.sqlserver.jdbc.SQLServerDriver";
  }

  @Override
  public String getURL( String hostname, String port, String databaseName ) {
    if ( getAccessType() == DatabaseMeta.TYPE_ACCESS_ODBC ) {
      return "jdbc:odbc:" + databaseName;
    } else {
      String useIntegratedSecurity = "false";
      Object value = getAttributes().get( ATTRIBUTE_USE_INTEGRATED_SECURITY );
      if ( value instanceof String ) {
        useIntegratedSecurity = (String) value;
        // Check if the String can be parsed into a boolean
        try {
          Boolean.parseBoolean( useIntegratedSecurity );
        } catch ( IllegalArgumentException e ) {
          useIntegratedSecurity = "false";
        }
      }

      String url = "jdbc:sqlserver://" + hostname;

      if ( !Utils.isEmpty( port ) && Const.toInt( port, -1 ) > 0 ) {
        url += ":" + port;
      }
      url += ";databaseName=" + databaseName + ";integratedSecurity=" + useIntegratedSecurity;

      return url;
    }
  }

  @Override
  public boolean supportsGetBlob() {
    return false;
  }

  @Override
  public boolean isMSSQLServerNativeVariant() {
    return true;
  }
}
