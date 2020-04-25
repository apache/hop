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

package org.apache.hop.databases.mssqlnative;

import org.apache.hop.core.Const;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabaseMetaPlugin;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.util.Utils;
import org.apache.hop.databases.mssql.MSSQLServerDatabaseMeta;
import org.apache.hop.metastore.persist.MetaStoreAttribute;

@DatabaseMetaPlugin(
  type = "MSSQLNATIVE",
  typeDescription = "MS SQL Server (Native)"
)
@GuiPlugin( id = "GUI-MSSQLServerNativeDatabaseMeta" )
public class MSSQLServerNativeDatabaseMeta extends MSSQLServerDatabaseMeta {

  @GuiWidgetElement(
	 id = "usingIntegratedSecurity",
	 order = "21",
	 parentId = DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
	 type = GuiElementType.CHECKBOX,
	 i18nPackage = "org.apache.hop.ui.core.database",
	 label = "DatabaseDialog.label.UseIntegratedSecurity"
  )
  @MetaStoreAttribute
  private boolean usingIntegratedSecurity;

  /**
   * Gets usingIntegratedSecurity
   *
   * @return value of usingIntegratedSecurity
   */
  public boolean isUsingIntegratedSecurity() {
	return this.usingIntegratedSecurity;
  }

  /**
   * @param usingIntegratedSecurity The usingIntegratedSecurity to set
   */
  public void setUsingIntegratedSecurity( boolean usingIntegratedSecurity ) {
	this.usingIntegratedSecurity = usingIntegratedSecurity;
  }

  @Override
  public String getDriverClass() {
    return "com.microsoft.sqlserver.jdbc.SQLServerDriver";
  }

  @Override  
  public String getURL( String serverName, String port, String databaseName ) {
    if ( getAccessType() == DatabaseMeta.TYPE_ACCESS_ODBC ) {
      return "jdbc:odbc:" + databaseName;
    } else {
      StringBuilder sb = new StringBuilder( "jdbc:sqlserver://" );
      
      sb.append(serverName);
      
      // When specifying the location of the SQL Server instance, one normally provides serverName\instanceName or serverName:portNumber
      // If both a portNumber and instanceName are used, the portNumber will take precedence and the instanceName will be ignored.                
      if ( !Utils.isEmpty( port ) && Const.toInt( port, -1 ) > 0 ) {
        sb.append( ':' );
        sb.append( port );
      }
      else if ( !Utils.isEmpty(this.getInstanceName()) ) {
          sb.append( '\\' );
          sb.append( this.getInstanceName() );        
      }
      
      if ( !Utils.isEmpty(databaseName) ) {
          sb.append( ";databaseName=" );
          sb.append( databaseName );   
      }
            
      if ( this.usingIntegratedSecurity ) {
          sb.append( ";integratedSecurity=" );
          sb.append( String.valueOf(this.usingIntegratedSecurity) );   
      }
      
      return sb.toString();
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
