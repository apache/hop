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

package org.apache.hop.databases.remedyarsystem;

import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.GenericDatabaseMeta;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.plugins.DatabaseMetaPlugin;

/**
 * Contains Database Connection information through static final members for a BMW Remedy Action Request System. These
 * connections are typically read-only ODBC only.
 *
 * @author Matt
 * @since 11-Sep-2007
 */
@DatabaseMetaPlugin(
  type = "REMEDY-AR-SYSTEM",
  typeDescription = "Remedy Action Request System"
)
@GuiPlugin( id = "GUI-RemedyActionRequestSystemDatabaseMeta" )
public class RemedyActionRequestSystemDatabaseMeta extends GenericDatabaseMeta implements IDatabase {
  @Override
  public int[] getAccessTypeList() {
    return new int[] { DatabaseMeta.TYPE_ACCESS_ODBC };
  }

  /**
   * @see IDatabase#getNotFoundTK(boolean)
   */
  @Override
  public int getNotFoundTK( boolean useAutoinc ) {
    return super.getNotFoundTK( useAutoinc );
  }

  @Override
  public String getDriverClass() {
    return "sun.jdbc.odbc.JdbcOdbcDriver"; // always ODBC!
  }

  @Override
  public String getURL( String hostname, String port, String databaseName ) {
    return "jdbc:odbc:" + databaseName;
  }

  /**
   * Checks whether or not the command setFetchSize() is supported by the JDBC driver...
   *
   * @return true is setFetchSize() is supported!
   */
  @Override
  public boolean isFetchSizeSupported() {
    return false;
  }

  /**
   * @return true if the database supports bitmap indexes
   */
  @Override
  public boolean supportsBitmapIndex() {
    return false;
  }

  /**
   * @return true if this database needs a transaction to perform a query (auto-commit turned off).
   */
  @Override
  public boolean isRequiringTransactionsOnQueries() {
    return false;
  }

  /**
   * The JDBC/ODBC Driver doesn't support listing views, so turn this feature off.
   */
  @Override
  public boolean supportsViews() {
    return false;
  }
}
