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

package org.apache.hop.core.database;

import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.row.IValueMeta;

public class NoneDatabaseMeta extends BaseDatabaseMeta implements IDatabase {
  @Override public String getFieldDefinition( IValueMeta v, String tk, String pk, boolean use_autoinc, boolean add_fieldname, boolean add_cr ) {
    return null;
  }

  @Override public int[] getAccessTypeList() {
    return new int[] { DatabaseMeta.TYPE_ACCESS_NATIVE };
  }

  @Override public String getDriverClass() {
    return "";
  }

  @Override public String getURL( String hostname, String port, String databaseName ) throws HopDatabaseException {
    return "jdbc://none";
  }

  @Override public String getAddColumnStatement( String tablename, IValueMeta v, String tk, boolean use_autoinc, String pk, boolean semicolon ) {
    return "";
  }

  @Override public String getModifyColumnStatement( String tablename, IValueMeta v, String tk, boolean use_autoinc, String pk, boolean semicolon ) {
    return "";
  }
}
