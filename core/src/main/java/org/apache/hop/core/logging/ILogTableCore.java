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

package org.apache.hop.core.logging;

import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;

import java.util.List;

public interface ILogTableCore {

  String getConnectionName();

  void setConnectionName( String connectionName );

  DatabaseMeta getDatabaseMeta();

  List<LogTableField> getFields();

  /**
   * @return The locally defined log schema name
   */
  String getSchemaName();

  /**
   * @return The locally defined log table name
   */
  String getTableName();

  /**
   * @return The actual schema name taking into account optionally defined HOP variables for global logging
   * configuration
   */
  String getActualSchemaName();

  /**
   * @return The actual table name taking into account optionally defined HOP variabled for global logging
   * configuration
   */
  String getActualTableName();

  /**
   * Assemble the log record from the logging subject.
   *
   * @param status  The status to log
   * @param subject The subject object to log
   * @param parent  The parent object to log
   * @return The log record to write
   * @throws HopException case there is a problem with the log record creation (incorrect settings, ...)
   */
  RowMetaAndData getLogRecord( LogStatus status, Object subject, Object parent ) throws HopException;

  String getLogTableType();

  String getConnectionNameVariable();

  String getSchemaNameVariable();

  String getTableNameVariable();

  boolean isDefined();

  /**
   * @return The string that describes the timeout in days (variable supported) as a floating point number
   */
  String getTimeoutInDays();

  /**
   * @return the field that represents the log date field or null if none was defined.
   */
  LogTableField getLogDateField();

  /**
   * @return the field that represents the key to this logging table (batch id etc)
   */
  LogTableField getKeyField();

  /**
   * @return the appropriately quoted (by the database metadata) schema/table combination
   */
  String getQuotedSchemaTableCombination();

  /**
   * @return the field that represents the logging text (or null if none is found)
   */
  LogTableField getLogField();

  /**
   * @return the field that represents the status (or null if none is found)
   */
  LogTableField getStatusField();

  /**
   * @return the field that represents the number of errors (or null if none is found)
   */
  LogTableField getErrorsField();

  /**
   * @return the field that represents the name of the object that is being used (or null if none is found)
   */
  LogTableField getNameField();

  /**
   * @return A list of rows that contain the recommended indexed fields for this logging table.
   */
  List<IRowMeta> getRecommendedIndexes();

  /**
   * Clone the log table
   *
   * @return The cloned log table
   */
  Object clone();

  /**
   * Replace the metadata of the logtable with the one of the specified
   *
   * @param logTableInterface the new log table details
   */
  void replaceMeta( ILogTableCore logTableInterface );
}
