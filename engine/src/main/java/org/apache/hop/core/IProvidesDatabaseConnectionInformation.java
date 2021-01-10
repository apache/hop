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

package org.apache.hop.core;

import org.apache.hop.core.database.DatabaseMeta;

/**
 * An interface for pipeline transforms that connect to a database table. For example a table output transform or a bulk
 * loader. This interface is used by the Agile BI plugin to determine which transforms it can model or visualize.
 *
 * @author jamesdixon
 */
public interface IProvidesDatabaseConnectionInformation {

  /**
   * Returns the database meta for this transform
   *
   * @return
   */
  DatabaseMeta getDatabaseMeta();

  /**
   * Returns the table name for this transform
   *
   * @return
   */
  String getTableName();

  /**
   * Returns the schema name for this transform.
   *
   * @return
   */
  String getSchemaName();

  /**
   * Provides a way for this object to return a custom message when database connection information is incomplete or
   * missing. If this returns {@code null} a default message will be displayed for missing information.
   *
   * @return A friendly message that describes that database connection information is missing and, potentially, why.
   */
  String getMissingDatabaseConnectionInformationMessage();

}
