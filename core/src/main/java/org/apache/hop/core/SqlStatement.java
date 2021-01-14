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
 * This class contains all that is needed to execute an SQL statement in a database. --> The Database connection --> The
 * SQL statement
 *
 * @author Matt
 * @since 02-dec-2004
 */
public class SqlStatement {
  private String transformName;
  private DatabaseMeta dbinfo;
  private String sql;
  private String error;

  /**
   * Creates a new SqlStatement
   *
   * @param dbinfo The database connection
   * @param sql    The sql to execute on the connection
   */
  public SqlStatement( String transformName, DatabaseMeta dbinfo, String sql ) {
    this.transformName = transformName;
    this.dbinfo = dbinfo;
    this.sql = sql;
    this.error = null;
  }

  /**
   * Set the name of the transform for which the SQL is intended
   *
   * @param transformName the name of the transform for which the SQL is intended
   */
  public void setTransformName( String transformName ) {
    this.transformName = transformName;
  }

  /**
   * Return the name of the transform for which the SQL is intended
   *
   * @return The name of the transform for which the SQL is intended
   */
  public String getTransformName() {
    return transformName;
  }

  /**
   * Sets the database connection for this SQL Statement
   *
   * @param dbinfo The databaseconnection
   */
  public void setDatabase( DatabaseMeta dbinfo ) {
    this.dbinfo = dbinfo;
  }

  /**
   * Sets the SQL to execute for this SQL Statement.
   *
   * @param sql The sql to execute, without trailing ";" or anything else.
   */
  public void setSql( String sql ) {
    this.sql = sql;
  }

  /**
   * Get the database connection for this SQL Statement
   *
   * @return The database connection for this SQL Statement
   */
  public DatabaseMeta getDatabase() {
    return dbinfo;
  }

  /**
   * Get the SQL for this SQL Statement
   *
   * @return The SQL to execute for this SQL Statement
   */
  public String getSql() {
    return sql;
  }

  /**
   * Sets the error that occurred when obtaining the SQL.
   *
   * @param error The error that occurred when obtaining the SQL.
   */
  public void setError( String error ) {
    this.error = error;
  }

  /**
   * Get the error that occurred when obtaining the SQL.
   *
   * @return the error that occurred when obtaining the SQL.
   */
  public String getError() {
    return error;
  }

  /**
   * Checks whether or not an error occurred obtaining the SQL.
   *
   * @return true if an error is set, false if no error is set.
   */
  public boolean hasError() {
    return error != null;
  }

  /**
   * Checks whether or not SQL statements are present
   *
   * @return true if changes are present, false if this is not the case.
   */
  public boolean hasSql() {
    return sql != null && sql.length() > 0;
  }
}
