/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.databases.cratedb;

import org.apache.hop.core.database.DatabaseMetaPlugin;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.databases.postgresql.PostgreSqlDatabaseMeta;

@DatabaseMetaPlugin(
    type = "CRATEDB",
    typeDescription = "CrateDB",
    image = "cratedb.svg",
    documentationUrl = "/database/databases/cratedb.html",
    classLoaderGroup = "crate-db")
@GuiPlugin(id = "GUI-CrateDBDatabaseMeta")
public class CrateDBDatabaseMeta extends PostgreSqlDatabaseMeta {

  private static final String SEQUENCES_NOT_SUPPORTED = "CrateDB does not support sequences";

  @Override
  public String getDriverClass() {
    return "io.crate.client.jdbc.CrateDriver";
  }

  @Override
  public String getURL(String hostname, String port, String databaseName) {
    return "jdbc:crate://" + hostname + ":" + port + "/" + databaseName;
  }

  /**
   * As of the time of writing (Apr 2024), CrateDB does not natively support sequence generators
   * like the CREATE SEQUENCE command in PostgreSQL and some other SQL databases. CrateDB is built
   * to be distributed and massively scalable. Certain features found in traditional single-node SQL
   * databases, like sequence generators, are not directly translatable to a distributed system
   * because they can become bottlenecks in a distributed environment due to the need for
   * synchronization between nodes
   */
  @Override
  public boolean isSupportsSequences() {
    return false;
  }

  @Override
  public boolean isSupportsSequenceNoMaxValueOption() {
    return false;
  }

  /**
   * CrateDB doesn't have a direct equivalent of PostgreSQL's SERIAL data type due to its
   * distributed nature. But you can achieve a similar outcome by using generated columns that
   * produce a unique UUID. Since UUIDs cannot be sorted, they can't be incremented, as there's no
   * concept of ordering.
   */
  @Override
  public boolean isSupportsAutoInc() {
    return false;
  }

  @Override
  public String[] getReservedWords() {
    /**
     * This list of reserved words has been obtained as per docs
     * (https://cratedb.com/docs/crate/reference/en/5.6/sql/general/lexical-structure.html#reserved-words)
     * from the following query against CrateDB 5.6: SELECT word FROM pg_catalog.pg_get_keywords()
     * WHERE catcode = 'R' ORDER BY 1
     */
    return new String[] {
      "ADD",
      "ALL",
      "ALTER",
      "AND",
      "ANY",
      "ARRAY",
      "AS",
      "ASC",
      "BETWEEN",
      "BY",
      "CALLED",
      "CASE",
      "CAST",
      "COLUMN",
      "CONSTRAINT",
      "COSTS",
      "CREATE",
      "CROSS",
      "CURRENT_DATE",
      "CURRENT_SCHEMA",
      "CURRENT_TIME",
      "CURRENT_TIMESTAMP",
      "CURRENT_USER",
      "DEFAULT",
      "DELETE",
      "DENY",
      "DESC",
      "DESCRIBE",
      "DIRECTORY",
      "DISTINCT",
      "DROP",
      "ELSE",
      "END",
      "ESCAPE",
      "EXCEPT",
      "EXISTS",
      "EXTRACT",
      "FALSE",
      "FIRST",
      "FOR",
      "FROM",
      "FULL",
      "FUNCTION",
      "GRANT",
      "GROUP",
      "HAVING",
      "IF",
      "IN",
      "INDEX",
      "INNER",
      "INPUT",
      "INSERT",
      "INTERSECT",
      "INTO",
      "IS",
      "JOIN",
      "LAST",
      "LEFT",
      "LIKE",
      "LIMIT",
      "MATCH",
      "NATURAL",
      "NOT",
      "NULL",
      "NULLS",
      "OBJECT",
      "OFFSET",
      "ON",
      "OR",
      "ORDER",
      "OUTER",
      "PERSISTENT",
      "RECURSIVE",
      "RESET",
      "RETURNS",
      "REVOKE",
      "RIGHT",
      "SELECT",
      "SESSION_USER",
      "SET",
      "SOME",
      "STRATIFY",
      "TABLE",
      "THEN",
      "TRANSIENT",
      "TRUE",
      "TRY_CAST",
      "UNBOUNDED",
      "UNION",
      "UPDATE",
      "USER",
      "USING",
      "WHEN",
      "WHERE",
      "WITH"
    };
  }

  @Override
  public String getExtraOptionsHelpText() {
    return "https://cratedb.com/docs/jdbc/en/latest/connect.html#connection-properties";
  }

  @Override
  public String getSqlListOfSequences() {
    throw new UnsupportedOperationException(SEQUENCES_NOT_SUPPORTED);
  }

  @Override
  public String getSqlNextSequenceValue(String sequenceName) {
    throw new UnsupportedOperationException(SEQUENCES_NOT_SUPPORTED);
  }

  @Override
  public String getSqlCurrentSequenceValue(String sequenceName) {
    throw new UnsupportedOperationException(SEQUENCES_NOT_SUPPORTED);
  }

  @Override
  public String getSqlSequenceExists(String sequenceName) {
    throw new UnsupportedOperationException(SEQUENCES_NOT_SUPPORTED);
  }

  @Override
  public String getFieldDefinition(
      IValueMeta v, String tk, String pk, boolean useAutoinc, boolean addFieldName, boolean addCr) {

    int type = v.getType();
    switch (type) {
      case IValueMeta.TYPE_NUMBER, IValueMeta.TYPE_BIGNUMBER, IValueMeta.TYPE_INTEGER:
        String retval = "";
        String fieldname = v.getName();
        int length = v.getLength();

        if (addFieldName) {
          retval += fieldname + " ";
        }
        if (fieldname.equalsIgnoreCase(tk)
            || // Technical key
            fieldname.equalsIgnoreCase(pk) // Primary key
        ) {
          retval += "BIGSERIAL";
        } else {
          if (type == IValueMeta.TYPE_INTEGER) {
            // Integer values...
            if (length > 9) {
              retval += "BIGINT";
            } else if (length > 4) {
              retval += "INTEGER";
            } else {
              retval += "SMALLINT";
            }
          } else if (type == IValueMeta.TYPE_BIGNUMBER) {
            // Fixed point value...
            // CrateDB doesn't support NUMERIC type for columns (only in expressions...)
            // as a work-around we use a double, which can be cast to NUMERIC via SQL by the user
            retval += "DOUBLE PRECISION";
          } else {
            // Floating point value with double precision...
            retval += "DOUBLE PRECISION";
          }
        }
        return retval;

      default:
        return super.getFieldDefinition(v, tk, pk, useAutoinc, addFieldName, addCr);
    }
  }

  @Override
  public String getSqlLockTables(String[] tableNames) {
    throw new UnsupportedOperationException("CrateDB does not support locking tables");
  }

  @Override
  public String getSqlUnlockTables(String[] tableName) {
    throw new UnsupportedOperationException("CrateDB does not support locking tables");
  }
}
