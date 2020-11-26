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
package org.apache.hop.databases.cassandra.spi;

import java.util.List;
import java.util.Map;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;

/**
 * Interface to something that can represent a keyspace in Cassandra
 *
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 */
public interface Keyspace {

  /**
   * Set the connection for this keyspace to use
   *
   * @param conn the connection to use
   * @throws Exception if a problem occurs
   */
  void setConnection(Connection conn) throws Exception;

  /**
   * Get the current connection
   *
   * @return the current connection
   */
  Connection getConnection();

  /**
   * Set the current keyspace
   *
   * @param keyspaceName the name of the keyspace to use
   * @throws Exception if a problem occurs
   */
  void setKeyspace(String keyspaceName) throws Exception;

  /**
   * Set any special options for keyspace operations (e.g. timeouts etc.)
   *
   * @param options the options to use. Can be null.
   */
  void setOptions(Map<String, String> options);

  /**
   * Execute a CQL statement.
   *
   * @param cql the CQL to execute
   * @param compression the compression to use (GZIP or NONE)
   * @param consistencyLevel the consistency level to use
   * @param log log to write to (may be null)
   * @throws Exception if a problem occurs
   */
  void executeCQL(String cql, String compression, String consistencyLevel, ILogChannel log)
      throws Exception;

  /**
   * Create a keyspace.
   *
   * @param keyspaceName the name of the keyspace
   * @param options additional options (see http://www.datastax.com/docs/1.0/configuration
   *     /storage_configuration)
   * @param log log to write to (may be null)
   * @throws Exception if a problem occurs
   */
  void createKeyspace(String keyspaceName, Map<String, Object> options, ILogChannel log)
      throws Exception;

  /**
   * Get a list of the names of the tables in this keyspace
   *
   * @return a list of table names in the current keyspace
   * @throws Exception if a problem occurs
   */
  List<String> getTableNamesCQL3() throws Exception;

  /**
   * Check to see if the named table exists in the current keyspace
   *
   * @param tableName the table name to check
   * @return true if the named table exists in the current keyspace
   * @throws Exception if a problem occurs
   */
  boolean tableExists(String tableName) throws Exception;

  /**
   * Get meta data for the named table
   *
   * @param tableName the name of the table to get meta data for
   * @return the table meta data
   * @throws Exception if the named table does not exist in the keyspace or a problem occurs
   */
  ITableMetaData getTableMetaData(String tableName) throws Exception;

  /**
   * Create a table in the current keyspace.
   *
   * @param tableName the name of the table to create
   * @param rowMeta the incoming fields to base the table schema on
   * @param keyIndexes the index(es) of the incoming field(s) to use as the key
   * @param createTableWithClause any WITH clause to include when creating the table
   * @param log log to write to (may be null)
   * @return true if the table was created successfully
   * @throws Exception if a problem occurs
   */
  boolean createTable(
      String tableName,
      IRowMeta rowMeta,
      List<Integer> keyIndexes,
      String createTableWithClause,
      ILogChannel log)
      throws Exception;

  /**
   * Update the named table with any incoming fields that are not present in its schema already
   *
   * @param tableName the name of the table to update
   * @param rowMeta the incoming row meta data
   * @param keyIndexes the index(es) of the incoming field(s) that make up the key
   * @param log the log to write to (may be null)
   * @throws UnsupportedOperationException if the underlying driver does not support updating table
   *     schema information
   * @throws Exception if a problem occurs
   */
  void updateTableCQL3(
      String tableName, IRowMeta rowMeta, List<Integer> keyIndexes, ILogChannel log)
      throws Exception;

  /**
   * Truncate the named table.
   *
   * @param tableName the name of the table to truncate
   * @param log log to write to (may be null)
   * @throws UnsupportedOperationException if the underlying driver does not support truncating a
   *     table
   * @throws Exception if a problem occurs
   */
  void truncateTable(String tableName, ILogChannel log) throws Exception;

  /**
   * Get a concrete implementation of CQLRowHandler for the underlying driver
   *
   * @return a CQLRowHandler or null if the driver in question does not support CQL
   */
  CQLRowHandler getCQLRowHandler();
}
