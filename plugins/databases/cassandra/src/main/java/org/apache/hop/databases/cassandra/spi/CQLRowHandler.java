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
package org.apache.hop.databases.cassandra.spi;

import java.util.Map;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.ITransform;

/**
 * Interface to something that can process rows (read and write) via CQL.
 *
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 */
public interface CQLRowHandler {
  /**
   * Set any special options for (e.g. timeouts etc)
   *
   * @param options the options to use. Can be null.
   */
  void setOptions(Map<String, String> options);

  /**
   * Set the specific underlying Keyspace implementation to use
   *
   * @param keyspace the keyspace to use
   */
  void setKeyspace(Keyspace keyspace);

  /**
   * Commit a batch CQL statement via the underlying driver
   *
   * @param requestingStep the step that is requesting the commit - clients can use this primarily
   *     to check whether the running transformation has been paused or stopped (via isPaused() and
   *     isStopped())
   * @param batch the CQL batch insert statement
   * @param compress name of the compression to use (may be null for no compression)
   * @param consistencyLevel the consistency level to use
   * @param log the log to use
   * @throws Exception if a problem occurs
   */
  void commitCQLBatch(
      ITransform requestingStep,
      StringBuilder batch,
      String compress,
      String consistencyLevel,
      ILogChannel log)
      throws Exception;

  /**
   * Executes a new CQL query and initializes ready for iteration over the results. Closes/discards
   * any previous query.
   *
   * @param requestingStep the step that is requesting rows - clients can use this primarily to
   *     check whether the running transformation has been paused or stopped (via isPaused() and
   *     isStopped())
   * @param tableName the name of the table to execute the query against
   * @param cqlQuery the CQL query to execute
   * @param compress the name of the compression to use (may be null for no compression)
   * @param consistencyLevel the consistency level to use
   * @param log the log to use
   * @throws Exception if a problem occurs
   */
  void newRowQuery(
      ITransform requestingStep,
      String tableName,
      String cqlQuery,
      String compress,
      String consistencyLevel,
      ILogChannel log)
      throws Exception;

  /**
   * Get the next output row(s) from the query. This might be a tuple row (i.e. a tuple representing
   * one column value from the current row) if tuple mode is activated. There might also be more
   * than one row returned if the query is CQL 3 and there is a collection that has been unwound.
   * Returns null when there are no more output rows to be produced from the query.
   *
   * @param outputRowMeta the Kettle output row structure
   * @param outputFormatMap map of field names to 0-based indexes in the outgoing row structure
   * @return the next output row(s) from the query
   * @throws Exception if a query hasn't been executed or another problem occurs.
   */
  Object[][] getNextOutputRow(IRowMeta outputRowMeta, Map<String, Integer> outputFormatMap)
      throws Exception;
}
