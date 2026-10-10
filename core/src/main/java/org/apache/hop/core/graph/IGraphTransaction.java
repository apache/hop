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

package org.apache.hop.core.graph;

import java.util.List;
import java.util.Map;
import org.apache.hop.core.exception.HopException;

/** A transaction on a graph database connection. Not thread-safe. */
public interface IGraphTransaction extends AutoCloseable {

  /**
   * Execute a statement in this transaction.
   *
   * @param statement The statement in the query language of the database
   * @param parameters The statement parameters, may be empty
   * @return The result rows, column name to value
   */
  List<Map<String, Object>> execute(String statement, Map<String, Object> parameters)
      throws HopException;

  void commit() throws HopException;

  void rollback() throws HopException;

  /** Close the transaction, rolling it back if it wasn't committed. */
  @Override
  void close() throws HopException;
}
