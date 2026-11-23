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

package org.apache.hop.core.database.batch;

import java.util.List;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;

/**
 * A batch of rows a transform writes through one statement: a {@link RecoverableBatch} on a one-row
 * statement, or a {@link MultiRowBatch} that writes several rows per statement. Both give every row
 * exactly one verdict, once, in the order the rows were added, through an {@link
 * IBatchRowListener}.
 */
public interface IRowBatch {

  /**
   * Adds a row. Nothing is sent to the database until {@link #flush}. A one-row batch binds the row
   * now and throws when a value can't be bound; a multi-row batch binds it at the flush and rejects
   * it there.
   *
   * @param bindRow the values of the statement's parameters
   * @param outputRow the row to report back once its fate is known
   */
  void add(Object[] bindRow, Object[] outputRow) throws HopDatabaseException;

  /**
   * @return the number of rows added since the last flush
   */
  int size();

  default boolean isEmpty() {
    return size() == 0;
  }

  /**
   * Sends and commits the pending rows, then reports every row to the listener. A failure is
   * recovered: only the rows the database refuses on their own are rejected.
   *
   * @param listener receives the verdict per row, or null to fail on the first error
   */
  void flush(IBatchRowListener listener) throws HopException;

  /**
   * Sends and commits the pending rows without recovering a failure: when it succeeds every row is
   * reported as written, when it fails the transaction is rolled back, nothing is reported and the
   * failure is thrown.
   *
   * @param listener receives the written rows, never a rejected one; may be null
   */
  void flushOrFail(IBatchRowListener listener) throws HopException;

  /**
   * Drops the rows that were added but not flushed, without running them.
   *
   * @return the output rows that were dropped, in the order they were added
   */
  List<Object[]> discard();

  /**
   * @return the most rows one statement carries: 1 for a one-row batch
   */
  int getRowsPerStatement();

  /**
   * @return how many round trips wrote rows
   */
  long getExecutions();

  /**
   * @return how many rows were rejected
   */
  long getRejectedRows();

  /**
   * @return how the driver behaves when a batch fails
   */
  BatchFailureContract getContract();

  /**
   * Closes the statements the batch prepared itself. The statement a one-row batch was created on
   * belongs to the caller and is left open.
   */
  default void close() {
    // Nothing of its own to close.
  }
}
