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

import org.apache.hop.core.exception.HopException;

/**
 * Receives the verdict for each row of a flushed batch. Every row added to a {@link
 * RecoverableBatch} reaches exactly one of these methods, once, after the database has committed or
 * refused it, and in the order the rows were added.
 */
public interface IBatchRowListener {

  /**
   * The row is committed to the database.
   *
   * @param outputRow the output row that was added with it
   */
  void rowWritten(Object[] outputRow) throws HopException;

  /**
   * The row is committed to the database, with the update count the driver gave for it. This is the
   * method the batch and the row window call; the default passes the row on to {@link
   * #rowWritten(Object[])}, so a listener that has no use for the count only implements that one.
   *
   * <p>The count is the driver's answer for the execution that put the row in the database: for a
   * row that ran in a batch its entry in the update counts of that batch, which may be {@link
   * java.sql.Statement#SUCCESS_NO_INFO} (-2) when the driver doesn't count per row (Oracle, and
   * drivers that rewrite a batch into multi-row statements); for a row that ran on its own the
   * result of {@link java.sql.PreparedStatement#executeUpdate()}. When a row is run more than once
   * during a recovery, the count is the one of the run that was committed. Zero is a success too:
   * an UPDATE or DELETE whose key matches nothing.
   *
   * @param outputRow the output row that was added with it
   * @param updateCount the update count of the row, or {@link java.sql.Statement#SUCCESS_NO_INFO}
   */
  default void rowWritten(Object[] outputRow, int updateCount) throws HopException {
    rowWritten(outputRow);
  }

  /**
   * The row is not in the database.
   *
   * @param outputRow the output row that was added with it
   * @param message why, usually the database's own error message
   * @param attempted false when the database never ran this row and could not be asked again, for
   *     example on a connection without transactions; true when the database refused it
   */
  void rowRejected(Object[] outputRow, String message, boolean attempted) throws HopException;
}
