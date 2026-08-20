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
package org.apache.hop.core.database.types;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;

/**
 * How a value of one column type crosses the JDBC boundary.
 *
 * <p>A binding belongs to whichever plugin owns the driver, because that is the only classloader in
 * which driver classes resolve. An external plugin that tries to unwrap, say, {@code
 * oracle.sql.STRUCT} from its own classloader gets "STRUCT cannot be cast to STRUCT"; the same code
 * inside the Oracle plugin, or in a plugin that joins its classLoaderGroup, works.
 *
 * <p>Implementations should exchange driver-neutral values (byte arrays, strings, numbers) so that
 * the value type on the other side needs no knowledge of any particular driver.
 */
public interface IValueBinding {

  /**
   * Reads the value of a column this binding's rule claimed.
   *
   * @param valueMeta the value metadata describing the column
   * @param database the dialect being read from
   * @param resultSet the result set to read from
   * @param index the 1-based column index
   * @return the value, or null
   */
  Object read(IDatabase database, IValueMeta valueMeta, ResultSet resultSet, int index)
      throws SQLException;

  /**
   * Writes a value into a prepared statement.
   *
   * @param valueMeta the value metadata describing the column
   * @param database the dialect being written to
   * @param preparedStatement the statement to write into
   * @param index the 1-based parameter index
   * @param value the value to write, possibly null
   */
  void write(
      IDatabase database,
      IValueMeta valueMeta,
      PreparedStatement preparedStatement,
      int index,
      Object value)
      throws SQLException, HopValueException;
}
