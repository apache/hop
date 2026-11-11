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

package org.apache.hop.core.database;

import java.util.List;
import org.apache.hop.core.Result;
import org.apache.hop.core.row.IRowMeta;

/**
 * What {@link Database#executeStatement(String, int)} found after running one statement.
 *
 * @param result the counts per verb, as {@link Database#execStatement(String)} reports them
 * @param updateCount the JDBC update count, {@code -1} when the statement returned rows
 * @param rowMeta the layout of the returned rows, {@code null} when there were none
 * @param rows the returned rows, {@code null} when the statement did not return a result set
 */
public record ExecutedStatement(
    Result result, int updateCount, IRowMeta rowMeta, List<Object[]> rows) {

  /**
   * @return {@code true} when the statement returned a result set, even an empty one
   */
  public boolean hasRows() {
    return rows != null;
  }
}
