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

package org.apache.hop.core.database;

import org.apache.hop.core.exception.HopException;

/**
 * The SQL of one row, run behind a savepoint by {@link Database#runInSavepoint(ISavepointWork)}.
 *
 * <p>The work runs only statements on the database. It must not pass rows on ({@code putRow},
 * {@code putError}) or commit: on a connection shared by a transactional pipeline the other
 * transforms wait while it runs, and a transform that waits for one of them would never return.
 *
 * @param <T> what the work returns
 */
@FunctionalInterface
public interface ISavepointWork<T> {
  T run() throws HopException;
}
