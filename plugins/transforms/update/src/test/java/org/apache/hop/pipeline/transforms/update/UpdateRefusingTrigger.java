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

package org.apache.hop.pipeline.transforms.update;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.h2.api.Trigger;

/**
 * An H2 trigger that refuses the second update of a chosen id, also when the first one was rolled
 * back: it remembers outside the transaction which ids it saw.
 */
public class UpdateRefusingTrigger implements Trigger {

  private static final Set<Long> REFUSED = ConcurrentHashMap.newKeySet();
  private static final Set<Long> SEEN = ConcurrentHashMap.newKeySet();

  /** Forgets what was seen, then refuses the second update of the given ids. */
  static void refuseTheSecondUpdateOf(Long... ids) {
    REFUSED.clear();
    SEEN.clear();
    REFUSED.addAll(Set.of(ids));
  }

  @Override
  public void fire(Connection connection, Object[] oldRow, Object[] newRow) throws SQLException {
    long id = ((Number) newRow[0]).longValue();
    if (REFUSED.contains(id) && !SEEN.add(id)) {
      throw new SQLException("The trigger refuses the second update of id " + id);
    }
  }
}
