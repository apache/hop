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

package org.apache.hop.pipeline.transforms.delete;

import java.sql.Connection;
import java.util.concurrent.atomic.AtomicInteger;
import org.h2.api.Trigger;

/**
 * An H2 trigger with a side effect outside the transaction: it counts every row a DELETE runs on,
 * also when the transaction is rolled back afterwards.
 */
public class DeleteCountingTrigger implements Trigger {

  static final AtomicInteger DELETES = new AtomicInteger();

  @Override
  public void fire(Connection connection, Object[] oldRow, Object[] newRow) {
    DELETES.incrementAndGet();
  }
}
