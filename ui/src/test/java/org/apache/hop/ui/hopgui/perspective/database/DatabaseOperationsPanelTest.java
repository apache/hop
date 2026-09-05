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

package org.apache.hop.ui.hopgui.perspective.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class DatabaseOperationsPanelTest {

  @Test
  void formatStatusLineMatchesRequestedPattern() {
    DatabaseOperation operation = new DatabaseOperation("Execute SQL on shop", "shop");
    operation.complete();
    assertEquals(
        "Execute SQL on shop - shop - Done - "
            + DatabaseOperationsPanel.formatElapsed(operation.elapsedMillis()),
        DatabaseOperationsPanel.formatStatusLine(operation));
  }

  @Test
  void formatStatusLineOmitsBlankConnection() {
    DatabaseOperation operation = new DatabaseOperation("Connect", "");
    operation.fail("boom");
    String line = DatabaseOperationsPanel.formatStatusLine(operation);
    assertTrue(line.startsWith("Connect - Failed - "));
    assertTrue(line.endsWith(" ms") || line.contains(" s"));
  }

  @Test
  void formatStatusLineEmptyWhenNoOperation() {
    assertEquals("", DatabaseOperationsPanel.formatStatusLine(null));
  }

  @Test
  void formatElapsed() {
    assertEquals("0 ms", DatabaseOperationsPanel.formatElapsed(0));
    assertEquals("12 ms", DatabaseOperationsPanel.formatElapsed(12));
    assertEquals("1.5 s", DatabaseOperationsPanel.formatElapsed(1500));
    assertEquals("1:05", DatabaseOperationsPanel.formatElapsed(65_000));
  }
}
