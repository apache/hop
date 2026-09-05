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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class DatabaseSqlEditorTabTest {

  @Test
  void queryResultMessageUnderTheCap() {
    assertEquals("Query 1: 12 row(s)", DatabaseSqlEditorTab.queryResultMessage(1, 12, 1000));
  }

  @Test
  void queryResultMessageAtTheCapExplainsTheLimit() {
    String message = DatabaseSqlEditorTab.queryResultMessage(1, 1000, 1000);
    assertTrue(message.startsWith("Query 1: 1000 row(s)"));
    assertTrue(message.contains("capped at 1000"));
    assertTrue(message.contains("Database Perspective"));
    assertTrue(message.contains("SQL LIMIT"));
  }

  @Test
  void queryResultMessageBelowCapDoesNotMentionTheOption() {
    String message = DatabaseSqlEditorTab.queryResultMessage(2, 50, 2000);
    assertEquals("Query 2: 50 row(s)", message);
    assertFalse(message.contains("capped"));
  }
}
