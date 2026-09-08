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

package org.apache.hop.ui.hopgui.delegates;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.ui.hopgui.file.empty.EmptyHopFileTypeHandler;
import org.junit.jupiter.api.Test;

class FileRefreshHandlerMapTest {

  @Test
  void twoHandlersShareAWatchUntilTheLastIsRemoved() {
    FileRefreshHandlerMap map = new FileRefreshHandlerMap();
    EmptyHopFileTypeHandler explorer = new EmptyHopFileTypeHandler();
    EmptyHopFileTypeHandler database = new EmptyHopFileTypeHandler();

    assertTrue(map.add("file:///tmp/q.sql", explorer));
    assertFalse(map.add("file:///tmp/q.sql", database));
    map.alias("file:///tmp/q.sql", "/tmp/q.sql");
    assertEquals(2, map.get("file:///tmp/q.sql").size());
    assertEquals(2, map.get("/tmp/q.sql").size());

    assertFalse(map.remove("/tmp/q.sql", database));
    assertEquals(1, map.get("file:///tmp/q.sql").size());
    assertTrue(map.remove("file:///tmp/q.sql", explorer));
    assertTrue(map.get("file:///tmp/q.sql").isEmpty());
    assertTrue(map.get("/tmp/q.sql").isEmpty());
  }

  @Test
  void removeAllDropsEveryHandler() {
    FileRefreshHandlerMap map = new FileRefreshHandlerMap();
    map.add("a.sql", new EmptyHopFileTypeHandler());
    map.add("a.sql", new EmptyHopFileTypeHandler());
    assertTrue(map.removeAll("a.sql"));
    assertTrue(map.get("a.sql").isEmpty());
  }
}
