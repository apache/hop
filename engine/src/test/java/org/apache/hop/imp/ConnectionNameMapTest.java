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
package org.apache.hop.imp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Locale;
import org.junit.jupiter.api.Test;

class ConnectionNameMapTest {

  @Test
  void caseVariantsCollapseToOneSpelling() {
    ConnectionNameMap map =
        ConnectionNameMap.build(List.of("Database", "database", "DATABASE"), List.of(), null);

    assertEquals("Database", map.targetFor("Database"));
    assertEquals("Database", map.targetFor("database"));
    assertEquals("Database", map.targetFor("DATABASE"));
  }

  @Test
  void preferredSharedXmlSpellingWins() {
    ConnectionNameMap map =
        ConnectionNameMap.build(
            List.of("WAREHOUSE", "warehouse", "Warehouse"), List.of("Warehouse"), null);

    assertEquals("Warehouse", map.targetFor("WAREHOUSE"));
    assertEquals("Warehouse", map.targetFor("warehouse"));
  }

  @Test
  void mostFrequentSpellingWinsWithoutPreference() {
    ConnectionNameMap map =
        ConnectionNameMap.build(List.of("Sales", "sales", "sales", "SALES"), List.of(), null);

    assertEquals("sales", map.targetFor("Sales"));
    assertEquals("sales", map.targetFor("SALES"));
  }

  @Test
  void mapperIsAppliedToTheCanonicalOriginal() {
    ConnectionNameMap map =
        ConnectionNameMap.build(
            List.of("My DB", "MY DB"),
            List.of(),
            value -> value.toLowerCase(Locale.ROOT).replace(' ', '_'));

    assertEquals("my_db", map.targetFor("My DB"));
    assertEquals("my_db", map.targetFor("MY DB"));
  }

  @Test
  void distinctGroupsThatMapToTheSameTargetAreCollisions() {
    ConnectionNameMap map =
        ConnectionNameMap.build(
            List.of("My-DB", "My_DB"),
            List.of(),
            value -> value.toLowerCase(Locale.ROOT).replace('-', '_'));

    assertEquals("my_db", map.targetFor("My-DB"));
    assertEquals("my_db", map.targetFor("My_DB"));
    assertEquals(1, map.getCollisions().size());
  }

  @Test
  void variableNamesAreLeftAlone() {
    ConnectionNameMap map =
        ConnectionNameMap.build(List.of("${CONN}", "Warehouse"), List.of(), String::toLowerCase);

    assertEquals("${CONN}", map.targetFor("${CONN}"));
    assertTrue(ConnectionNameMap.shouldSkip("${CONN}"));
    assertEquals("warehouse", map.targetFor("Warehouse"));
  }

  @Test
  void unknownNamesPassThrough() {
    ConnectionNameMap map = ConnectionNameMap.build(List.of("Warehouse"), List.of(), null);
    assertEquals("Other", map.targetFor("Other"));
  }
}
