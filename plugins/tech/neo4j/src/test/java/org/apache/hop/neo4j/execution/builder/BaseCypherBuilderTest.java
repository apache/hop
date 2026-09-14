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

package org.apache.hop.neo4j.execution.builder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class BaseCypherBuilderTest {

  private static Object mapped(Object value) {
    return CypherMergeBuilder.of().withValue("x", value).parameters().get("x");
  }

  @Test
  void integerBecomesLong() {
    assertEquals(5L, mapped(5));
  }

  @Test
  void bigDecimalBecomesString() {
    assertEquals("1.25", mapped(new BigDecimal("1.25")));
  }

  @Test
  void dateBecomesLocalDateTime() {
    Date date = new Date(1_700_000_000_000L);
    assertInstanceOf(LocalDateTime.class, mapped(date));
  }

  @Test
  void mapBecomesJsonString() {
    Object value = mapped(Map.of("a", "b"));
    assertInstanceOf(String.class, value);
    assertTrue(value.toString().contains("\"a\""));
  }

  @Test
  void mixedListBecomesJsonString() {
    Object value = mapped(List.of("a", 1));
    assertInstanceOf(String.class, value);
  }

  @Test
  void homogeneousStringListIsKept() {
    Object value = mapped(List.of("a", "b"));
    assertEquals(List.of("a", "b"), value);
  }

  @Test
  void unknownObjectBecomesString() {
    assertEquals("hello", mapped(new StringBuilder("hello")));
  }
}
