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

package org.apache.hop.neo4j.logging.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.internal.InternalNode;
import org.neo4j.driver.types.Node;

/**
 * The registration date of an Execution node is written by both the Neo4j execution information
 * location (a local date time) and Neo4j logging (a string), see issue #8704.
 */
class LoggingCoreTest {

  private static Node execution(Value registrationDate) {
    return new InternalNode(1, List.of("Execution"), Map.of("registrationDate", registrationDate));
  }

  private static Date toDate(LocalDateTime localDateTime) {
    return Date.from(localDateTime.atZone(ZoneId.systemDefault()).toInstant());
  }

  @Test
  void localDateTimeWrittenByTheExecutionInformationLocation() {
    LocalDateTime registered = LocalDateTime.of(2026, 9, 30, 14, 15, 16);

    assertEquals(
        toDate(registered),
        LoggingCore.getDateValue(execution(Values.value(registered)), "registrationDate"));
  }

  @Test
  void stringWrittenByNeo4jLogging() {
    // The format LoggingCore.writeHierarchies() uses
    //
    assertEquals(
        toDate(LocalDateTime.of(2026, 9, 30, 14, 15, 16)),
        LoggingCore.getDateValue(
            execution(Values.value("2026/09/30T14:15:16")), "registrationDate"));
  }

  @Test
  void dateTimeWithATimeZone() {
    ZonedDateTime registered = ZonedDateTime.of(2026, 9, 30, 14, 15, 16, 0, ZoneOffset.UTC);

    assertEquals(
        Date.from(registered.toInstant()),
        LoggingCore.getDateValue(execution(Values.value(registered)), "registrationDate"));
  }

  @Test
  void unparsableStringIsNull() {
    assertNull(LoggingCore.getDateValue(execution(Values.value("not a date")), "registrationDate"));
  }

  @Test
  void otherTypeIsNull() {
    assertNull(LoggingCore.getDateValue(execution(Values.value(42)), "registrationDate"));
  }

  @Test
  void missingPropertyIsNull() {
    assertNull(LoggingCore.getDateValue(execution(Values.NULL), "registrationDate"));
    assertNull(
        LoggingCore.getDateValue(new InternalNode(1, List.of("Execution"), Map.of()), "other"));
  }
}
