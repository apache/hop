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
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The registration date of an Execution node is written by both the Neo4j execution information
 * location (a local date time) and Neo4j logging (a string), see issue #8704.
 */
class LoggingCoreTest {

  /** The properties of an Execution node, as a graph connection returns them. */
  private static Map<String, Object> execution(Object registrationDate) {
    Map<String, Object> properties = new HashMap<>();
    properties.put("registrationDate", registrationDate);
    return properties;
  }

  private static Date toDate(LocalDateTime localDateTime) {
    return Date.from(localDateTime.atZone(ZoneId.systemDefault()).toInstant());
  }

  @Test
  void localDateTimeWrittenByTheExecutionInformationLocation() {
    LocalDateTime registered = LocalDateTime.of(2026, 9, 30, 14, 15, 16);

    assertEquals(
        toDate(registered), LoggingCore.getDateValue(execution(registered), "registrationDate"));
  }

  @Test
  void stringWrittenByNeo4jLogging() {
    // The format LoggingCore.writeHierarchies() uses
    //
    assertEquals(
        toDate(LocalDateTime.of(2026, 9, 30, 14, 15, 16)),
        LoggingCore.getDateValue(execution("2026/09/30T14:15:16"), "registrationDate"));
  }

  @Test
  void dateTimeWithATimeZone() {
    ZonedDateTime registered = ZonedDateTime.of(2026, 9, 30, 14, 15, 16, 0, ZoneOffset.UTC);

    assertEquals(
        Date.from(registered.toInstant()),
        LoggingCore.getDateValue(execution(registered), "registrationDate"));
  }

  @Test
  void isoStringWrittenByTheExecutionInformationLocationOnFalkorDbAndAge() {
    assertEquals(
        toDate(LocalDateTime.of(2026, 9, 30, 14, 15, 16)),
        LoggingCore.getDateValue(execution("2026-09-30T14:15:16"), "registrationDate"));
  }

  @Test
  void loggingDateReportedByTheExecutionInformationLocation() {
    // Issue #9039: Neo4j logging stores this text, and the execution list has to read it.
    assertEquals(
        toDate(LocalDateTime.of(2026, 10, 7, 16, 8, 52)),
        LoggingCore.parseDate("2026/10/07T16:08:52"));
  }

  @Test
  void timestampTextAndOffset() {
    assertEquals(
        toDate(LocalDateTime.of(2026, 9, 30, 14, 15, 16)),
        LoggingCore.parseDate("2026-09-30 14:15:16.000000000"));
    assertEquals(
        Date.from(OffsetDateTime.parse("2026-09-30T12:15:16Z").toInstant()),
        LoggingCore.parseDate("2026-09-30T12:15:16Z"));
  }

  @Test
  void blankOrUnparsableStringIsNull() {
    assertNull(LoggingCore.parseDate(null));
    assertNull(LoggingCore.parseDate(""));
    assertNull(LoggingCore.parseDate("   "));
    assertNull(LoggingCore.getDateValue(execution("not a date"), "registrationDate"));
  }

  @Test
  void otherTypeIsNull() {
    assertNull(LoggingCore.getDateValue(execution(42), "registrationDate"));
  }

  @Test
  void missingPropertyIsNull() {
    assertNull(LoggingCore.getDateValue(execution(null), "registrationDate"));
    assertNull(LoggingCore.getDateValue(Map.of(), "other"));
  }
}
