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

package org.apache.hop.neo4j.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.exception.HopRuntimeException;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.execution.IExecutionSelector;
import org.junit.jupiter.api.Test;

/** Reading execution dates must survive the text Neo4j logging stores, see issue #9039. */
class NeoExecutionInfoLocationReadTest {

  private static Date at(int year, int month, int day, int hour, int minute, int second) {
    return Date.from(
        LocalDateTime.of(year, month, day, hour, minute, second)
            .atZone(ZoneId.systemDefault())
            .toInstant());
  }

  @Test
  void loggingRegistrationDateIsRead() {
    Date registered = at(2026, 10, 7, 16, 8, 52);

    assertEquals(registered, NeoExecutionInfoLocation.toDate("2026/10/07T16:08:52"));
    assertEquals(registered, NeoExecutionInfoLocation.toDate("2026-10-07T16:08:52"));
    assertEquals(registered, NeoExecutionInfoLocation.toDate("2026-10-07 16:08:52"));
    assertEquals(
        registered, NeoExecutionInfoLocation.toDate(LocalDateTime.of(2026, 10, 7, 16, 8, 52)));
    assertEquals(
        Date.from(OffsetDateTime.parse("2026-10-07T14:08:52Z").toInstant()),
        NeoExecutionInfoLocation.toDate("2026-10-07T14:08:52Z"));
  }

  @Test
  void unparsableDateIsEmpty() {
    assertNull(NeoExecutionInfoLocation.toDate(null));
    assertNull(NeoExecutionInfoLocation.toDate(""));
    assertNull(NeoExecutionInfoLocation.toDate("   "));
    assertNull(NeoExecutionInfoLocation.toDate("not a date"));
    assertNull(NeoExecutionInfoLocation.toDate(42));
  }

  @Test
  void sampledDateReportsABadValueOnThatCell() {
    ValueMetaDate when = new ValueMetaDate("when");

    assertEquals(
        at(2026, 10, 7, 16, 8, 52),
        NeoExecutionInfoLocation.extractHopValue(when, "2026/10/07T16:08:52"));
    assertNull(NeoExecutionInfoLocation.extractHopValue(when, ""));
    assertThrows(
        HopRuntimeException.class,
        () -> NeoExecutionInfoLocation.extractHopValue(when, "not a date"));
  }

  @Test
  void oneUnreadableExecutionDoesNotHideTheOthers() {
    Map<String, Object> badDetails = execution("bad-details", "Workflow", null);
    badDetails.put("n.details", "{");

    List<String> ids =
        new NeoExecutionInfoLocation()
            .idsOfReadableExecutions(
                List.of(
                    execution("good", "Pipeline", LocalDateTime.of(2026, 10, 7, 16, 8, 52)),
                    execution("bad-type", "PIPELINE", "2026/10/07T16:08:52"),
                    execution("logging", "Pipeline", "2026/10/07T16:08:52"),
                    badDetails,
                    execution("later", "Workflow", "not a date")),
                IExecutionSelector.ALL);

    assertEquals(List.of("good", "logging", "later"), ids);
  }

  /** A row as {@link NeoExecutionInfoLocation} reads it back from a Cypher result. */
  private static Map<String, Object> execution(String id, String type, Object registrationDate) {
    Map<String, Object> record = new HashMap<>();
    record.put("n.id", id);
    record.put("n.executionType", type);
    record.put("n.name", id);
    if (registrationDate != null) {
      record.put("n.registrationDate", registrationDate);
    }
    return record;
  }
}
