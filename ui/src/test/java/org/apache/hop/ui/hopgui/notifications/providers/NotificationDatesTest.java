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
package org.apache.hop.ui.hopgui.notifications.providers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.time.Instant;
import java.util.Date;
import org.junit.jupiter.api.Test;

/** Unit tests for the timestamps a provider reads off a feed. */
public class NotificationDatesTest {

  @Test
  public void testUtcInstantIsNotShiftedByTheLocalOffset() {
    // The bug this guards against: a pattern quoting the 'Z' read a UTC instant as a local one, so
    // every release moved by the machine's offset and then fed that into the age filter.
    assertEquals(
        Date.from(Instant.parse("2026-02-01T10:00:00Z")),
        NotificationDates.parseIso("2026-02-01T10:00:00Z"));
  }

  @Test
  public void testOffsetIsHonoured() {
    assertEquals(
        Date.from(Instant.parse("2026-02-01T09:00:00Z")),
        NotificationDates.parseIso("2026-02-01T10:00:00+01:00"));
  }

  @Test
  public void testTimestampWithoutAnOffsetIsReadAsUtc() {
    assertEquals(
        Date.from(Instant.parse("2026-02-01T10:00:00Z")),
        NotificationDates.parseIso("2026-02-01T10:00:00"));
  }

  @Test
  public void testFractionalSecondsAreAccepted() {
    assertEquals(
        Date.from(Instant.parse("2026-02-01T10:00:00.500Z")),
        NotificationDates.parseIso("2026-02-01T10:00:00.500Z"));
  }

  @Test
  public void testUnparseableValuesGiveNull() {
    assertNull(NotificationDates.parseIso(null));
    assertNull(NotificationDates.parseIso(""));
    assertNull(NotificationDates.parseIso("   "));
    assertNull(NotificationDates.parseIso("last Tuesday"));
  }

  @Test
  public void testSurroundingWhitespaceIsIgnored() {
    assertEquals(
        Date.from(Instant.parse("2026-02-01T10:00:00Z")),
        NotificationDates.parseIso("  2026-02-01T10:00:00Z\n"));
  }
}
