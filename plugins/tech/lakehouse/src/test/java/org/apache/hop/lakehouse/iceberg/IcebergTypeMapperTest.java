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

package org.apache.hop.lakehouse.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaTimestamp;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Dates and timestamps without a zone are read and written in the JVM time zone, like Parquet
 * Input. Surefire runs in UTC, so these tests switch the default zone.
 */
class IcebergTypeMapperTest {

  private TimeZone original;

  @BeforeEach
  void saveZone() {
    original = TimeZone.getDefault();
  }

  @AfterEach
  void restoreZone() {
    TimeZone.setDefault(original);
  }

  static List<String> zones() {
    return List.of(
        "UTC", "America/Los_Angeles", "Europe/Berlin", "Asia/Kolkata", "Pacific/Kiritimati");
  }

  @ParameterizedTest
  @MethodSource("zones")
  void dateIsMidnightOfThatDay(String zone) throws Exception {
    TimeZone.setDefault(TimeZone.getTimeZone(zone));
    LocalDate day = LocalDate.of(2026, 10, 1);

    Date date = (Date) IcebergTypeMapper.toHopValue(day, Types.DateType.get());

    assertEquals(
        day.atStartOfDay(), date.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime());
    assertEquals(
        day, IcebergTypeMapper.toIcebergValue(new ValueMetaDate("d"), date, Types.DateType.get()));
  }

  @ParameterizedTest
  @MethodSource("zones")
  void timestampWithoutZoneKeepsItsWallClockTime(String zone) throws Exception {
    TimeZone.setDefault(TimeZone.getTimeZone(zone));
    LocalDateTime noon = LocalDateTime.of(2026, 10, 1, 12, 0, 0, 123_456_000);

    for (var type :
        List.of(Types.TimestampType.withoutZone(), Types.TimestampNanoType.withoutZone())) {
      Timestamp timestamp = (Timestamp) IcebergTypeMapper.toHopValue(noon, type);

      assertEquals(noon, timestamp.toLocalDateTime());
      assertEquals(
          noon, IcebergTypeMapper.toIcebergValue(new ValueMetaTimestamp("ts"), timestamp, type));
    }
  }

  @ParameterizedTest
  @MethodSource("zones")
  void timestampWithZoneIsAnInstant(String zone) throws Exception {
    TimeZone.setDefault(TimeZone.getTimeZone(zone));
    OffsetDateTime instant = OffsetDateTime.of(2026, 10, 1, 12, 0, 0, 0, ZoneOffset.UTC);

    for (var type : List.of(Types.TimestampType.withZone(), Types.TimestampNanoType.withZone())) {
      Timestamp timestamp = (Timestamp) IcebergTypeMapper.toHopValue(instant, type);

      assertEquals(instant.toInstant(), timestamp.toInstant());
      Object back = IcebergTypeMapper.toIcebergValue(new ValueMetaTimestamp("ts"), timestamp, type);
      assertInstanceOf(OffsetDateTime.class, back);
      assertEquals(instant.toInstant(), ((OffsetDateTime) back).toInstant());
    }
  }
}
