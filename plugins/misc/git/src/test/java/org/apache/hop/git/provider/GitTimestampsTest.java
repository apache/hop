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

package org.apache.hop.git.provider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.time.Instant;
import java.util.Date;
import org.junit.jupiter.api.Test;

/**
 * The five supported providers each format timestamps differently; all must land on one instant.
 */
class GitTimestampsTest {

  private static final Instant EXPECTED = Instant.parse("2026-05-01T12:00:00Z");

  @Test
  void parsesGitHubAndGiteaZuluForm() {
    assertEquals(Date.from(EXPECTED), GitTimestamps.toDate("2026-05-01T12:00:00Z"));
  }

  @Test
  void parsesGitLabMillisecondForm() {
    assertEquals(Date.from(EXPECTED), GitTimestamps.toDate("2026-05-01T12:00:00.000Z"));
  }

  @Test
  void parsesBitbucketMicrosecondOffsetForm() {
    assertEquals(Date.from(EXPECTED), GitTimestamps.toDate("2026-05-01T12:00:00.000000+00:00"));
  }

  @Test
  void parsesAnExplicitNonUtcOffset() {
    assertEquals(Date.from(EXPECTED), GitTimestamps.toDate("2026-05-01T14:00:00+02:00"));
  }

  @Test
  void parsesABareLocalDateTimeAsUtc() {
    assertEquals(Date.from(EXPECTED), GitTimestamps.toDate("2026-05-01T12:00:00"));
  }

  @Test
  void toleratesSurroundingWhitespace() {
    assertEquals(Date.from(EXPECTED), GitTimestamps.toDate("  2026-05-01T12:00:00Z  "));
  }

  @Test
  void returnsNullForAbsentOrUnparseableValues() {
    assertNull(GitTimestamps.toDate(null));
    assertNull(GitTimestamps.toDate(""));
    assertNull(GitTimestamps.toDate("   "));
    assertNull(GitTimestamps.toDate("not-a-date"));
    assertNull(GitTimestamps.toDate("0000-00-00"));
  }
}
