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

package org.apache.hop.marketplace.install;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Locale;
import org.apache.hop.marketplace.install.TransferFormat.EtaUnit;
import org.junit.jupiter.api.Test;

/** Formatting shared by the GUI progress dialog and the CLI progress bar. */
class TransferFormatTest {

  @Test
  void bytesUseOneDecimalBelowOneHundred() {
    // Same rule a browser uses: keeps the width stable as the number climbs.
    assertEquals("512B", TransferFormat.bytes(512));
    assertEquals("1.0KB", TransferFormat.bytes(1024));
    assertEquals("488KB", TransferFormat.bytes(500_000));
    assertEquals("42.0MB", TransferFormat.bytes(42L * 1024 * 1024));
    assertEquals("353MB", TransferFormat.bytes(353L * 1024 * 1024));
    assertEquals("1.5GB", TransferFormat.bytes(1536L * 1024 * 1024));
  }

  @Test
  void bytesNeverPromotePastTheLargestUnit() {
    // 4096TB rather than a unit label we do not have.
    assertTrue(TransferFormat.bytes(4096L * 1024 * 1024 * 1024 * 1024).endsWith("TB"));
  }

  @Test
  void speedIsAByteCountPerSecond() {
    assertEquals("1.8MB/s", TransferFormat.speed(1_835_008L));
    assertEquals("0B/s", TransferFormat.speed(0L));
  }

  @Test
  void etaRoundsToTheCoarsestUsefulUnit() {
    assertEquals(new TransferFormat.Eta(30, EtaUnit.SECONDS), TransferFormat.eta(30));
    assertEquals(new TransferFormat.Eta(1, EtaUnit.MINUTES), TransferFormat.eta(60));
    assertEquals(new TransferFormat.Eta(3, EtaUnit.MINUTES), TransferFormat.eta(178));
    assertEquals(new TransferFormat.Eta(1, EtaUnit.HOURS), TransferFormat.eta(3700));
    assertEquals(new TransferFormat.Eta(2, EtaUnit.HOURS), TransferFormat.eta(7200));
    // 2.5 hours rounds up rather than truncating to "2 hrs".
    assertEquals(new TransferFormat.Eta(3, EtaUnit.HOURS), TransferFormat.eta(9000));
  }

  @Test
  void etaNeverReportsZero() {
    // "0 secs left" reads as finished when it is not; the last tick should say "1 sec left".
    assertEquals(new TransferFormat.Eta(1, EtaUnit.SECONDS), TransferFormat.eta(0));
  }

  @Test
  void byteFormattingIgnoresTheDefaultLocale() {
    // A comma-decimal locale must not turn "17.6MB" into "17,6MB", nor group thousands with a dot
    // where "1.750MB" would be ambiguous. Guards against the suite passing only on dot-decimal
    // machines while the shipped binary prints something else.
    Locale original = Locale.getDefault();
    try {
      Locale.setDefault(Locale.GERMANY);
      assertEquals("17.6MB", TransferFormat.bytes(18_454_938L));
      assertEquals("353MB", TransferFormat.bytes(353L * 1024 * 1024));
      assertEquals("1.8MB/s", TransferFormat.speed(1_835_008L));
    } finally {
      Locale.setDefault(original);
    }
  }

  @Test
  void etaFlagsSingularAmounts() {
    assertTrue(TransferFormat.eta(60).isSingular());
    assertFalse(TransferFormat.eta(120).isSingular());
  }
}
