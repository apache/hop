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

package org.apache.hop.marketplace.gui;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.marketplace.install.PluginVerifier.FileFinding;
import org.apache.hop.marketplace.install.PluginVerifier.Finding;
import org.apache.hop.marketplace.install.PluginVerifier.Result;
import org.apache.hop.marketplace.install.PluginVerifier.State;
import org.junit.jupiter.api.Test;

/** The verify report of the marketplace dialog has a message for every state and finding. */
class VerifyReportTest {

  @Test
  void everyStateAndFindingHasAMessage() {
    List<FileFinding> files = new ArrayList<>();
    for (Finding finding : Finding.values()) {
      files.add(new FileFinding("plugins/tech/x/" + finding + ".jar", finding, "hop-other"));
    }
    for (State state : State.values()) {
      for (boolean withoutChecksums : new boolean[] {false, true}) {
        String report =
            MarketplaceDialog.verifyReport(
                "X 1.0", new Result("hop-x", "1.0", state, withoutChecksums, files, "broken"));
        // BaseMessages shows a missing key as !key!
        assertFalse(report.contains("!"), report);
        assertTrue(report.startsWith("X 1.0: "), report);
      }
    }
  }

  @Test
  void longListsAreCut() {
    List<FileFinding> files = new ArrayList<>();
    for (int i = 0; i < 13; i++) {
      files.add(new FileFinding("plugins/tech/x/" + i + ".jar", Finding.MISSING, null));
    }
    String report =
        MarketplaceDialog.verifyReport(
            "X", new Result("hop-x", "1.0", State.DAMAGED, false, files, null));
    assertEquals(12, report.lines().count(), report);
    assertTrue(report.contains("and 3 more"), report);
  }
}
