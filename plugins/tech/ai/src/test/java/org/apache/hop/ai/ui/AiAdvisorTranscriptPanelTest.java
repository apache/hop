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

package org.apache.hop.ai.ui;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import org.junit.jupiter.api.Test;

class AiAdvisorTranscriptPanelTest {

  @Test
  void userAndAssistantColorsDifferInBothModes() {
    int[] lightUser = AiAdvisorTranscriptPanel.roleRgb(AiAdvisorTranscriptPanel.Role.USER, false);
    int[] lightAssistant =
        AiAdvisorTranscriptPanel.roleRgb(AiAdvisorTranscriptPanel.Role.ASSISTANT, false);
    int[] darkUser = AiAdvisorTranscriptPanel.roleRgb(AiAdvisorTranscriptPanel.Role.USER, true);
    int[] darkAssistant =
        AiAdvisorTranscriptPanel.roleRgb(AiAdvisorTranscriptPanel.Role.ASSISTANT, true);
    assertFalse(Arrays.equals(lightUser, lightAssistant));
    assertFalse(Arrays.equals(darkUser, darkAssistant));
    assertNotEquals(
        lightUser[0] + lightUser[1] + lightUser[2], darkUser[0] + darkUser[1] + darkUser[2]);
  }

  @Test
  void formatUsageIncludesTokensAndDuration() {
    String usage = AiAdvisorTranscriptPanel.formatUsage(1234, 56, 12_400L);
    assertTrue(usage.contains("1,234") || usage.contains("1234"), usage);
    assertTrue(usage.contains("56"), usage);
    assertTrue(usage.contains("12.4 s"), usage);
    assertTrue(usage.contains("in"), usage);
    assertTrue(usage.contains("out"), usage);
  }

  @Test
  void formatUsageOmitsMissingParts() {
    assertEquals("", AiAdvisorTranscriptPanel.formatUsage(null, null, null));
    assertEquals("850 ms", AiAdvisorTranscriptPanel.formatDuration(850));
    assertEquals("3 s", AiAdvisorTranscriptPanel.formatDuration(3000));
    assertEquals("1 m 05 s", AiAdvisorTranscriptPanel.formatDuration(65_000));
  }
}
