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

package org.apache.hop.ai.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.apache.hop.ai.advisor.AiProposal;
import org.apache.hop.core.HopEnvironment;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class AiClipboardProposalsTest {

  @BeforeAll
  static void initHop() throws Exception {
    HopEnvironment.init();
  }

  @Test
  void prefersTransformXmlOverMetadataJson() throws Exception {
    AiProposal transforms =
        proposal(
            "CLIPBOARD_TRANSFORMS",
            Map.of("xml", AiProposalXmlSupportTest.dummyTransformXml("Check")));
    AiProposal metadata =
        proposal(
            "CLIPBOARD_METADATA",
            Map.of("typeKey", "ai-provider", "name", "demo", "json", "{\"name\":\"demo\"}"));
    String text = AiClipboardProposals.buildClipboardText(List.of(metadata, transforms));
    assertTrue(text.contains("<pipeline-transforms>"));
    assertTrue(text.contains("Check"));
  }

  @Test
  void saveMetadataIsNotCopied() {
    AiProposal save =
        proposal(
            "SAVE_METADATA",
            Map.of("typeKey", "ai-provider", "name", "demo", "json", "{\"name\":\"demo\"}"));
    assertEquals("", AiClipboardProposals.buildClipboardText(List.of(save)));
    assertEquals(0, AiClipboardProposals.clipboardCount(List.of(save)));
  }

  @Test
  void clipboardCountIncludesClipboardTypes() {
    AiProposal clipboard = proposal("CLIPBOARD_METADATA", Map.of("json", "{\"name\":\"demo\"}"));
    assertEquals(1, AiClipboardProposals.clipboardCount(List.of(clipboard)));
  }

  private static AiProposal proposal(String type, Map<String, String> parameters) {
    AiProposal proposal = new AiProposal();
    proposal.setType(type);
    proposal.setParameters(parameters);
    return proposal;
  }
}
