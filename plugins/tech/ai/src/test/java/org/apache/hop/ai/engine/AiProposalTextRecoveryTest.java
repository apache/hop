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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.ai.advisor.AiAdvisorResponse;
import org.apache.hop.ai.advisor.AiProposalParser;
import org.junit.jupiter.api.Test;

/** What llama3.2 actually sent: the values in text, the block without parameters. */
class AiProposalTextRecoveryTest {

  private static final String ANSWER =
      """
      To add a hop from "Output" to the new transform:

      ```markdown
      ADD_PIPELINE_HOP
        fromTransform: Output
        toTransform: dummy-new
        enabled: Y
      ```

      ```hop_proposals
      {"proposals":[{"id":"1","description":"Add a hop from Output to Dummy (do nothing).","riskLevel":"LOW","type":"ADD_PIPELINE_HOP"}]}
      ```
      """;

  @Test
  void parametersMissingFromTheBlockAreTakenFromTheText() {
    AiAdvisorResponse response = AiProposalParser.parse(ANSWER);
    assertTrue(response.getProposals().get(0).getParameters().isEmpty());

    assertTrue(AiProposalTextRecovery.fill(response, ANSWER));
    assertEquals("Output", response.getProposals().get(0).parameter("fromTransform"));
    assertEquals("dummy-new", response.getProposals().get(0).parameter("toTransform"));
  }

  @Test
  void withoutAUsableBlockTheTextProposalsAreUsed() {
    String answer =
        """
        **ADD_TRANSFORM**
          name: "dummy-new"
          pluginId: "Dummy"

        ```hop_proposals
        {"proposals":[{"type":"ADD_TRANSFORM|ADD_PIPELINE_HOP"}]}
        ```
        """;
    AiAdvisorResponse response = AiProposalParser.parse(answer);
    assertTrue(response.getProposalParseError() != null);

    assertTrue(AiProposalTextRecovery.fill(response, answer));
    assertNull(response.getProposalParseError());
    assertEquals(1, response.getProposals().size());
    assertEquals("ADD_TRANSFORM", response.getProposals().get(0).getType());
    assertEquals("Dummy", response.getProposals().get(0).parameter("pluginId"));
  }
}
