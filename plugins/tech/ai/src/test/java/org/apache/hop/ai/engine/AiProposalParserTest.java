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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.ai.advisor.AiAdvisorResponse;
import org.junit.jupiter.api.Test;

class AiProposalParserTest {

  @Test
  void parsesAdviceAndHopProposalsBlock() {
    String raw =
        """
        ## Suggestion
        Add a Filter Rows transform after the input.

        ```hop_proposals
        {
          "proposals": [
            {
              "id": "1",
              "description": "Add filter step",
              "riskLevel": "LOW",
              "type": "ADD_TRANSFORM",
              "parameters": {
                "transformPluginId": "FilterRows",
                "name": "Filter bad rows",
                "locationX": "320",
                "locationY": "120"
              }
            }
          ]
        }
        ```
        """;

    AiAdvisorResponse response = AiProposalParser.parse(raw);

    assertTrue(response.getMarkdownAdvice().contains("Add a Filter Rows transform"));
    assertFalse(response.getMarkdownAdvice().contains("hop_proposals"));
    assertTrue(response.isProposalBlockPresent());
    assertEquals(1, response.getProposals().size());
    assertEquals("ADD_TRANSFORM", response.getProposals().get(0).getType());
    assertEquals("FilterRows", response.getProposals().get(0).parameter("transformPluginId"));
  }

  @Test
  void malformedBlockIsDroppedButAdviceRemains() {
    String raw =
        """
        Keep this advice.

        ```hop_proposals
        { not json
        ```
        """;
    AiAdvisorResponse response = AiProposalParser.parse(raw);
    assertTrue(response.isProposalBlockPresent());
    assertTrue(response.getProposals().isEmpty());
    assertTrue(response.getMarkdownAdvice().contains("Keep this advice."));
  }

  @Test
  void unknownTypeIsKeptForAdvisorSpecificHandling() {
    String raw =
        """
        ```hop_proposals
        {"proposals":[{"type":"CREATE_HUB","parameters":{"name":"HUB_CUSTOMER"}}]}
        ```
        """;
    AiAdvisorResponse response = AiProposalParser.parse(raw);
    assertTrue(response.isProposalBlockPresent());
    assertEquals(1, response.getProposals().size());
    assertEquals("CREATE_HUB", response.getProposals().get(0).getType());
    assertEquals("HUB_CUSTOMER", response.getProposals().get(0).parameter("name"));
  }
}
