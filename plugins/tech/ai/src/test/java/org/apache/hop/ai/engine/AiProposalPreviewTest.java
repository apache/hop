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

import org.apache.hop.ai.advisor.AiProposal;
import org.junit.jupiter.api.Test;

class AiProposalPreviewTest {

  @Test
  void appliedSummaryFallsBackToTypeAndDescription() {
    AiProposal proposal = new AiProposal();
    proposal.setType("CREATE_HUB");
    proposal.setDescription("Create Customer Hub");
    assertEquals("CREATE_HUB: Create Customer Hub", AiProposalPreview.appliedSummary(proposal));
    AiProposal typeOnly = new AiProposal();
    typeOnly.setType("CREATE_HUB");
    assertEquals("CREATE_HUB", AiProposalPreview.appliedSummary(typeOnly));
    assertEquals("unknown change", AiProposalPreview.appliedSummary(null));
  }
}
