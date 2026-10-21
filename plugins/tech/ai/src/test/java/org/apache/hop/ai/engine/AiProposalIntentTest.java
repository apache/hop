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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.ai.advisor.AiAdvisorMetadataSelection;
import org.apache.hop.ai.advisor.AiAdvisorRequest;
import org.junit.jupiter.api.Test;

class AiProposalIntentTest {

  @Test
  void explanationsGoWithoutTheProposalFormat() {
    for (String question :
        new String[] {
          "What does this pipeline do?",
          "Explain the Filter rows transform",
          "Why did the last run fail?",
          "Wat doet deze pipeline?",
          "Warum schlägt das fehl?",
          "Pourquoi ce workflow échoue-t-il ?",
          "¿Qué hace este pipeline?"
        }) {
      assertFalse(AiProposalIntent.offersProposals("pipeline-general", question), question);
    }
  }

  @Test
  void changesGetTheProposalFormat() {
    for (String question :
        new String[] {
          "Add a Dummy after Output",
          "Why does it fail? Fix it.",
          "What should I add to write the rows to Excel?",
          "Voeg een Dummy toe na Output",
          "Lösche die Transformation Sort"
        }) {
      assertTrue(AiProposalIntent.offersProposals("pipeline-general", question), question);
    }
  }

  @Test
  void whenInDoubtTheFormatGoesAlong() {
    // A short follow-up to an answer that offered to propose the change.
    assertTrue(AiProposalIntent.offersProposals("pipeline-general", "yes please"));
    assertTrue(AiProposalIntent.offersProposals("pipeline-general", ""));
    assertTrue(AiProposalIntent.offersProposals(null, "Read a table into Excel"));
    // "was" is German for "what", and English for something else.
    assertTrue(AiProposalIntent.offersProposals(null, "The run was slow, speed it up"));
  }

  @Test
  void designScenariosAlwaysGetIt() {
    assertTrue(AiProposalIntent.offersProposals("pipeline-design", "What is the best layout?"));
    assertTrue(AiProposalIntent.offersProposals("action-selection", "Which action fits?"));
  }

  @Test
  void theDatabasePluginsGoWithDatabaseQuestions() {
    AiAdvisorRequest request = new AiAdvisorRequest();
    request.setUserPrompt("Add a Dummy after Output");
    assertFalse(AiProposalIntent.needsDatabasePlugins(request));
    request.setUserPrompt("Create a PostgreSQL connection");
    assertTrue(AiProposalIntent.needsDatabasePlugins(request));
    request.setUserPrompt("Change the host of this one");
    request.getMetadataSelections().add(new AiAdvisorMetadataSelection("rdbms", "dwh"));
    assertTrue(AiProposalIntent.needsDatabasePlugins(request));
  }

  @Test
  void theRequestRemembersWhetherTheFormatWasOffered() {
    AiAdvisorRequest request = new AiAdvisorRequest();
    assertTrue(AiProposalIntent.wasOffered(request), "unknown counts as offered");
    AiProposalIntent.markOffered(request, false);
    assertFalse(AiProposalIntent.wasOffered(request));
  }
}
