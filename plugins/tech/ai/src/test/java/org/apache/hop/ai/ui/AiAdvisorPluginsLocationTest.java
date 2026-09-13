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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.ai.advisor.AiAdvisorLocations;
import org.junit.jupiter.api.Test;

class AiAdvisorPluginsLocationTest {

  @Test
  void emptyLocationsMatchAnySession() {
    assertTrue(AiAdvisorPlugins.keywordsOfferLocation(new String[0], "data-vault-graph"));
    assertTrue(AiAdvisorPlugins.keywordsOfferLocation(null, AiAdvisorLocations.PIPELINE_GRAPH));
  }

  @Test
  void pipelineAdvisorIsHiddenOnVaultSession() {
    assertFalse(
        AiAdvisorPlugins.keywordsOfferLocation(
            new String[] {AiAdvisorLocations.PIPELINE_GRAPH}, "data-vault-graph"));
    assertTrue(
        AiAdvisorPlugins.keywordsOfferLocation(
            new String[] {AiAdvisorLocations.PIPELINE_GRAPH}, AiAdvisorLocations.PIPELINE_GRAPH));
  }

  @Test
  void vaultAdvisorMatchesVaultOnly() {
    assertTrue(
        AiAdvisorPlugins.keywordsOfferLocation(
            new String[] {"data-vault-graph"}, "data-vault-graph"));
    assertFalse(
        AiAdvisorPlugins.keywordsOfferLocation(
            new String[] {"data-vault-graph"}, AiAdvisorLocations.PIPELINE_GRAPH));
  }
}
