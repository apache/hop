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

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class AiM2PromptSupportTest {

  @Test
  void supplementIncludesSchemaAndExcludesPluginConfig() throws Exception {
    String supplement = AiM2PromptSupport.buildSupplement();
    assertTrue(supplement.contains("hop_proposals"));
    assertTrue(supplement.contains("ADD_TRANSFORM"));
    assertTrue(supplement.contains("ADD_ACTION"));
    assertTrue(supplement.contains("Do not emit SET_TRANSFORM_PROPERTY"));
  }

  @Test
  void appendsAppliedSummaries() {
    StringBuilder prompt = new StringBuilder();
    AiM2PromptSupport.appendAppliedSummaries(prompt, List.of("ADD_TRANSFORM: Check (Dummy)"));
    assertTrue(prompt.toString().contains("ADD_TRANSFORM: Check (Dummy)"));
  }
}
