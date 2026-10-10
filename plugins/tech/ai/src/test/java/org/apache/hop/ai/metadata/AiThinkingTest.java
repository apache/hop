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

package org.apache.hop.ai.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class AiThinkingTest {

  @Test
  void emptyMeansDefault() {
    assertEquals(AiThinking.DEFAULT, AiThinking.lookup(null));
    assertEquals(AiThinking.DEFAULT, AiThinking.lookup(""));
    assertEquals(AiThinking.DEFAULT, AiThinking.lookup("  "));
  }

  @Test
  void readsBackTheLabelAndTheCodeIgnoringCase() {
    assertEquals(AiThinking.OFF, AiThinking.lookup(AiThinking.OFF.getDescription()));
    assertEquals(AiThinking.OFF, AiThinking.lookup("OFF"));
    assertEquals(AiThinking.ON, AiThinking.lookup(" on "));
    assertEquals(AiThinking.DEFAULT, AiThinking.lookup("default"));
  }

  @Test
  void anythingElseIsNotASetting() {
    assertNull(AiThinking.lookup("maybe"));
    assertNull(AiThinking.lookup("${AI_THINKING}"));
  }

  @Test
  void eachSettingMapsOntoThink() {
    assertNull(AiThinking.DEFAULT.think());
    assertEquals(Boolean.FALSE, AiThinking.OFF.think());
    assertEquals(Boolean.TRUE, AiThinking.ON.think());
  }

  @Test
  void aRecognisedValueIsStoredAsItsCode() {
    assertEquals("OFF", AiThinking.toCode("Off"));
    assertEquals("ON", AiThinking.toCode(" on "));
    assertEquals("DEFAULT", AiThinking.toCode("Default"));
    assertEquals("OFF", AiThinking.toCode("OFF"));
  }

  @Test
  void anythingElseIsStoredAsItIs() {
    assertEquals("${AI_THINKING}", AiThinking.toCode("${AI_THINKING}"));
    assertEquals("maybe", AiThinking.toCode("maybe"));
    assertEquals("", AiThinking.toCode(""));
    assertNull(AiThinking.toCode(null));
  }

  @Test
  void aStoredCodeIsShownAsItsLabel() {
    assertEquals(AiThinking.OFF.getDescription(), AiThinking.toDescription("OFF"));
    assertEquals("${AI_THINKING}", AiThinking.toDescription("${AI_THINKING}"));
    assertEquals("", AiThinking.toDescription(""));
  }

  @Test
  void theCodeIsTheConstantName() {
    for (AiThinking thinking : AiThinking.values()) {
      assertEquals(thinking.name(), thinking.getCode());
      assertEquals(thinking.name(), thinking.toString());
    }
  }
}
