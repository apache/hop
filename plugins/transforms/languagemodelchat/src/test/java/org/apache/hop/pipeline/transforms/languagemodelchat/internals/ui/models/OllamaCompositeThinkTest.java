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
package org.apache.hop.pipeline.transforms.languagemodelchat.internals.ui.models;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hop.pipeline.transforms.languagemodelchat.internals.OllamaThink;
import org.junit.jupiter.api.Test;

/** The Thinking combo lists the settings in order, and reads each one back. */
class OllamaCompositeThinkTest {

  @Test
  void eachEntryReadsBackAsItsSetting() {
    for (OllamaThink think : OllamaThink.values()) {
      assertEquals(think, OllamaComposite.think(OllamaComposite.thinkIndex(think)));
    }
  }

  @Test
  void anOlderTransformShowsDefault() {
    assertEquals(OllamaThink.DEFAULT.ordinal(), OllamaComposite.thinkIndex(null));
  }

  @Test
  void noSelectionLeavesItToTheModel() {
    assertEquals(OllamaThink.DEFAULT, OllamaComposite.think(-1));
  }
}
