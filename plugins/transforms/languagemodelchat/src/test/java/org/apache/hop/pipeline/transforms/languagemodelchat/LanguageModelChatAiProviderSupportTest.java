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

package org.apache.hop.pipeline.transforms.languagemodelchat;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.Test;

class LanguageModelChatAiProviderSupportTest {

  @Test
  void blankNameReturnsSameMeta() throws Exception {
    LanguageModelChatMeta meta = new LanguageModelChatMeta();
    meta.setDefault();
    assertSame(meta, LanguageModelChatAiProviderSupport.resolve(meta, new Variables(), null));
  }

  @Test
  void namedProviderWithoutFactoryOrMetadataFails() {
    LanguageModelChatMeta meta = new LanguageModelChatMeta();
    meta.setDefault();
    meta.setAiProviderName("prod-openai");
    HopException e =
        assertThrows(
            HopException.class,
            () -> LanguageModelChatAiProviderSupport.resolve(meta, new Variables(), null));
    assertTrue(
        e.getMessage().contains("hop-tech-ai") || e.getMessage().contains("metadata provider"));
  }
}
