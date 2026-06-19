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

package org.apache.hop.pipeline.transforms.languagemodelchat.internals;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transforms.languagemodelchat.LanguageModelChatMeta;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class LanguageModelFacadeReadMessagesJsonTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private LanguageModelFacade facade() {
    return new LanguageModelFacade(new Variables(), new LanguageModelChatMeta());
  }

  @Test
  void readMessagesJson_topLevelArray() throws HopValueException {
    List<Message> messages =
        facade().readMessagesJson("[{\"role\":\"user\",\"content\":\"hello from array\"}]");
    assertEquals(1, messages.size());
    assertEquals("user", messages.get(0).getRole());
    assertEquals("hello from array", messages.get(0).getContent());
  }
}
