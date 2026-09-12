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

import java.util.List;
import org.apache.hop.ai.advisor.AiAdvisorMetadataSelection;
import org.apache.hop.ai.advisor.AiAdvisorRequest;
import org.apache.hop.ai.advisors.AiAdvisorInclusions;
import org.apache.hop.ai.metadata.AiProvider;
import org.apache.hop.ai.providers.OpenAiProvider;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.junit.jupiter.api.Test;

public class AiAdvisorMetadataContextTest {

  @Test
  void appendSkippedWhenInclusionOffOrNothingSelected() {
    AiAdvisorRequest request = new AiAdvisorRequest();
    request.getInclusions().put(AiAdvisorInclusions.METADATA, false);
    request.getMetadataSelections().add(new AiAdvisorMetadataSelection("ai-provider", "prod"));
    StringBuilder off = new StringBuilder("head\n");
    AiAdvisorMetadataContext.appendToPrompt(off, request);
    assertEquals("head\n", off.toString());

    request.getInclusions().put(AiAdvisorInclusions.METADATA, true);
    request.getMetadataSelections().clear();
    StringBuilder empty = new StringBuilder("head\n");
    AiAdvisorMetadataContext.appendToPrompt(empty, request);
    assertEquals("head\n", empty.toString());
  }

  @Test
  void serializeRedactsApiKeyAndListsType() throws Exception {
    TestMetadataProvider provider = new TestMetadataProvider();
    AiProvider object = new AiProvider();
    OpenAiProvider backend = new OpenAiProvider();
    backend.setPluginId("openai");
    backend.setPluginName("OpenAI");
    object.setName("prod-openai");
    object.setProvider(backend);
    object.setApiKey("sk-live-secret");
    object.setModelName("gpt-4o");
    provider.getSerializer(AiProvider.class).save(object);

    List<AiAdvisorMetadataContext.TypeCatalog> types = AiAdvisorMetadataContext.listTypes(provider);
    assertEquals(1, types.size());
    assertEquals("ai-provider", types.get(0).getTypeKey());
    assertTrue(types.get(0).getNames().contains("prod-openai"));

    String json =
        AiAdvisorMetadataContext.serialize(
            provider, List.of(new AiAdvisorMetadataSelection("ai-provider", "prod-openai")));
    assertTrue(json.contains("prod-openai"));
    assertTrue(json.contains("gpt-4o"));
    assertFalse(json.contains("sk-live-secret"));
    assertTrue(json.contains("***"));
  }

  @Test
  void serializeTypeKeysIncludesEmptyTypes() {
    TestMetadataProvider provider = new TestMetadataProvider();
    String json = AiAdvisorMetadataContext.serializeTypeKeys(provider);
    assertTrue(json.contains("ai-provider"));
    assertTrue(json.contains("\"key\""));
  }

  @Test
  void missingObjectIsReported() {
    TestMetadataProvider provider = new TestMetadataProvider();
    String json =
        AiAdvisorMetadataContext.serialize(
            provider, List.of(new AiAdvisorMetadataSelection("ai-provider", "missing")));
    assertTrue(json.contains("not found") || json.contains("error"));
  }

  public static final class TestMetadataProvider extends MemoryMetadataProvider {
    @Override
    @SuppressWarnings("unchecked")
    public <T extends IHopMetadata> Class<T> getMetadataClassForKey(String key)
        throws HopException {
      if ("ai-provider".equals(key)) {
        return (Class<T>) AiProvider.class;
      }
      return super.getMetadataClassForKey(key);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends IHopMetadata> List<Class<T>> getMetadataClasses() {
      return List.of((Class<T>) AiProvider.class);
    }
  }
}
