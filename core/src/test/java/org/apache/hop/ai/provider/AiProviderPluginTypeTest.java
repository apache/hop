/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.ai.provider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.ai.AiAuthKind;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopMissingPluginsException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.junit.jupiter.api.Test;

class AiProviderPluginTypeTest {

  @Test
  void singleton() {
    assertSame(AiProviderPluginType.getInstance(), AiProviderPluginType.getInstance());
  }

  @Test
  void extractsAnnotation() {
    AiProviderPluginType type = AiProviderPluginType.getInstance();
    AiProviderPlugin annotation = FakeProvider.class.getAnnotation(AiProviderPlugin.class);
    assertNotNull(annotation);
    assertEquals("fake-openai", type.extractID(annotation));
    assertEquals("Fake OpenAI", type.extractName(annotation));
    assertEquals("For tests", type.extractDesc(annotation));
    assertEquals("fake.svg", type.extractImageFile(annotation));
    assertEquals("/docs/fake.html", type.extractDocumentationUrl(annotation));
    assertEquals("hop-ai", type.extractClassLoaderGroup(annotation));
  }

  @Test
  void pluginTypeIds() {
    AiProviderPluginType type = AiProviderPluginType.getInstance();
    assertEquals("AI_PROVIDERS", type.getId());
    assertEquals("AI providers", type.getName());
  }

  @Test
  void objectFactoryRejectsUnknownId() {
    PluginRegistry.addPluginType(AiProviderPluginType.getInstance());
    AiProviderObjectFactory factory = new AiProviderObjectFactory();
    assertThrows(HopMissingPluginsException.class, () -> factory.createObject("missing", null));
  }

  @Test
  void objectFactoryRejectsNonProvider() {
    AiProviderObjectFactory factory = new AiProviderObjectFactory();
    HopException e = assertThrows(HopException.class, () -> factory.getObjectId("not-a-provider"));
    assertTrue(e.getMessage().contains("IAiProvider"));
  }

  @Test
  void objectFactoryReturnsPluginId() throws Exception {
    FakeProvider provider = new FakeProvider();
    provider.setPluginId("fake-openai");
    assertEquals("fake-openai", new AiProviderObjectFactory().getObjectId(provider));
  }

  @Test
  void baseProviderCloneCopiesIdentity() {
    FakeProvider original = new FakeProvider();
    original.setPluginId("fake-openai");
    original.setPluginName("Fake OpenAI");
    original.setAuthKind(AiAuthKind.NONE);
    original.setDefaultBaseUrl("http://localhost");
    original.setDefaultModelName("demo");
    original.setHopModelType("OLLAMA");
    original.setRequiresApiKey(false);

    FakeProvider copy = (FakeProvider) original.clone();
    assertEquals("fake-openai", copy.getPluginId());
    assertEquals("Fake OpenAI", copy.getPluginName());
    assertEquals(AiAuthKind.NONE, copy.getAuthKind());
    assertEquals("http://localhost", copy.getDefaultBaseUrl());
    assertEquals("demo", copy.getDefaultModelName());
    assertEquals("OLLAMA", copy.getHopModelType());
    assertFalse(copy.requiresApiKey());
  }

  @Test
  void metadataPropertyTypeExists() {
    assertEquals("AI_PROVIDER", HopMetadataPropertyType.AI_PROVIDER.name());
  }

  @AiProviderPlugin(
      id = "fake-openai",
      name = "Fake OpenAI",
      description = "For tests",
      image = "fake.svg",
      documentationUrl = "/docs/fake.html",
      classLoaderGroup = "hop-ai")
  static class FakeProvider extends BaseAiProvider {}
}
