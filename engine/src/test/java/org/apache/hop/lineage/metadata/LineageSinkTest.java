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

package org.apache.hop.lineage.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.lineage.hub.LineageSinkTarget;
import org.apache.hop.lineage.model.LineageEvent;
import org.apache.hop.lineage.plugin.LineageSinkPlugin;
import org.apache.hop.lineage.plugin.LineageSinkPluginType;
import org.apache.hop.lineage.spi.ILineageSink;
import org.apache.hop.metadata.serializer.json.JsonMetadataParser;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class LineageSinkTest {

  /** A sink that can only be configured with variables, as third-party sinks are today. */
  @LineageSinkPlugin(id = "variables-only", name = "Variables only")
  public static class VariablesOnlySink implements ILineageSink {
    @Override
    public void accept(List<LineageEvent> events) {
      // Nothing to deliver to
    }
  }

  @BeforeAll
  static void registerTheFakeSinks() throws Exception {
    HopClientEnvironment.init();
    PluginRegistry.addPluginType(LineageSinkPluginType.getInstance());
    PluginRegistry registry = PluginRegistry.getInstance();
    registry.registerPluginClass(
        FakeLineageSink.class.getName(), LineageSinkPluginType.class, LineageSinkPlugin.class);
    registry.registerPluginClass(
        VariablesOnlySink.class.getName(), LineageSinkPluginType.class, LineageSinkPlugin.class);
  }

  @Test
  void theSinkTypeAndItsConfigurationSurviveASaveAndLoad() throws Exception {
    JsonMetadataParser<LineageSink> parser =
        new JsonMetadataParser<>(LineageSink.class, new MemoryMetadataProvider());

    String json =
        parser
            .getJsonObject(FakeLineageSink.named("marquez", "http://marquez:5000"))
            .toJSONString();
    assertTrue(json.contains("\"fake-sink\""), json);
    assertFalse(json.contains("s3cr3t-token"), "the token is stored encoded: " + json);

    LineageSink loaded;
    try (JsonParser jsonParser = new JsonFactory().createParser(json)) {
      jsonParser.nextToken();
      loaded = parser.loadJsonObject(LineageSink.class, jsonParser);
    }
    assertEquals("A fake sink", loaded.getDescription());
    FakeLineageSink.Configuration sink =
        assertInstanceOf(FakeLineageSink.Configuration.class, loaded.getSink());
    assertEquals("fake-sink", sink.getPluginId());
    assertEquals("Fake sink", sink.getPluginName());
    assertEquals("http://marquez:5000", sink.getUrl());
    assertEquals("s3cr3t-token", sink.getToken());
  }

  @Test
  void aCloneHasItsOwnConfiguration() {
    LineageSink sink = FakeLineageSink.named("marquez", "http://marquez:5000");

    LineageSink copy = sink.clone();
    ((FakeLineageSink.Configuration) copy.getSink()).setUrl("http://other:5000");

    assertNotSame(sink.getSink(), copy.getSink());
    assertEquals("http://marquez:5000", ((FakeLineageSink.Configuration) sink.getSink()).getUrl());
  }

  @Test
  void theTargetHasTheSettingsResolvedWithTheRunsVariables() throws Exception {
    IVariables variables = new Variables();
    variables.setVariable("MARQUEZ_URL", "http://marquez:5000");

    LineageSinkTarget target =
        FakeLineageSink.named("marquez", "${MARQUEZ_URL}")
            .toTarget(variables, new MemoryMetadataProvider());

    assertEquals("marquez", target.name());
    assertEquals("fake-sink", target.pluginId());
    assertEquals(
        Map.of("FAKE_URL", "http://marquez:5000", "FAKE_TOKEN", "s3cr3t-token"), target.settings());
  }

  @Test
  void aSinkWithoutATypeHasNoTarget() {
    LineageSink sink = new LineageSink("empty", null, null);

    HopException e =
        assertThrows(
            HopException.class, () -> sink.toTarget(new Variables(), new MemoryMetadataProvider()));
    assertTrue(e.getMessage().contains("empty"), e.getMessage());
  }

  @Test
  void aTypeThatIsNotInstalledSaysSo() {
    HopException e =
        assertThrows(
            HopException.class,
            () ->
                new ILineageSinkConfiguration.LineageSinkConfigurationFactory()
                    .createObject("not-installed", null));
    assertTrue(e.getMessage().contains("not-installed"), e.getMessage());
  }

  @Test
  void aSinkThatOnlyReadsVariablesCannotBeMetadata() {
    HopException e =
        assertThrows(
            HopException.class,
            () ->
                ILineageSinkConfiguration.LineageSinkConfigurationFactory.create("variables-only"));
    assertTrue(e.getMessage().contains("only be configured with variables"), e.getMessage());
  }

  @Test
  void theKeyFollowsTheSettingsButDoesNotShowThem() {
    LineageSinkTarget target =
        new LineageSinkTarget("datahub", "datahub", Map.of("TOKEN", "s3cr3t-token", "URL", "a"));
    LineageSinkTarget same =
        new LineageSinkTarget("datahub", "datahub", Map.of("URL", "a", "TOKEN", "s3cr3t-token"));
    LineageSinkTarget otherUrl =
        new LineageSinkTarget("datahub", "datahub", Map.of("URL", "b", "TOKEN", "s3cr3t-token"));
    LineageSinkTarget otherType =
        new LineageSinkTarget(
            "datahub", "openlineage", Map.of("URL", "a", "TOKEN", "s3cr3t-token"));

    assertEquals(target.key(), same.key());
    assertFalse(target.key().equals(otherUrl.key()));
    assertFalse(target.key().equals(otherType.key()));
    assertTrue(target.key().startsWith("datahub@"), target.key());
    assertFalse(target.key().contains("s3cr3t"));
    assertFalse(target.toString().contains("s3cr3t"));
  }
}
