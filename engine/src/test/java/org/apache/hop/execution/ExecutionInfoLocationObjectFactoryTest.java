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
 *
 */

package org.apache.hop.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.execution.local.FileExecutionInfoLocation;
import org.apache.hop.execution.plugin.ExecutionInfoLocationPlugin;
import org.apache.hop.execution.plugin.ExecutionInfoLocationPluginType;
import org.apache.hop.metadata.serializer.json.JsonMetadataParser;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Saving execution metadata asks the location factory for a plugin id and refuses to write the JSON
 * when that id is empty. A location built with {@code new} (as NeoExecutionInfoLocationSlowSaveIT
 * does) has no id yet; the factory has to take it from the registered plugin class.
 */
class ExecutionInfoLocationObjectFactoryTest {

  @BeforeAll
  static void registerFileLocation() throws Exception {
    PluginRegistry registry = PluginRegistry.getInstance();
    registry.registerPluginType(ExecutionInfoLocationPluginType.class);
    if (registry.findPluginWithId(ExecutionInfoLocationPluginType.class, "local-folder") == null) {
      registry.registerPluginClass(
          FileExecutionInfoLocation.class.getName(),
          ExecutionInfoLocationPluginType.class,
          ExecutionInfoLocationPlugin.class);
    }
  }

  @Test
  void serializesLocationThatHasNoPluginIdYet() throws Exception {
    FileExecutionInfoLocation fileLocation = new FileExecutionInfoLocation();
    fileLocation.setRootFolder("/tmp/hop-exec-info");
    assertNull(fileLocation.getPluginId());
    assertNull(fileLocation.getPluginName());

    ExecutionInfoLocation location = new ExecutionInfoLocation();
    location.setName("files");
    location.setExecutionInfoLocation(fileLocation);

    MemoryMetadataProvider provider = new MemoryMetadataProvider();
    JsonMetadataParser<ExecutionInfoLocation> parser =
        new JsonMetadataParser<>(ExecutionInfoLocation.class, provider);
    String json = parser.getJsonObject(location).toJSONString();

    assertEquals("local-folder", fileLocation.getPluginId());
    assertEquals("File location", fileLocation.getPluginName());
    assertTrue(json.contains("\"local-folder\""));
    assertTrue(json.contains("\"pluginId\":\"local-folder\""));
    assertFalse(json.contains("\"null\":"));

    ExecutionInfoLocation loaded;
    try (JsonParser jsonParser = new JsonFactory().createParser(json)) {
      jsonParser.nextToken();
      loaded = parser.loadJsonObject(ExecutionInfoLocation.class, jsonParser);
    }

    assertEquals("files", loaded.getName());
    FileExecutionInfoLocation loadedFile =
        assertInstanceOf(FileExecutionInfoLocation.class, loaded.getExecutionInfoLocation());
    assertEquals("local-folder", loadedFile.getPluginId());
    assertEquals("File location", loadedFile.getPluginName());
    assertEquals("/tmp/hop-exec-info", loadedFile.getRootFolder());
    assertTrue(loadedFile.isCreateParentFolder());
  }

  @Test
  void keepsAnIdThatWasAlreadySet() throws Exception {
    FileExecutionInfoLocation fileLocation = new FileExecutionInfoLocation();
    fileLocation.setPluginId("already-set");
    fileLocation.setPluginName("Already");
    fileLocation.setRootFolder("/tmp/kept");

    ExecutionInfoLocation location = new ExecutionInfoLocation();
    location.setName("files");
    location.setExecutionInfoLocation(fileLocation);

    JsonMetadataParser<ExecutionInfoLocation> parser =
        new JsonMetadataParser<>(ExecutionInfoLocation.class, new MemoryMetadataProvider());
    String json = parser.getJsonObject(location).toJSONString();

    assertEquals("already-set", fileLocation.getPluginId());
    assertEquals("Already", fileLocation.getPluginName());
    assertTrue(json.contains("\"already-set\""));
    assertFalse(json.contains("local-folder"));
  }

  @Test
  void stillRefusesALocationWhosePluginCannotBeResolved() {
    IExecutionInfoLocation unknown = mock(IExecutionInfoLocation.class);
    ExecutionInfoLocation location = new ExecutionInfoLocation();
    location.setName("unknown");
    location.setExecutionInfoLocation(unknown);

    JsonMetadataParser<ExecutionInfoLocation> parser =
        new JsonMetadataParser<>(ExecutionInfoLocation.class, new MemoryMetadataProvider());
    HopException thrown = assertThrows(HopException.class, () -> parser.getJsonObject(location));

    StringBuilder messages = new StringBuilder();
    for (Throwable cause = thrown; cause != null; cause = cause.getCause()) {
      if (cause.getMessage() != null) {
        messages.append(cause.getMessage()).append('\n');
      }
    }
    assertTrue(messages.toString().contains("Unable to determine object ID"));
    assertTrue(messages.toString().contains(unknown.getClass().getName()));
  }
}
