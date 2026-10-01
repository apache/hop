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

package org.apache.hop.core.metadata;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.encryption.HopTwoWayPasswordEncoder;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.serializer.json.JsonMetadataProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Metadata is serialized to JSON, keyed by metadata type, to ship it to a Hop server or another
 * engine. Client and server can be different versions of Hop, so both keys of a renamed type have
 * to be understood (issue #5597).
 */
class SerializableMetadataLegacyKeysTest {

  /** A metadata type which was renamed from key "RenamedType" to "renamed-type". */
  @Getter
  @Setter
  @HopMetadata(
      name = "Renamed type",
      key = "renamed-type",
      legacyKeys = {"RenamedType"})
  public static class RenamedType extends HopMetadataBase implements IHopMetadata {
    @HopMetadataProperty private String description;

    public RenamedType() {}
  }

  /** An unknown type in an export is logged and skipped. */
  @BeforeAll
  static void initLogging() {
    HopLogStore.init();
  }

  /** The metadata types known in these tests, instead of the plugin registry. */
  private static <T extends IHopMetadata> List<Class<T>> testTypes() {
    return List.of((Class<T>) (Class<?>) RenamedType.class);
  }

  private static SerializableMetadataProvider fromJson(String json) throws HopException {
    return new SerializableMetadataProvider(json) {
      @Override
      public <T extends IHopMetadata> List<Class<T>> getMetadataClasses() {
        return testTypes();
      }
    };
  }

  private static List<String> sortedNames(IHopMetadataSerializer<?> serializer)
      throws HopException {
    List<String> names = new ArrayList<>(serializer.listObjectNames());
    names.sort(String::compareTo);
    return names;
  }

  /** What an older Hop client sends: the objects under the legacy key. */
  @Test
  void testExportWithLegacyKeyIsLoaded() throws Exception {
    String json = "{\"RenamedType\":[{\"name\":\"old\",\"description\":\"from an older client\"}]}";

    SerializableMetadataProvider provider = fromJson(json);

    IHopMetadataSerializer<RenamedType> serializer = provider.getSerializer(RenamedType.class);
    assertEquals(List.of("old"), serializer.listObjectNames());
    assertEquals("from an older client", serializer.load("old").getDescription());
  }

  @Test
  void testExportWithCurrentKeyIsLoaded() throws Exception {
    String json = "{\"renamed-type\":[{\"name\":\"new\",\"description\":\"current\"}]}";

    SerializableMetadataProvider provider = fromJson(json);

    assertEquals("current", provider.getSerializer(RenamedType.class).load("new").getDescription());
  }

  /** Should both keys ever end up in one export, the objects of both are loaded. */
  @Test
  void testExportWithBothKeysIsMerged() throws Exception {
    String json =
        "{\"RenamedType\":[{\"name\":\"a\",\"description\":\"legacy\"}],"
            + "\"renamed-type\":[{\"name\":\"b\",\"description\":\"current\"}]}";

    SerializableMetadataProvider provider = fromJson(json);

    assertEquals(List.of("a", "b"), sortedNames(provider.getSerializer(RenamedType.class)));
  }

  @Test
  void testExportWithUnknownKeyIsStillSkipped() throws Exception {
    String json =
        "{\"NoSuchType\":[{\"name\":\"x\"}],"
            + "\"RenamedType\":[{\"name\":\"a\",\"description\":\"legacy\"}]}";

    SerializableMetadataProvider provider = fromJson(json);

    assertEquals(List.of("a"), provider.getSerializer(RenamedType.class).listObjectNames());
  }

  /**
   * Exporting a project which is halfway its migration ships every object once, the current copy of
   * an object in both folders, under the current key.
   */
  @Test
  void testExportOfMixedProject(@TempDir Path folder) throws Exception {
    write(folder.resolve("RenamedType"), "legacy-only", "legacy");
    write(folder.resolve("RenamedType"), "both", "outdated legacy copy");
    write(folder.resolve("renamed-type"), "both", "current copy");
    write(folder.resolve("renamed-type"), "current-only", "current");

    JsonMetadataProvider project =
        new JsonMetadataProvider(
            new HopTwoWayPasswordEncoder(),
            folder.toString(),
            Variables.getADefaultVariableSpace()) {
          @Override
          public <T extends IHopMetadata> List<Class<T>> getMetadataClasses() {
            return testTypes();
          }
        };

    SerializableMetadataProvider exported =
        new SerializableMetadataProvider(project) {
          @Override
          public <T extends IHopMetadata> List<Class<T>> getMetadataClasses() {
            return testTypes();
          }
        };
    IHopMetadataSerializer<RenamedType> exportedSerializer =
        exported.getSerializer(RenamedType.class);
    assertEquals(List.of("both", "current-only", "legacy-only"), sortedNames(exportedSerializer));
    assertEquals("current copy", exportedSerializer.load("both").getDescription());

    String json = exported.toJson();
    assertTrue(json.contains("\"renamed-type\""), json);
    assertFalse(json.contains("\"RenamedType\""), json);
    assertFalse(json.contains("outdated legacy copy"), json);

    // What the server makes of it.
    IHopMetadataSerializer<RenamedType> received = fromJson(json).getSerializer(RenamedType.class);
    assertEquals(List.of("both", "current-only", "legacy-only"), sortedNames(received));
    assertEquals("current copy", received.load("both").getDescription());
    assertEquals("legacy", received.load("legacy-only").getDescription());

    // Exporting reads, it never moves files around in the project.
    assertTrue(Files.exists(folder.resolve("RenamedType").resolve("legacy-only.json")));
    assertTrue(Files.exists(folder.resolve("RenamedType").resolve("both.json")));
  }

  private static void write(Path typeFolder, String name, String description) throws Exception {
    Files.createDirectories(typeFolder);
    Files.write(
        typeFolder.resolve(name + ".json"),
        ("{\"name\":\"" + name + "\",\"description\":\"" + description + "\"}").getBytes(UTF_8));
  }
}
