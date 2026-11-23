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

package org.apache.hop.airflow.launcher;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BundleManifestTest {

  @TempDir Path folder;

  static Path writeStub(Path folder, String... nameValues) throws IOException {
    Manifest manifest = new Manifest();
    Attributes attributes = manifest.getMainAttributes();
    attributes.put(Attributes.Name.MANIFEST_VERSION, "1.0");
    for (int i = 0; i < nameValues.length; i += 2) {
      attributes.putValue(nameValues[i], nameValues[i + 1]);
    }
    Path stub = folder.resolve(HopAirflowBundle.STUB_FILE_NAME);
    try (OutputStream out = Files.newOutputStream(stub);
        JarOutputStream jar = new JarOutputStream(out, manifest)) {
      jar.flush();
    }
    return stub;
  }

  @Test
  void readsTheAttributesWithDefaults() throws Exception {
    Path stub =
        writeStub(
            folder,
            HopAirflowBundle.ATTRIBUTE_PROJECT_NAME,
            "My project",
            HopAirflowBundle.ATTRIBUTE_SCHEMA_VERSION,
            "2026-10-30");

    BundleManifest bundle = BundleManifest.read(stub);

    assertEquals(folder.toAbsolutePath(), bundle.folder());
    assertEquals("My project", bundle.projectName());
    assertEquals("2026-10-30", bundle.schemaVersion());
    assertEquals("local", bundle.runConfiguration());
    assertEquals("Basic", bundle.logLevel());
    assertEquals(List.of(), bundle.workflowPatterns());
    assertEquals(List.of(), bundle.environmentFiles());
    assertEquals("My_project__", bundle.dagIdPrefix());
  }

  @Test
  void readsTheListsAndOverrides() throws Exception {
    Path stub =
        writeStub(
            folder,
            HopAirflowBundle.ATTRIBUTE_PROJECT_NAME,
            "sales",
            HopAirflowBundle.ATTRIBUTE_RUN_CONFIGURATION,
            "remote",
            HopAirflowBundle.ATTRIBUTE_LOG_LEVEL,
            "Detailed",
            HopAirflowBundle.ATTRIBUTE_WORKFLOWS,
            "main-*.hwf, etl/**.hwf",
            HopAirflowBundle.ATTRIBUTE_ENVIRONMENT_FILES,
            "env/prod.json;env/secrets.json",
            HopAirflowBundle.ATTRIBUTE_DAG_ID_PREFIX,
            "hop_");

    BundleManifest bundle = BundleManifest.read(stub);

    assertEquals("remote", bundle.runConfiguration());
    assertEquals("Detailed", bundle.logLevel());
    assertEquals(List.of("main-*.hwf", "etl/**.hwf"), bundle.workflowPatterns());
    assertEquals(List.of("env/prod.json", "env/secrets.json"), bundle.environmentFiles());
    assertEquals("hop_", bundle.dagIdPrefix());
  }

  @Test
  void needsAProjectName() throws Exception {
    Path stub = writeStub(folder, HopAirflowBundle.ATTRIBUTE_SCHEMA_VERSION, "2026-10-30");

    IOException e = assertThrows(IOException.class, () -> BundleManifest.read(stub));
    assertTrue(e.getMessage().contains(HopAirflowBundle.ATTRIBUTE_PROJECT_NAME), e.getMessage());
  }

  @Test
  void findsTheStubThroughTheSystemProperty() throws Exception {
    writeStub(folder, HopAirflowBundle.ATTRIBUTE_PROJECT_NAME, "p");
    String previous = System.getProperty(BundleManifest.PROPERTY_BUNDLE);
    System.setProperty(BundleManifest.PROPERTY_BUNDLE, folder.toString());
    try {
      assertEquals("p", BundleManifest.locate().projectName());
    } finally {
      if (previous == null) {
        System.clearProperty(BundleManifest.PROPERTY_BUNDLE);
      } else {
        System.setProperty(BundleManifest.PROPERTY_BUNDLE, previous);
      }
    }
  }

  @Test
  void rejectsAStubForAnotherSchema() throws Exception {
    Path stub =
        writeStub(
            folder,
            HopAirflowBundle.ATTRIBUTE_PROJECT_NAME,
            "p",
            HopAirflowBundle.ATTRIBUTE_SCHEMA_VERSION,
            "2026-01-01",
            HopAirflowBundle.ATTRIBUTE_HOP_VERSION,
            "2.21.0");
    BundleManifest bundle = BundleManifest.read(stub);

    bundle.checkSchemaVersion("2026-01-01");
    IllegalStateException e =
        assertThrows(IllegalStateException.class, () -> bundle.checkSchemaVersion("2026-10-30"));
    assertTrue(e.getMessage().contains("hop airflow bundle"), e.getMessage());
    assertTrue(e.getMessage().contains("2.21.0"), e.getMessage());
  }

  /** The SDK jar carries its schema version, so Hop does not have to keep a copy of it. */
  @Test
  void readsTheSchemaVersionOfTheSdk() throws Exception {
    assertTrue(
        BundleManifest.sdkSchemaVersion().matches("\\d{4}-\\d{2}-\\d{2}"),
        BundleManifest.sdkSchemaVersion());
  }
}
