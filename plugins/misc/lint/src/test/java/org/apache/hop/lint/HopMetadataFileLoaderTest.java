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
package org.apache.hop.lint;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.hop.core.database.DatabaseMeta;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Metadata is addressed by the folder name under {@code metadata/}, which is the key the plugin
 * registry indexes types by. Getting that mapping right is what lets any registered type be linted
 * rather than a hard-coded list of them.
 */
public class HopMetadataFileLoaderTest {

  @Test
  public void metadataKeyIsTheFolderUnderMetadata() {
    assertEquals("rdbms", HopMetadataFileLoader.metadataKeyOf("/p/metadata/rdbms/SALES.json"));
    assertEquals(
        "pipeline-run-configuration",
        HopMetadataFileLoader.metadataKeyOf("/p/metadata/pipeline-run-configuration/local.json"));
    assertEquals("server", HopMetadataFileLoader.metadataKeyOf("/p/metadata/server/prod.json"));
  }

  /** Windows paths arrive with backslashes; matching only "/" made metadata rules no-op there. */
  @Test
  public void metadataKeyHandlesWindowsSeparators() {
    assertEquals(
        "rdbms",
        HopMetadataFileLoader.metadataKeyOf("C:\\projects\\p\\metadata\\rdbms\\SALES.json"));
  }

  @Test
  public void aFileWithNoTypeFolderHasNoKey() {
    assertNull(HopMetadataFileLoader.metadataKeyOf("/p/metadata/loose.json"));
    assertNull(HopMetadataFileLoader.metadataKeyOf("/p/pipelines/a.hpl"));
    assertNull(HopMetadataFileLoader.metadataKeyOf(null));
  }

  /** A nested project should resolve against the innermost metadata folder. */
  @Test
  public void theLastMetadataFolderWins() {
    assertEquals(
        "rdbms",
        HopMetadataFileLoader.metadataKeyOf("/outer/metadata/sub/inner/metadata/rdbms/A.json"));
  }

  @Test
  public void nameComesFromTheFileName(@TempDir Path dir) throws Exception {
    File file = dir.resolve("SALES_PRD.json").toFile();
    Files.writeString(file.toPath(), "{}", StandardCharsets.UTF_8);

    assertEquals("SALES_PRD", HopMetadataFileLoader.metadataNameOf(file));
  }

  @Test
  public void relationalConnectionsLoadWithoutAProvider(@TempDir Path dir) throws Exception {
    File file =
        metadataFile(
            dir,
            "rdbms",
            "SALES_PRD.json",
            """
        {"rdbms": {"POSTGRESQL": {
          "hostname": "db.internal", "username": "etl", "password": "hunter2", "port": "5432"
        }}}
        """);

    HopMetadataFileLoader.MetadataLoad load = HopMetadataFileLoader.read(file, null);

    assertFalse(load.isFailure());
    assertNotNull(load.object());
    DatabaseMeta databaseMeta = (DatabaseMeta) load.object();
    assertEquals("db.internal", databaseMeta.getHostname());
    // The name lives in the file name, never the document, and rules target it.
    assertEquals("SALES_PRD", databaseMeta.getName());
  }

  /**
   * A file that cannot be read has to be reported. Skipping it quietly would mean a corrupt
   * connection passes every rule and the run calls the project clean.
   */
  @Test
  public void anUnreadableConnectionIsAFailureNotASkip(@TempDir Path dir) throws Exception {
    File file = metadataFile(dir, "rdbms", "BROKEN.json", "{ \"rdbms\": { \"POSTGRESQL\": { ");

    HopMetadataFileLoader.MetadataLoad load = HopMetadataFileLoader.read(file, null);

    assertTrue(load.isFailure());
    assertNull(load.object());
    assertNotNull(load.error());
  }

  /**
   * Without a provider, types other than rdbms are skipped rather than failed: the plugin class is
   * what gives the document meaning, and its absence is not the project's fault.
   */
  @Test
  public void otherTypesAreSkippedWithoutAProvider(@TempDir Path dir) throws Exception {
    File file = metadataFile(dir, "server", "prod.json", "{\"hostname\": \"h\"}");

    HopMetadataFileLoader.MetadataLoad load = HopMetadataFileLoader.read(file, null);

    assertFalse(load.isFailure(), "a missing provider is not the project's fault");
    assertNull(load.object());
  }

  @Test
  public void nonMetadataFilesAreSkipped(@TempDir Path dir) throws Exception {
    File file = dir.resolve("a.hpl").toFile();
    Files.writeString(file.toPath(), "<pipeline/>", StandardCharsets.UTF_8);

    HopMetadataFileLoader.MetadataLoad load = HopMetadataFileLoader.read(file, null);

    assertFalse(load.isFailure());
    assertNull(load.object());
  }

  private File metadataFile(Path dir, String type, String name, String content) throws Exception {
    Path folder = dir.resolve("metadata").resolve(type);
    Files.createDirectories(folder);
    Path file = folder.resolve(name);
    Files.writeString(file, content, StandardCharsets.UTF_8);
    return file.toFile();
  }
}
