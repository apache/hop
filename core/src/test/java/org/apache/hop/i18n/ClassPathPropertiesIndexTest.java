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

package org.apache.hop.i18n;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ClassPathPropertiesIndexTest {

  @TempDir Path folder;

  private Path jar(String name, String classPath, String... entries) throws IOException {
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
    if (classPath != null) {
      manifest.getMainAttributes().put(Attributes.Name.CLASS_PATH, classPath);
    }
    Path jar = folder.resolve(name);
    try (OutputStream out = Files.newOutputStream(jar);
        JarOutputStream jarOut = new JarOutputStream(out, manifest)) {
      for (String entry : entries) {
        jarOut.putNextEntry(new JarEntry(entry));
        jarOut.closeEntry();
      }
    }
    return jar;
  }

  @Test
  void listsThePropertiesFilesOfTheJars() throws Exception {
    Path first = jar("first.jar", null, "a/messages/messages_en_US.properties", "a/A.class");
    Path second = jar("second.jar", null, "b/messages/messages_fr_FR.properties");
    assertEquals(
        Set.of("a/messages/messages_en_US.properties", "b/messages/messages_fr_FR.properties"),
        ClassPathPropertiesIndex.read(List.of(first.toString(), second.toString())));
  }

  @Test
  void followsManifestClassPathEntries() throws Exception {
    Files.createDirectories(folder.resolve("lib dir"));
    jar("lib dir/extra.jar", null, "c/messages/messages_en_US.properties");
    Path main = jar("main.jar", "lib%20dir/extra.jar missing.jar");
    assertEquals(
        Set.of("c/messages/messages_en_US.properties"),
        ClassPathPropertiesIndex.read(List.of(main.toString())));
  }

  @Test
  void skipsMissingEntries() throws Exception {
    Path first = jar("first.jar", null, "a/messages/messages_en_US.properties");
    assertEquals(
        Set.of("a/messages/messages_en_US.properties"),
        ClassPathPropertiesIndex.read(
            List.of(folder.resolve("nothing.jar").toString(), first.toString())));
  }

  @Test
  void aFolderOnTheClassPathTurnsTheListOff() throws Exception {
    Path first = jar("first.jar", null, "a/messages/messages_en_US.properties");
    Files.createDirectories(folder.resolve("classes"));
    assertNull(
        ClassPathPropertiesIndex.read(
            List.of(first.toString(), folder.resolve("classes").toString())));
  }
}
