/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.core.plugins;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit test for {@link HopURLClassLoader#addJar(URL)}, the hook the JDBC driver download uses to
 * hot-load a freshly installed driver into a database plugin's classloader without a restart.
 */
class HopURLClassLoaderTest {

  @Test
  void addJarWiresTheJarIntoTheClasspath(@TempDir Path tempDir) throws Exception {
    Path jar = tempDir.resolve("probe.jar");
    try (JarOutputStream jos = new JarOutputStream(Files.newOutputStream(jar))) {
      jos.putNextEntry(new JarEntry("probe/marker.txt"));
      jos.write("hello".getBytes(StandardCharsets.UTF_8));
      jos.closeEntry();
    }
    URL jarUrl = jar.toUri().toURL();

    try (HopURLClassLoader loader =
        new HopURLClassLoader(new URL[0], getClass().getClassLoader())) {
      assertEquals(0, loader.getURLs().length, "the loader starts with no jars of its own");
      assertNull(
          loader.getResource("probe/marker.txt"),
          "the jar's resource must not be reachable before it is added");

      loader.addJar(jarUrl);

      assertTrue(
          Arrays.asList(loader.getURLs()).contains(jarUrl),
          "addJar must add the jar to the loader's search path");
      assertNotNull(
          loader.getResource("probe/marker.txt"),
          "the jar's resource must be reachable after addJar");
    }
  }
}
