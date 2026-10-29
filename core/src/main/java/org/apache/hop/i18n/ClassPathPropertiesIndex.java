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

package org.apache.hop.i18n;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;

/**
 * The properties files on the system class path. Most candidate message bundles exist nowhere, and
 * looking one up searches every jar on the class path; with this list such a lookup ends at once.
 * The list is read from the jars at startup, in the background, so it always matches what is on
 * disk. Until it is ready, and when the class path holds something it can not list, every lookup
 * searches the class path as before.
 */
final class ClassPathPropertiesIndex {

  private static volatile Set<String> names;

  static {
    Thread thread =
        new Thread(
            () ->
                names =
                    read(
                        List.of(
                            System.getProperty("java.class.path", "").split(File.pathSeparator))),
            "Hop class path properties index");
    thread.setDaemon(true);
    thread.start();
  }

  private ClassPathPropertiesIndex() {}

  /**
   * @param resourceName a properties file, as a resource name
   * @return true if the file is known not to be on the system class path, false if it is there or
   *     the list is not available
   */
  static boolean isAbsent(String resourceName) {
    Set<String> known = names;
    return known != null && !known.contains(resourceName);
  }

  /**
   * Lists the properties files in the given class path entries and in the jars their manifests add,
   * as the system class loader sees them.
   *
   * @return the resource names, or null if an entry can not be listed
   */
  static Set<String> read(List<String> classPath) {
    Set<String> found = new HashSet<>();
    Set<File> seen = new HashSet<>();
    Deque<File> todo = new ArrayDeque<>();
    for (String entry : classPath) {
      if (!entry.isEmpty()) {
        todo.add(new File(entry).getAbsoluteFile());
      }
    }
    while (!todo.isEmpty()) {
      File file = todo.poll();
      if (!seen.add(file) || !file.exists()) {
        // The class loader skips a missing entry as well
        continue;
      }
      if (!file.isFile()) {
        // A folder (a development setup): not listed, so no lookup is skipped
        return null;
      }
      try (JarFile jar = new JarFile(file, false)) {
        Enumeration<? extends ZipEntry> entries = jar.entries();
        while (entries.hasMoreElements()) {
          String name = entries.nextElement().getName();
          if (name.endsWith(".properties")) {
            found.add(name);
          }
        }
        Manifest manifest = jar.getManifest();
        String extra =
            manifest == null
                ? null
                : manifest.getMainAttributes().getValue(Attributes.Name.CLASS_PATH);
        if (extra != null) {
          for (String url : extra.trim().split("\\s+")) {
            if (url.isEmpty()) {
              continue;
            }
            // URLs relative to the jar, as the class loader reads them
            URI uri = file.getParentFile().toURI().resolve(url);
            if (!"file".equals(uri.getScheme())) {
              return null;
            }
            todo.add(new File(uri).getAbsoluteFile());
          }
        }
      } catch (IOException | IllegalArgumentException e) {
        return null;
      }
    }
    return found;
  }
}
