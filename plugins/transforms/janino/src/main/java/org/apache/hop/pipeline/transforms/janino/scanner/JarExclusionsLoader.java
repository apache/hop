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
package org.apache.hop.pipeline.transforms.janino.scanner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.function.Predicate;
import org.apache.xbean.finder.filter.Filters;

public class JarExclusionsLoader {
  public Predicate<String> load(String resourcePath) {
    try (final var is =
        JarExclusionsLoader.class.getClassLoader().getResourceAsStream(resourcePath)) {
      if (is == null) {
        throw new IllegalArgumentException("Resource not found: " + resourcePath);
      }
      return load(is);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public Predicate<String> load(InputStream inputStream) {
    final var ignoredPrefixes = new ArrayList<String>();
    final var notIgnoredPrefixes = new ArrayList<String>();
    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        line = line.trim();
        if (line.isEmpty() || line.startsWith("#")) {
          continue;
        }
        if (line.startsWith("!")) {
          notIgnoredPrefixes.add(line.substring(1));
        } else {
          ignoredPrefixes.add(line);
        }
      }
      if (notIgnoredPrefixes.isEmpty()) {
        final var onlyIgnored = Filters.prefixes(ignoredPrefixes.toArray(new String[0]));
        return p -> !onlyIgnored.accept(p);
      }
      final var ignored = Filters.prefixes(ignoredPrefixes.toArray(new String[0]));
      final var notIgnored = Filters.prefixes(notIgnoredPrefixes.toArray(new String[0]));
      return p -> notIgnored.accept(p) || !ignored.accept(p);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
