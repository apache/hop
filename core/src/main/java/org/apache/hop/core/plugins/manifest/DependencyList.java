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
package org.apache.hop.core.plugins.manifest;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.Getter;

/**
 * The resolved dependencies of a Maven module. The Maven goal {@code hop:manifest} passes them in
 * directly ({@link #of}); {@link #parse} reads them as written by {@code dependency:list} with
 * {@code outputAbsoluteArtifactFilename=true}, where each line looks like
 *
 * <pre>
 *    groupId:artifactId:type[:classifier]:version:scope:/path/to/file.jar -- module name (auto)
 * </pre>
 *
 * <p>The build uses it to put Maven coordinates on the jars found in a plugin zip. The parser
 * tolerates the header line, ANSI colour codes and the JPMS module suffix newer plugin versions
 * append.
 */
@Getter
public class DependencyList {

  /** One resolved artifact. */
  public record Artifact(
      String groupId,
      String artifactId,
      String type,
      String classifier,
      String version,
      String scope,
      String file) {

    public String ga() {
      return groupId + ":" + artifactId;
    }

    /** File name as an assembly writes it: {@code artifactId-version[-classifier].type}. */
    public String defaultFileName() {
      String name = artifactId + "-" + version;
      if (classifier != null && !classifier.isEmpty()) {
        name += "-" + classifier;
      }
      return name + "." + ("test-jar".equals(type) ? "jar" : type);
    }

    /** Base name of the resolved file, or the assembly default when no file was recorded. */
    public String fileName() {
      if (file == null || file.isBlank()) {
        return defaultFileName();
      }
      String normalized = file.replace('\\', '/');
      return normalized.substring(normalized.lastIndexOf('/') + 1);
    }
  }

  private static final Pattern ANSI = Pattern.compile("\\u001B\\[[0-9;]*m");

  // g:a:type[:classifier]:version:scope[:file]  -- the scope alternation anchors the optional
  // classifier, and the file may itself contain ':' (Windows drive letters).
  private static final Pattern LINE =
      Pattern.compile(
          "^([^:\\s]+):([^:\\s]+):([^:\\s]+)(?::([^:\\s]+))?:([^:\\s]+)"
              + ":(compile|provided|runtime|test|system|import)(?::(.+))?$");

  private final List<Artifact> artifacts = new ArrayList<>();
  private final Map<String, Artifact> byFileName = new HashMap<>();
  private final Map<String, Artifact> byGa = new HashMap<>();

  public static DependencyList parse(Path file) throws IOException {
    return parse(Files.readString(file, StandardCharsets.UTF_8));
  }

  public static DependencyList parse(String content) {
    DependencyList list = new DependencyList();
    for (String rawLine : content.split("\\R")) {
      String line = ANSI.matcher(rawLine).replaceAll("");
      int moduleSuffix = line.indexOf(" -- ");
      if (moduleSuffix >= 0) {
        line = line.substring(0, moduleSuffix);
      }
      line = line.trim();
      if (line.isEmpty()) {
        continue;
      }
      Matcher m = LINE.matcher(line);
      if (!m.matches()) {
        continue; // header line, "none", or anything else that is not an artifact
      }
      Artifact artifact =
          new Artifact(
              m.group(1), m.group(2), m.group(3), m.group(4), m.group(5), m.group(6), m.group(7));
      list.add(artifact);
    }
    return list;
  }

  /** A list of artifacts resolved some other way, such as by a Maven goal. */
  public static DependencyList of(Collection<Artifact> artifacts) {
    DependencyList list = new DependencyList();
    artifacts.forEach(list::add);
    return list;
  }

  /** An empty list, for builds that did not record their dependencies. */
  public static DependencyList empty() {
    return new DependencyList();
  }

  private void add(Artifact artifact) {
    artifacts.add(artifact);
    byFileName.putIfAbsent(artifact.fileName(), artifact);
    byFileName.putIfAbsent(artifact.defaultFileName(), artifact);
    byGa.putIfAbsent(artifact.ga(), artifact);
  }

  /** The artifact whose resolved (or assembly-default) file name matches, or {@code null}. */
  public Artifact findByFileName(String fileName) {
    return byFileName.get(fileName);
  }

  /** The artifact with these coordinates, or {@code null}. */
  public Artifact findByGa(String groupId, String artifactId) {
    return byGa.get(groupId + ":" + artifactId);
  }
}
