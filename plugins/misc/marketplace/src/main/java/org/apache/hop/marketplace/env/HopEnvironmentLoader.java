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

package org.apache.hop.marketplace.env;

import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.json.HopJson;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

/** Loads and saves {@link HopEnvironmentSpec} as YAML or JSON. */
public final class HopEnvironmentLoader {

  private HopEnvironmentLoader() {}

  public static HopEnvironmentSpec load(Path file) throws HopException {
    if (file == null || !Files.isRegularFile(file)) {
      throw new HopException("Environment file not found: " + file);
    }
    String name = file.getFileName().toString().toLowerCase(Locale.ROOT);
    try (InputStream in = Files.newInputStream(file)) {
      if (name.endsWith(".json")) {
        return HopJson.newMapper().readValue(in, HopEnvironmentSpec.class);
      }
      // default: YAML (.yaml / .yml / anything else)
      Yaml yaml = new Yaml(new SafeConstructor(new LoaderOptions()));
      Object loaded = yaml.load(in);
      if (loaded == null) {
        return new HopEnvironmentSpec();
      }
      if (!(loaded instanceof Map)) {
        throw new HopException("Environment file root must be a YAML mapping: " + file);
      }
      return HopJson.newMapper().convertValue(loaded, HopEnvironmentSpec.class);
    } catch (IOException e) {
      throw new HopException("Unable to read environment file: " + file, e);
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException("Unable to parse environment file: " + file, e);
    }
  }

  /**
   * Writes {@code spec} to {@code file}. Format is JSON when the path ends with {@code .json};
   * otherwise YAML.
   */
  public static void save(Path file, HopEnvironmentSpec spec) throws HopException {
    if (file == null) {
      throw new HopException("Environment file path is required");
    }
    if (spec == null) {
      throw new HopException("Environment specification is required");
    }
    try {
      Path parent = file.getParent();
      if (parent != null) {
        Files.createDirectories(parent);
      }
      String name = file.getFileName().toString().toLowerCase(Locale.ROOT);
      if (name.endsWith(".json")) {
        HopJson.newMapper().writerWithDefaultPrettyPrinter().writeValue(file.toFile(), spec);
        return;
      }
      Map<String, Object> map = toYamlMap(spec);
      DumperOptions options = new DumperOptions();
      options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
      options.setPrettyFlow(true);
      options.setIndent(2);
      options.setIndicatorIndent(0);
      Yaml yaml = new Yaml(options);
      try (Writer writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
        yaml.dump(map, writer);
      }
    } catch (Exception e) {
      throw new HopException("Unable to write environment file: " + file, e);
    }
  }

  /**
   * Builds a LinkedHashMap with stable field order and omits blank optional fields / empty lists.
   */
  static Map<String, Object> toYamlMap(HopEnvironmentSpec spec) {
    Map<String, Object> root = new LinkedHashMap<>();
    root.put("version", StringUtils.defaultIfBlank(spec.getVersion(), "1.0"));
    if (StringUtils.isNotBlank(spec.getHopVersion())) {
      root.put("hopVersion", spec.getHopVersion());
    }
    root.put("enforceOnRun", spec.isEnforceOnRun());

    List<Map<String, Object>> repos = new ArrayList<>();
    for (HopEnvironmentSpec.RepositoryRef ref : nullSafe(spec.getRepositories())) {
      if (ref == null || (StringUtils.isBlank(ref.getId()) && StringUtils.isBlank(ref.getUrl()))) {
        continue;
      }
      Map<String, Object> m = new LinkedHashMap<>();
      if (StringUtils.isNotBlank(ref.getId())) {
        m.put("id", ref.getId());
      }
      if (StringUtils.isNotBlank(ref.getUrl())) {
        m.put("url", ref.getUrl());
      }
      if (StringUtils.isNotBlank(ref.getUsername())) {
        m.put("username", ref.getUsername());
      }
      if (StringUtils.isNotBlank(ref.getPassword())) {
        m.put("password", ref.getPassword());
      }
      repos.add(m);
    }
    if (!repos.isEmpty()) {
      root.put("repositories", repos);
    }

    List<Map<String, Object>> plugins = new ArrayList<>();
    for (HopEnvironmentSpec.PluginRef ref : nullSafe(spec.getPlugins())) {
      if (ref == null || StringUtils.isBlank(ref.getArtifactId())) {
        continue;
      }
      Map<String, Object> m = new LinkedHashMap<>();
      if (StringUtils.isNotBlank(ref.getGroupId())) {
        m.put("groupId", ref.getGroupId());
      }
      m.put("artifactId", ref.getArtifactId());
      if (StringUtils.isNotBlank(ref.getVersion())) {
        m.put("version", ref.getVersion());
      }
      plugins.add(m);
    }
    if (!plugins.isEmpty()) {
      root.put("plugins", plugins);
    }

    List<Map<String, Object>> deps = new ArrayList<>();
    for (HopEnvironmentSpec.DependencyRef ref : nullSafe(spec.getDependencies())) {
      if (ref == null
          || StringUtils.isAnyBlank(ref.getGroupId(), ref.getArtifactId(), ref.getVersion())) {
        continue;
      }
      Map<String, Object> m = new LinkedHashMap<>();
      m.put("groupId", ref.getGroupId());
      m.put("artifactId", ref.getArtifactId());
      m.put("version", ref.getVersion());
      String target = StringUtils.defaultIfBlank(ref.getTarget(), "lib/jdbc");
      if (!"lib/jdbc".equals(target)) {
        m.put("target", target);
      } else if (StringUtils.isNotBlank(ref.getTarget())) {
        // keep explicit default if user set it
        m.put("target", target);
      }
      deps.add(m);
    }
    if (!deps.isEmpty()) {
      root.put("dependencies", deps);
    }
    return root;
  }

  private static <T> List<T> nullSafe(List<T> list) {
    return list == null ? List.of() : list;
  }
}
