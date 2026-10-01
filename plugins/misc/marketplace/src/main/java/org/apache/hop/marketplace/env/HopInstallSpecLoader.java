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

import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.marketplace.config.MarketplaceSecrets;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

/** Loads and saves {@link HopInstallSpec} as YAML or JSON through HopVfs. */
public final class HopInstallSpecLoader {

  private HopInstallSpecLoader() {}

  public static HopInstallSpec load(Path file) throws HopException {
    if (file == null) {
      throw new HopException("Install spec file not found: " + file);
    }
    return load(file.toString(), null);
  }

  public static HopInstallSpec load(String filename, IVariables variables) throws HopException {
    if (StringUtils.isBlank(filename)) {
      throw new HopException("Install spec file path is required");
    }
    String resolved = HopInstallSpecFiles.resolve(filename, variables);
    try {
      FileObject fileObject = HopVfs.getFileObject(resolved, variables);
      if (fileObject == null || !fileObject.exists() || !fileObject.isFile()) {
        throw new HopException("Install spec file not found: " + resolved);
      }
      String name = fileObject.getName().getBaseName().toLowerCase(Locale.ROOT);
      try (InputStream in = HopVfs.getInputStream(fileObject)) {
        if (name.endsWith(".json")) {
          return HopJson.newMapper().readValue(in, HopInstallSpec.class);
        }
        Yaml yaml = new Yaml(new SafeConstructor(new LoaderOptions()));
        Object loaded = yaml.load(in);
        if (loaded == null) {
          return new HopInstallSpec();
        }
        if (!(loaded instanceof Map)) {
          throw new HopException("Install spec file root must be a YAML mapping: " + resolved);
        }
        return HopJson.newMapper().convertValue(loaded, HopInstallSpec.class);
      }
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException("Unable to read install spec file: " + resolved, e);
    }
  }

  /**
   * Writes {@code spec} to {@code file}. Format is JSON when the path ends with {@code .json};
   * otherwise YAML.
   */
  public static void save(Path file, HopInstallSpec spec) throws HopException {
    if (file == null) {
      throw new HopException("Install spec file path is required");
    }
    save(file.toString(), spec, null);
  }

  public static void save(String filename, HopInstallSpec spec, IVariables variables)
      throws HopException {
    if (StringUtils.isBlank(filename)) {
      throw new HopException("Install spec file path is required");
    }
    if (spec == null) {
      throw new HopException("Install specification is required");
    }
    String resolved = HopInstallSpecFiles.resolve(filename, variables);
    try {
      FileObject fileObject = HopVfs.getFileObject(resolved, variables);
      FileObject parent = fileObject.getParent();
      if (parent != null && !parent.exists()) {
        parent.createFolder();
      }
      String name = fileObject.getName().getBaseName().toLowerCase(Locale.ROOT);
      try (OutputStream out = HopVfs.getOutputStream(fileObject, false)) {
        if (name.endsWith(".json")) {
          HopJson.newMapper().writerWithDefaultPrettyPrinter().writeValue(out, spec);
          return;
        }
        Map<String, Object> map = toYamlMap(spec);
        DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        options.setPrettyFlow(true);
        options.setIndent(2);
        options.setIndicatorIndent(0);
        Yaml yaml = new Yaml(options);
        try (Writer writer = new OutputStreamWriter(out, StandardCharsets.UTF_8)) {
          yaml.dump(map, writer);
        }
      }
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException("Unable to write install spec file: " + resolved, e);
    }
  }

  /**
   * Builds a LinkedHashMap with stable field order and omits blank optional fields / empty lists.
   */
  static Map<String, Object> toYamlMap(HopInstallSpec spec) {
    Map<String, Object> root = new LinkedHashMap<>();
    root.put("version", StringUtils.defaultIfBlank(spec.getVersion(), "1.0"));
    if (StringUtils.isNotBlank(spec.getHopVersion())) {
      root.put("hopVersion", spec.getHopVersion());
    }
    root.put("enforceOnRun", spec.isEnforceOnRun());

    List<Map<String, Object>> repos = new ArrayList<>();
    for (HopInstallSpec.RepositoryRef ref : nullSafe(spec.getRepositories())) {
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
        m.put("password", MarketplaceSecrets.encode(ref.getPassword()));
      }
      repos.add(m);
    }
    if (!repos.isEmpty()) {
      root.put("repositories", repos);
    }

    List<Map<String, Object>> plugins = new ArrayList<>();
    for (HopInstallSpec.PluginRef ref : nullSafe(spec.getPlugins())) {
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
    for (HopInstallSpec.DependencyRef ref : nullSafe(spec.getDependencies())) {
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
