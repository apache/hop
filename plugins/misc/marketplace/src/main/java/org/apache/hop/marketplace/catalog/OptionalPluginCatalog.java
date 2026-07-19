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

package org.apache.hop.marketplace.catalog;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.yaml.snakeyaml.Yaml;

/**
 * Catalog of marketplace-optional plugins loaded from {@code optional-plugins.yaml} on the
 * classpath (source of truth for Wave 1+ optional plugins and full-client-env generation).
 */
public final class OptionalPluginCatalog {

  public static final String REGISTRY_RESOURCE =
      "/org/apache/hop/marketplace/optional-plugins.yaml";

  private static final List<OptionalPluginInfo> PLUGINS = loadRegistry();

  private OptionalPluginCatalog() {}

  public static List<OptionalPluginInfo> listWave1() {
    return listOptional();
  }

  /** All marketplace-optional plugins from the registry. */
  public static List<OptionalPluginInfo> listOptional() {
    return PLUGINS;
  }

  public static boolean isInstalledOnDisk(Path hopHome, OptionalPluginInfo info) {
    if (info.getInstallPath() == null) {
      return false;
    }
    Path path = hopHome.resolve(info.getInstallPath());
    return Files.isDirectory(path);
  }

  @SuppressWarnings("unchecked")
  private static List<OptionalPluginInfo> loadRegistry() {
    try (InputStream in = OptionalPluginCatalog.class.getResourceAsStream(REGISTRY_RESOURCE)) {
      if (in == null) {
        return Collections.unmodifiableList(fallbackCatalog());
      }
      Yaml yaml = new Yaml();
      Object loaded = yaml.load(in);
      if (!(loaded instanceof Map)) {
        return Collections.unmodifiableList(fallbackCatalog());
      }
      Map<String, Object> root = (Map<String, Object>) loaded;
      Object pluginsObj = root.get("plugins");
      if (!(pluginsObj instanceof List)) {
        return Collections.unmodifiableList(fallbackCatalog());
      }
      List<OptionalPluginInfo> result = new ArrayList<>();
      for (Object entry : (List<?>) pluginsObj) {
        if (!(entry instanceof Map)) {
          continue;
        }
        Map<String, Object> m = (Map<String, Object>) entry;
        String artifactId = stringVal(m.get("artifactId"));
        if (StringUtils.isBlank(artifactId)) {
          continue;
        }
        result.add(
            new OptionalPluginInfo(
                artifactId,
                stringVal(m.get("name")),
                stringVal(m.get("category")),
                stringVal(m.get("description")),
                stringVal(m.get("installPath"))));
      }
      if (result.isEmpty()) {
        return Collections.unmodifiableList(fallbackCatalog());
      }
      return Collections.unmodifiableList(result);
    } catch (Exception e) {
      return Collections.unmodifiableList(fallbackCatalog());
    }
  }

  private static String stringVal(Object o) {
    return o == null ? null : String.valueOf(o);
  }

  /** Hard-coded fallback if the registry resource is missing (tests / broken packaging). */
  private static List<OptionalPluginInfo> fallbackCatalog() {
    List<OptionalPluginInfo> wave1 = new ArrayList<>();
    wave1.add(
        new OptionalPluginInfo(
            "hop-engines-spark",
            "Native Spark engine",
            "Engines",
            "Run pipelines on Apache Spark (local or cluster).",
            "plugins/engines/spark"));
    wave1.add(
        new OptionalPluginInfo(
            "hop-engines-beam",
            "Apache Beam engine",
            "Engines",
            "Beam pipeline engine and transforms (plugin only; lib/beam stays in the core install).",
            "plugins/engines/beam"));
    wave1.add(
        new OptionalPluginInfo(
            "hop-transform-script",
            "Script transform",
            "Transforms",
            "Scripting transform with embedded language runtimes.",
            "plugins/transforms/script"));
    wave1.add(
        new OptionalPluginInfo(
            "hop-tech-cassandra",
            "Cassandra",
            "Technology",
            "Apache Cassandra input/output and related tech.",
            "plugins/tech/cassandra"));
    wave1.add(
        new OptionalPluginInfo(
            "hop-transform-tika",
            "Apache Tika",
            "Transforms",
            "Extract text and metadata from documents via Apache Tika.",
            "plugins/transforms/tika"));
    wave1.add(
        new OptionalPluginInfo(
            "hop-transform-drools",
            "Drools",
            "Transforms",
            "Business rules with Drools.",
            "plugins/transforms/drools"));
    wave1.add(
        new OptionalPluginInfo(
            "hop-tech-parquet",
            "Parquet",
            "Technology",
            "Parquet file format support.",
            "plugins/tech/parquet"));
    wave1.add(
        new OptionalPluginInfo(
            "hop-transform-stanfordnlp",
            "Stanford NLP",
            "Transforms",
            "Natural language processing with Stanford NLP.",
            "plugins/transforms/stanfordnlp"));
    wave1.add(
        new OptionalPluginInfo(
            "hop-tech-arrow",
            "Apache Arrow",
            "Technology",
            "Apache Arrow integration.",
            "plugins/tech/arrow"));
    wave1.add(
        new OptionalPluginInfo(
            "hop-tech-dropbox",
            "Dropbox",
            "Technology",
            "Dropbox VFS and related support.",
            "plugins/tech/dropbox"));
    wave1.add(
        new OptionalPluginInfo(
            "hop-transform-edi2xml",
            "EDI to XML",
            "Transforms",
            "Convert EDI messages to XML.",
            "plugins/transforms/edi2xml"));
    return wave1;
  }

  /** Artifact ids only (for tests / tooling). */
  public static List<String> artifactIds() {
    List<String> ids = new ArrayList<>();
    for (OptionalPluginInfo info : PLUGINS) {
      ids.add(info.getArtifactId());
    }
    return ids;
  }

  /** Expose registry as simple maps for tests. */
  public static Map<String, String> installPathsByArtifactId() {
    Map<String, String> map = new LinkedHashMap<>();
    for (OptionalPluginInfo info : PLUGINS) {
      map.put(info.getArtifactId(), info.getInstallPath());
    }
    return map;
  }
}
