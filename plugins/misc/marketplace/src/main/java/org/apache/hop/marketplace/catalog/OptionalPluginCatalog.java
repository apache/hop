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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Built-in catalog of Wave 1 optional plugins until remote Maven search is available in the GUI.
 */
public final class OptionalPluginCatalog {

  private static final List<OptionalPluginInfo> WAVE1 = new ArrayList<>();

  static {
    WAVE1.add(
        new OptionalPluginInfo(
            "hop-engines-spark",
            "Native Spark engine",
            "Engines",
            "Run pipelines on Apache Spark (local or cluster).",
            "plugins/engines/spark"));
    WAVE1.add(
        new OptionalPluginInfo(
            "hop-engines-beam",
            "Apache Beam engine",
            "Engines",
            "Beam pipeline engine and transforms (plugin only; lib/beam stays in the core install).",
            "plugins/engines/beam"));
    WAVE1.add(
        new OptionalPluginInfo(
            "hop-transform-script",
            "Script transform",
            "Transforms",
            "Scripting transform with embedded language runtimes.",
            "plugins/transforms/script"));
    WAVE1.add(
        new OptionalPluginInfo(
            "hop-tech-cassandra",
            "Cassandra",
            "Technology",
            "Apache Cassandra input/output and related tech.",
            "plugins/tech/cassandra"));
    WAVE1.add(
        new OptionalPluginInfo(
            "hop-transform-tika",
            "Apache Tika",
            "Transforms",
            "Extract text and metadata from documents via Apache Tika.",
            "plugins/transforms/tika"));
    WAVE1.add(
        new OptionalPluginInfo(
            "hop-transform-drools",
            "Drools",
            "Transforms",
            "Business rules with Drools.",
            "plugins/transforms/drools"));
    WAVE1.add(
        new OptionalPluginInfo(
            "hop-tech-parquet",
            "Parquet",
            "Technology",
            "Parquet file format support.",
            "plugins/tech/parquet"));
    WAVE1.add(
        new OptionalPluginInfo(
            "hop-transform-stanfordnlp",
            "Stanford NLP",
            "Transforms",
            "Natural language processing with Stanford NLP.",
            "plugins/transforms/stanfordnlp"));
    WAVE1.add(
        new OptionalPluginInfo(
            "hop-tech-arrow",
            "Apache Arrow",
            "Technology",
            "Apache Arrow integration.",
            "plugins/tech/arrow"));
    WAVE1.add(
        new OptionalPluginInfo(
            "hop-tech-dropbox",
            "Dropbox",
            "Technology",
            "Dropbox VFS and related support.",
            "plugins/tech/dropbox"));
    WAVE1.add(
        new OptionalPluginInfo(
            "hop-transform-edi2xml",
            "EDI to XML",
            "Transforms",
            "Convert EDI messages to XML.",
            "plugins/transforms/edi2xml"));
  }

  private OptionalPluginCatalog() {}

  public static List<OptionalPluginInfo> listWave1() {
    return Collections.unmodifiableList(WAVE1);
  }

  public static boolean isInstalledOnDisk(Path hopHome, OptionalPluginInfo info) {
    if (info.getInstallPath() == null) {
      return false;
    }
    Path path = hopHome.resolve(info.getInstallPath());
    return Files.isDirectory(path);
  }
}
