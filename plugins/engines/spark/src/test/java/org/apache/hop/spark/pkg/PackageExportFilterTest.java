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

package org.apache.hop.spark.pkg;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PackageExportFilterTest {

  @TempDir File tempDir;

  @Test
  void defaultsSkipWorkAndDatasets() {
    PackageExportFilter f = PackageExportFilter.empty();
    assertTrue(f.shouldSkipRelative("work/fat.jar"));
    assertTrue(f.shouldSkipRelative("datasets/big.csv"));
    assertFalse(f.shouldSkipRelative("pipelines/a.hpl"));
    assertFalse(f.shouldSkipRelative("mappings/child.hpl"));
  }

  @Test
  void includePathsOverrideWorkSkip() {
    PackageExportFilter f =
        new PackageExportFilter(List.of(), List.of(), List.of("work/cluster-env.json"), false);
    assertFalse(f.shouldSkipRelative("work/cluster-env.json"));
    assertTrue(f.shouldSkipRelative("work/hop-native.jar"));
  }

  @Test
  void excludeGlobsSkipJars() {
    PackageExportFilter f =
        new PackageExportFilter(List.of(), List.of("**/*.jar"), List.of(), false);
    assertTrue(f.shouldSkipRelative("lib/extra.jar"));
    assertFalse(f.shouldSkipRelative("lib/readme.txt"));
  }

  @Test
  void extraExcludeDirsMerged() {
    PackageExportFilter f =
        new PackageExportFilter(List.of("tmp", "screenshots"), List.of(), List.of(), false);
    assertTrue(f.shouldSkipRelative("tmp/x"));
    assertTrue(f.shouldSkipRelative("screenshots/a.png"));
    assertTrue(f.shouldSkipRelative("work/j.jar")); // still default
  }

  @Test
  void replaceDefaultsDropsWorkUnlessListed() {
    PackageExportFilter f =
        new PackageExportFilter(List.of("datasets"), List.of(), List.of(), true);
    assertFalse(f.shouldSkipRelative("work/fat.jar"));
    assertTrue(f.shouldSkipRelative("datasets/a.csv"));
  }

  @Test
  void loadSaveRoundTripAndExportRespectsConfig() throws Exception {
    File projectHome = new File(tempDir, "proj");
    assertTrue(projectHome.mkdirs());
    File pipelines = new File(projectHome, "pipelines");
    assertTrue(pipelines.mkdirs());
    Files.writeString(new File(pipelines, "p.hpl").toPath(), "<p/>", StandardCharsets.UTF_8);
    File tmp = new File(projectHome, "tmp");
    assertTrue(tmp.mkdirs());
    Files.writeString(new File(tmp, "x.txt").toPath(), "nope", StandardCharsets.UTF_8);
    File work = new File(projectHome, "work");
    assertTrue(work.mkdirs());
    Files.writeString(new File(work, "keep-me.json").toPath(), "{}", StandardCharsets.UTF_8);
    Files.writeString(new File(work, "skip.jar").toPath(), "jar", StandardCharsets.UTF_8);

    PackageExportFilter cfg =
        new PackageExportFilter(List.of("tmp"), List.of(), List.of("work/keep-me.json"), false);
    PackageExportFilter.saveToProjectHome(projectHome.getAbsolutePath(), cfg);
    assertTrue(new File(projectHome, PackageExportFilter.CONFIG_FILENAME).isFile());

    File zip = new File(tempDir, "out.zip");
    SparkProjectPackage.exportProject(
        projectHome.getAbsolutePath(),
        zip.getAbsolutePath(),
        new MemoryMetadataProvider(),
        new Variables());

    SparkProjectPackage.Materialized m = SparkProjectPackage.materialize(zip.getAbsolutePath());
    assertTrue(new File(m.projectHome(), "pipelines/p.hpl").isFile());
    assertFalse(new File(m.projectHome(), "tmp/x.txt").exists());
    assertTrue(new File(m.projectHome(), "work/keep-me.json").isFile());
    assertFalse(new File(m.projectHome(), "work/skip.jar").exists());
  }

  @Test
  void mergeCliOnTopOfFile() {
    PackageExportFilter file =
        new PackageExportFilter(List.of("tmp"), List.of("**/*.csv"), List.of(), false);
    PackageExportFilter cli =
        new PackageExportFilter(List.of("screenshots"), List.of("**/*.zip"), List.of("a"), false);
    PackageExportFilter m = PackageExportFilter.empty().merge(file).merge(cli);
    assertTrue(m.getExcludeDirs().contains("tmp"));
    assertTrue(m.getExcludeDirs().contains("screenshots"));
    assertTrue(m.getExcludeGlobs().contains("**/*.csv"));
    assertTrue(m.getExcludeGlobs().contains("**/*.zip"));
    assertTrue(m.getIncludePaths().contains("a"));
  }
}
