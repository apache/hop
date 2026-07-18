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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import org.apache.hop.core.metadata.SerializableMetadataProvider;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SparkProjectPackageTest {

  @TempDir File tempDir;

  @BeforeEach
  @AfterEach
  void clearCache() {
    SparkProjectPackage.clearMaterializationCache();
  }

  @Test
  void exportAndMaterializeSetsProjectHomeAndMetadata() throws Exception {
    File projectHome = new File(tempDir, "my-project");
    assertTrue(projectHome.mkdirs());
    File pipelines = new File(projectHome, "pipelines");
    assertTrue(pipelines.mkdirs());
    File parent = new File(pipelines, "parent.hpl");
    Files.writeString(parent.toPath(), "<pipeline/>", StandardCharsets.UTF_8);
    File mapping = new File(projectHome, "mappings");
    assertTrue(mapping.mkdirs());
    Files.writeString(
        new File(mapping, "child.hpl").toPath(), "<pipeline/>", StandardCharsets.UTF_8);
    // datasets/ should be skipped
    File datasets = new File(projectHome, "datasets");
    assertTrue(datasets.mkdirs());
    Files.writeString(new File(datasets, "big.csv").toPath(), "a,b\n", StandardCharsets.UTF_8);

    File zip = new File(tempDir, "pkg.zip");
    MemoryMetadataProvider memory = new MemoryMetadataProvider();
    SparkProjectPackage.exportProject(
        projectHome.getAbsolutePath(), zip.getAbsolutePath(), memory, new Variables());

    assertTrue(zip.isFile());

    SparkProjectPackage.Materialized m = SparkProjectPackage.materialize(zip.getAbsolutePath());
    assertNotNull(m.projectHome());
    assertTrue(new File(m.projectHome(), "pipelines/parent.hpl").isFile());
    assertTrue(new File(m.projectHome(), "mappings/child.hpl").isFile());
    assertTrue(!new File(m.projectHome(), "datasets/big.csv").exists());
    assertNotNull(m.metadataPath());
    assertTrue(new File(m.metadataPath()).isFile());

    Variables vars = new Variables();
    SparkProjectPackage.applyToVariables(vars, m, zip.getAbsolutePath());
    assertEquals(m.projectHome(), vars.getVariable("PROJECT_HOME"));
    assertEquals(zip.getAbsolutePath(), vars.getVariable(SparkProjectPackage.VAR_PACKAGE_URI));

    String resolved = SparkProjectPackage.resolvePipelinePath("pipelines/parent.hpl", m, vars);
    assertTrue(resolved.endsWith("parent.hpl"));
    assertTrue(new File(resolved).isFile());
  }

  @Test
  void ensureMaterializedOnWorkerIsIdempotent() throws Exception {
    File projectHome = new File(tempDir, "p2");
    assertTrue(projectHome.mkdirs());
    Files.writeString(new File(projectHome, "a.hpl").toPath(), "<p/>", StandardCharsets.UTF_8);
    File zip = new File(tempDir, "p2.zip");
    SparkProjectPackage.exportProject(
        projectHome.getAbsolutePath(),
        zip.getAbsolutePath(),
        new MemoryMetadataProvider(),
        new Variables());

    Variables vars = new Variables();
    vars.setVariable(SparkProjectPackage.VAR_PACKAGE_URI, zip.getAbsolutePath());
    SparkProjectPackage.ensureMaterializedOnWorker(vars);
    String home1 = vars.getVariable("PROJECT_HOME");
    SparkProjectPackage.ensureMaterializedOnWorker(vars);
    assertEquals(home1, vars.getVariable("PROJECT_HOME"));
  }

  @Test
  void materializeReExtractsWhenZipContentChanges() throws Exception {
    File projectHome = new File(tempDir, "p-change");
    assertTrue(projectHome.mkdirs());
    File pipelines = new File(projectHome, "pipelines");
    assertTrue(pipelines.mkdirs());
    Files.writeString(
        new File(pipelines, "v1.hpl").toPath(), "<pipeline id=\"v1\"/>", StandardCharsets.UTF_8);
    File zip = new File(tempDir, "change.zip");
    SparkProjectPackage.exportProject(
        projectHome.getAbsolutePath(),
        zip.getAbsolutePath(),
        new MemoryMetadataProvider(),
        new Variables());

    SparkProjectPackage.Materialized m1 = SparkProjectPackage.materialize(zip.getAbsolutePath());
    assertTrue(new File(m1.projectHome(), "pipelines/v1.hpl").isFile());
    assertFalse(new File(m1.projectHome(), "pipelines/v2.hpl").exists());
    String fp1 = SparkProjectPackage.cachedFingerprintForTest(zip.getAbsolutePath());
    assertNotNull(fp1);
    assertTrue(fp1.contains("sha256="));

    // Overwrite the same zip path with new project content (same path, new fingerprint)
    Files.writeString(
        new File(pipelines, "v2.hpl").toPath(), "<pipeline id=\"v2\"/>", StandardCharsets.UTF_8);
    // Ensure mtime differs even on coarse filesystems
    Thread.sleep(10);
    SparkProjectPackage.exportProject(
        projectHome.getAbsolutePath(),
        zip.getAbsolutePath(),
        new MemoryMetadataProvider(),
        new Variables());

    SparkProjectPackage.Materialized m2 = SparkProjectPackage.materialize(zip.getAbsolutePath());
    assertTrue(new File(m2.projectHome(), "pipelines/v2.hpl").isFile());
    String fp2 = SparkProjectPackage.cachedFingerprintForTest(zip.getAbsolutePath());
    assertNotNull(fp2);
    assertFalse(fp1.equals(fp2), "fingerprint must change when zip content changes");
  }

  @Test
  void projectHomeDataPathWarning() {
    assertNull(SparkProjectPackage.projectHomeDataPathWarning("s3a://b/k"));
    assertNotNull(SparkProjectPackage.projectHomeDataPathWarning("${PROJECT_HOME}/files/x.csv"));
  }

  @Test
  void metadataRoundTripInPackage() throws Exception {
    File projectHome = new File(tempDir, "meta-proj");
    assertTrue(projectHome.mkdirs());
    Files.writeString(new File(projectHome, "x.hpl").toPath(), "<p/>", StandardCharsets.UTF_8);
    File zip = new File(tempDir, "meta.zip");
    MemoryMetadataProvider memory = new MemoryMetadataProvider();
    SparkProjectPackage.exportProject(
        projectHome.getAbsolutePath(), zip.getAbsolutePath(), memory, new Variables());
    SparkProjectPackage.Materialized m = SparkProjectPackage.materialize(zip.getAbsolutePath());
    String json = Files.readString(new File(m.metadataPath()).toPath());
    // parseable metadata JSON
    new SerializableMetadataProvider(json);
  }

  @Test
  void isSparkFilesLocalPathDetectsUserFiles() {
    assertTrue(
        SparkProjectPackage.isSparkFilesLocalPath("/tmp/spark-abc/userFiles-xyz/spark-demo.zip"));
    assertFalse(
        SparkProjectPackage.isSparkFilesLocalPath("/data/hop-data/packages/spark-demo.zip"));
  }

  @Test
  void isClusterSharedPackagePathDetectsVolumesAndObjectStore() {
    assertTrue(
        SparkProjectPackage.isClusterSharedPackagePath(
            "/Volumes/apache-hop/default/jars/hop-spark-package.zip"));
    assertTrue(SparkProjectPackage.isClusterSharedPackagePath("/dbfs/FileStore/pkg.zip"));
    assertTrue(SparkProjectPackage.isClusterSharedPackagePath("dbfs:/FileStore/pkg.zip"));
    assertTrue(SparkProjectPackage.isClusterSharedPackagePath("s3a://bucket/pkg.zip"));
    assertFalse(SparkProjectPackage.isClusterSharedPackagePath("/tmp/local-pkg.zip"));
    assertFalse(SparkProjectPackage.isClusterSharedPackagePath("file:/tmp/local-pkg.zip"));
    assertFalse(SparkProjectPackage.isClusterSharedPackagePath(null));
  }

  @Test
  void resolvePrefersClusterSharedUriOverSparkFileBasename() throws Exception {
    File projectHome = new File(tempDir, "vol-proj");
    assertTrue(projectHome.mkdirs());
    Files.writeString(new File(projectHome, "a.hpl").toPath(), "<p/>", StandardCharsets.UTF_8);
    // Simulate UC Volume-style path by using a real local file under a /Volumes-like prefix is
    // not portable on every OS; use s3a-style remote marker via path that isClusterShared and
    // exists as local under a fake mount name.
    File volumesRoot = new File(tempDir, "Volumes");
    File volumeZipDir = new File(volumesRoot, "c/s/v");
    assertTrue(volumeZipDir.mkdirs());
    File zip = new File(volumeZipDir, "hop-spark-package.zip");
    SparkProjectPackage.exportProject(
        projectHome.getAbsolutePath(),
        zip.getAbsolutePath(),
        new MemoryMetadataProvider(),
        new Variables());

    // Absolute path that contains /Volumes/ after normalize — on Linux temp paths won't start
    // with /Volumes/; test isClusterShared on the string form used by Databricks Deploy.
    Variables vars = new Variables();
    String sharedUri = "/Volumes/apache-hop/default/jars/hop-spark-package.zip";
    // Point URI at our real zip by using isClusterShared detection only for the string; for
    // resolve we need the file to exist — use real zip path with shared detection via s3a skip.
    // Instead: set URI to real zip absolute path that we mark via dbfs: if needed.
    // Practical unit test: shared object-store URI returned when no local file.
    vars.setVariable(SparkProjectPackage.VAR_PACKAGE_URI, "s3a://bucket/hop-spark-package.zip");
    vars.setVariable(SparkProjectPackage.VAR_PACKAGE_SPARK_FILE, "hop-spark-package.zip");
    assertEquals(
        "s3a://bucket/hop-spark-package.zip",
        SparkProjectPackage.resolvePackagePathForMaterialize(vars));
  }

  @Test
  void resolvePrefersHopDataPackagesOverMissingSparkFiles() throws Exception {
    File projectHome = new File(tempDir, "shared-proj");
    assertTrue(projectHome.mkdirs());
    Files.writeString(new File(projectHome, "a.hpl").toPath(), "<p/>", StandardCharsets.UTF_8);
    File zip = new File(tempDir, "shared-pkg.zip");
    SparkProjectPackage.exportProject(
        projectHome.getAbsolutePath(),
        zip.getAbsolutePath(),
        new MemoryMetadataProvider(),
        new Variables());

    File hopData = new File(tempDir, "hop-data");
    File packages = new File(hopData, "packages");
    assertTrue(packages.mkdirs());
    File sharedZip = new File(packages, "shared-pkg.zip");
    Files.copy(zip.toPath(), sharedZip.toPath());

    Variables vars = new Variables();
    vars.setVariable("HOP_DATA", hopData.getAbsolutePath());
    vars.setVariable(SparkProjectPackage.VAR_PACKAGE_SPARK_FILE, "shared-pkg.zip");
    // Deliberately point URI at a non-existent driver-only path
    vars.setVariable(SparkProjectPackage.VAR_PACKAGE_URI, "/nonexistent/shared-pkg.zip");

    String resolved = SparkProjectPackage.resolvePackagePathForMaterialize(vars);
    assertEquals(sharedZip.getAbsolutePath(), resolved);
  }

  @Test
  void openPackageStreamReadsLocalZip() throws Exception {
    File projectHome = new File(tempDir, "stream-proj");
    assertTrue(projectHome.mkdirs());
    Files.writeString(new File(projectHome, "x.hpl").toPath(), "<p/>", StandardCharsets.UTF_8);
    File zip = new File(tempDir, "stream.zip");
    SparkProjectPackage.exportProject(
        projectHome.getAbsolutePath(),
        zip.getAbsolutePath(),
        new MemoryMetadataProvider(),
        new Variables());
    assertNotNull(SparkProjectPackage.resolveLocalPackageFile(zip.getAbsolutePath()));
    try (InputStream in = SparkProjectPackage.openPackageStream(zip.getAbsolutePath())) {
      assertTrue(in.read() >= 0);
    }
  }

  @Test
  void ensureLocalPackageFileReturnsExistingFile() throws Exception {
    File projectHome = new File(tempDir, "local-src");
    assertTrue(projectHome.mkdirs());
    Files.writeString(new File(projectHome, "z.hpl").toPath(), "<p/>", StandardCharsets.UTF_8);
    File zip = new File(tempDir, "local.zip");
    SparkProjectPackage.exportProject(
        projectHome.getAbsolutePath(),
        zip.getAbsolutePath(),
        new MemoryMetadataProvider(),
        new Variables());
    String local = SparkProjectPackage.ensureLocalPackageFile(zip.getAbsolutePath());
    assertEquals(zip.getAbsolutePath(), local);
  }

  @Test
  void distributeToClusterWithLocalSparkSession() throws Exception {
    File projectHome = new File(tempDir, "dist-proj");
    assertTrue(projectHome.mkdirs());
    Files.writeString(new File(projectHome, "p.hpl").toPath(), "<p/>", StandardCharsets.UTF_8);
    File zip = new File(tempDir, "dist.zip");
    SparkProjectPackage.exportProject(
        projectHome.getAbsolutePath(),
        zip.getAbsolutePath(),
        new MemoryMetadataProvider(),
        new Variables());

    org.apache.spark.sql.SparkSession spark =
        org.apache.spark.sql.SparkSession.builder()
            .master("local[2]")
            .appName("SparkProjectPackageTest")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate();
    try {
      Variables vars = new Variables();
      vars.setVariable(SparkProjectPackage.VAR_PACKAGE_URI, zip.getAbsolutePath());
      SparkProjectPackage.distributeToCluster(spark, vars);
      assertEquals(zip.getName(), vars.getVariable(SparkProjectPackage.VAR_PACKAGE_SPARK_FILE));
      assertTrue(new File(vars.getVariable(SparkProjectPackage.VAR_PACKAGE_URI)).isFile());

      String resolved = SparkProjectPackage.resolvePackagePathForMaterialize(vars);
      assertNotNull(resolved);
      assertTrue(new File(resolved).isFile());

      SparkProjectPackage.ensureMaterializedOnWorker(vars);
      assertTrue(new File(vars.getVariable("PROJECT_HOME"), "p.hpl").isFile());
    } finally {
      spark.stop();
    }
  }
}
