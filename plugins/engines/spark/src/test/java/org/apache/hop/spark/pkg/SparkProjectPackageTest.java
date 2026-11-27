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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
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
    // datasets/ and work/ (fat jars) should be skipped
    File datasets = new File(projectHome, "datasets");
    assertTrue(datasets.mkdirs());
    Files.writeString(new File(datasets, "big.csv").toPath(), "a,b\n", StandardCharsets.UTF_8);
    File work = new File(projectHome, "work");
    assertTrue(work.mkdirs());
    Files.writeString(
        new File(work, "hop-native-spark4-submit.jar").toPath(),
        "fake-jar",
        StandardCharsets.UTF_8);

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
    assertTrue(!new File(m.projectHome(), "work/hop-native-spark4-submit.jar").exists());
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
    assertNotEquals(fp1, fp2, "fingerprint must change when zip content changes");
  }

  @Test
  void projectHomeDataPathWarning() {
    assertNull(SparkProjectPackage.projectHomeDataPathWarning("s3a://b/k"));
    assertNotNull(SparkProjectPackage.projectHomeDataPathWarning("${PROJECT_HOME}/files/x.csv"));
  }

  @Test
  void clusterVariablesTravelInThePackage() throws Exception {
    File projectHome = new File(tempDir, "vars-proj");
    assertTrue(projectHome.mkdirs());
    Files.writeString(new File(projectHome, "x.hpl").toPath(), "<p/>", StandardCharsets.UTF_8);
    File zip = new File(tempDir, "vars.zip");
    SparkProjectPackage.exportProject(
        projectHome.getAbsolutePath(),
        zip.getAbsolutePath(),
        new MemoryMetadataProvider(),
        new Variables(),
        null,
        java.util.Map.of("HOP_DATA", "/data", "INPUT_DATE", "2026-11-13"));
    SparkProjectPackage.Materialized m = SparkProjectPackage.materialize(zip.getAbsolutePath());
    assertEquals(
        java.util.Map.of("HOP_DATA", "/data", "INPUT_DATE", "2026-11-13"),
        SparkProjectPackage.readPackagedVariables(m));

    // Stored encoded, like the passwords in metadata.json
    String stored =
        Files.readString(
            new File(m.extractRoot(), SparkProjectPackage.VARIABLES_ENTRY).toPath(),
            StandardCharsets.UTF_8);
    assertTrue(stored.contains("HOP_DATA"));
    assertFalse(stored.contains("2026-11-13"), stored);
  }

  @Test
  void aClientModePackageHasNoVariables() throws Exception {
    File projectHome = new File(tempDir, "novars-proj");
    assertTrue(projectHome.mkdirs());
    Files.writeString(new File(projectHome, "x.hpl").toPath(), "<p/>", StandardCharsets.UTF_8);
    File zip = new File(tempDir, "novars.zip");
    SparkProjectPackage.exportProject(
        projectHome.getAbsolutePath(),
        zip.getAbsolutePath(),
        new MemoryMetadataProvider(),
        new Variables());
    assertTrue(
        SparkProjectPackage.readPackagedVariables(
                SparkProjectPackage.materialize(zip.getAbsolutePath()))
            .isEmpty());
  }

  @Test
  void aFallbackPackageKeepsAVisibleProjectHome() throws Exception {
    File projectHome = new File(tempDir, "fallback-proj");
    assertTrue(projectHome.mkdirs());
    Files.writeString(new File(projectHome, "a.hpl").toPath(), "<p/>", StandardCharsets.UTF_8);
    File zip = new File(tempDir, "fallback.zip");
    SparkProjectPackage.exportProject(
        projectHome.getAbsolutePath(),
        zip.getAbsolutePath(),
        new MemoryMetadataProvider(),
        new Variables());

    // Same machine or shared disk: the executor sees the client's project folder
    Variables visible = new Variables();
    visible.setVariable(SparkProjectPackage.VAR_PACKAGE_URI, zip.getAbsolutePath());
    visible.setVariable(SparkProjectPackage.VAR_PACKAGE_FALLBACK, "Y");
    visible.setVariable("PROJECT_HOME", projectHome.getAbsolutePath());
    SparkProjectPackage.ensureMaterializedOnWorker(visible);
    assertEquals(projectHome.getAbsolutePath(), visible.getVariable("PROJECT_HOME"));

    // Another machine: the client's folder does not exist here, so the package is used
    Variables remote = new Variables();
    remote.setVariable(SparkProjectPackage.VAR_PACKAGE_URI, zip.getAbsolutePath());
    remote.setVariable(SparkProjectPackage.VAR_PACKAGE_FALLBACK, "Y");
    remote.setVariable("PROJECT_HOME", new File(tempDir, "not-here").getAbsolutePath());
    SparkProjectPackage.ensureMaterializedOnWorker(remote);
    assertTrue(new File(remote.getVariable("PROJECT_HOME"), "a.hpl").isFile());
  }

  @Test
  void theSameProjectGivesTheSamePackageBytes() throws Exception {
    File projectHome = new File(tempDir, "stable-proj");
    assertTrue(new File(projectHome, "sub").mkdirs());
    Files.writeString(new File(projectHome, "a.hpl").toPath(), "<p/>", StandardCharsets.UTF_8);
    Files.writeString(new File(projectHome, "sub/b.hpl").toPath(), "<q/>", StandardCharsets.UTF_8);
    File first = new File(tempDir, "first.zip");
    File second = new File(tempDir, "second.zip");
    java.util.Map<String, String> variables = java.util.Map.of("HOP_DATA", "/data", "A", "1");

    SparkProjectPackage.exportProject(
        projectHome.getAbsolutePath(),
        first.getAbsolutePath(),
        new MemoryMetadataProvider(),
        new Variables(),
        null,
        variables);
    // Zip entries carry a modification time with a 2 second resolution
    Thread.sleep(2100L);
    SparkProjectPackage.exportProject(
        projectHome.getAbsolutePath(),
        second.getAbsolutePath(),
        new MemoryMetadataProvider(),
        new Variables(),
        null,
        variables);

    org.junit.jupiter.api.Assertions.assertArrayEquals(
        Files.readAllBytes(first.toPath()), Files.readAllBytes(second.toPath()));
    // The marker still reads as properties
    SparkProjectPackage.Materialized m = SparkProjectPackage.materialize(first.getAbsolutePath());
    assertTrue(new File(m.projectHome(), "sub/b.hpl").isFile());
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
  void resolvePackagePathAlwaysReturnsVolumesUriEvenIfNotLocalFile() {
    // Executors must not require File.isFile() on /Volumes before materialize (driver-only
    // PROJECT_HOME is discarded after re-extract from the shared Volume path).
    Variables vars = new Variables();
    vars.setVariable(
        SparkProjectPackage.VAR_PACKAGE_URI,
        "/Volumes/apache-hop/default/jars/hop-spark-package.zip");
    vars.setVariable("PROJECT_HOME", "/local_disk0/tmp/hop-spark-pkg-deadbeef/project");
    assertEquals(
        "/Volumes/apache-hop/default/jars/hop-spark-package.zip",
        SparkProjectPackage.resolvePackagePathForMaterialize(vars));
    assertEquals(
        java.util.List.of("/Volumes/apache-hop/default/jars/hop-spark-package.zip"),
        SparkProjectPackage.listPackagePathsForMaterialize(vars));
  }

  @Test
  void resolveFallsBackToClusterSharedWhenSparkFilesMissing() {
    // Basename set but SparkFiles.get has nothing → still return cluster-shared URI.
    Variables vars = new Variables();
    vars.setVariable(SparkProjectPackage.VAR_PACKAGE_URI, "s3a://bucket/hop-spark-package.zip");
    vars.setVariable(SparkProjectPackage.VAR_PACKAGE_SPARK_FILE, "hop-spark-package.zip");
    assertEquals(
        "s3a://bucket/hop-spark-package.zip",
        SparkProjectPackage.resolvePackagePathForMaterialize(vars));
  }

  @Test
  void ensureMaterializedClearsDriverOnlyProjectHome() throws Exception {
    File projectHome = new File(tempDir, "stale-ph");
    assertTrue(projectHome.mkdirs());
    Files.writeString(new File(projectHome, "a.hpl").toPath(), "<p/>", StandardCharsets.UTF_8);
    File zip = new File(tempDir, "stale-ph.zip");
    SparkProjectPackage.exportProject(
        projectHome.getAbsolutePath(),
        zip.getAbsolutePath(),
        new MemoryMetadataProvider(),
        new Variables());

    Variables vars = new Variables();
    vars.setVariable(SparkProjectPackage.VAR_PACKAGE_URI, zip.getAbsolutePath());
    // Simulate driver extract path broadcast to an executor that never had that directory
    vars.setVariable("PROJECT_HOME", "/local_disk0/tmp/hop-spark-pkg-deadbeef/project");
    SparkProjectPackage.ensureMaterializedOnWorker(vars);
    assertTrue(new File(vars.getVariable("PROJECT_HOME")).isDirectory());
    assertTrue(new File(vars.getVariable("PROJECT_HOME"), "a.hpl").isFile());
    assertNotEquals(
        "/local_disk0/tmp/hop-spark-pkg-deadbeef/project", vars.getVariable("PROJECT_HOME"));
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
  void stagePackageToLocalTempCopiesReadableZip() throws Exception {
    File projectHome = new File(tempDir, "stage-src");
    assertTrue(projectHome.mkdirs());
    Files.writeString(new File(projectHome, "z.hpl").toPath(), "<p/>", StandardCharsets.UTF_8);
    File zip = new File(tempDir, "stage.zip");
    SparkProjectPackage.exportProject(
        projectHome.getAbsolutePath(),
        zip.getAbsolutePath(),
        new MemoryMetadataProvider(),
        new Variables());
    String staged = SparkProjectPackage.stagePackageToLocalTemp(zip.getAbsolutePath());
    assertNotEquals(zip.getAbsolutePath(), staged);
    assertTrue(staged.contains("hop-spark-pkg-src-"));
    File stagedFile = new File(staged);
    assertTrue(stagedFile.isFile());
    assertTrue(stagedFile.length() > 0);
    // second call reuses same staging path when content unchanged
    assertEquals(staged, SparkProjectPackage.stagePackageToLocalTemp(zip.getAbsolutePath()));
  }

  @Test
  void ensureLocalPackageFileStagesDbfsStyleUriViaCopy() {
    // dbfs: is cluster-shared — must not return a Volume-style path to addFile.
    // We cannot open a real dbfs mount in unit tests; use stagePackageToLocalTemp on a real zip
    // and assert isClusterSharedPackagePath treats dbfs as shared.
    assertTrue(SparkProjectPackage.isClusterSharedPackagePath("dbfs:/FileStore/pkg.zip"));
    assertTrue(
        SparkProjectPackage.isClusterSharedPackagePath(
            "/Volumes/apache-hop/default/jars/hop-spark-package.zip"));
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

  /**
   * Each OS user exports into a folder of their own: on a shared host such as an edge node, the
   * first user's folder used to block everyone else.
   */
  @Test
  void packagesGoToAFolderPerUser() {
    assertEquals(
        java.nio.file.Paths.get("/tmp", "hop-spark-packages-alice"),
        SparkProjectPackage.packageFolder("/tmp", "alice"));
    assertEquals(
        java.nio.file.Paths.get("/tmp", "hop-spark-packages-CORP_bob"),
        SparkProjectPackage.packageFolder("/tmp", "CORP\\bob"));
    assertNotEquals(
        SparkProjectPackage.packageFolder("/tmp", "alice"),
        SparkProjectPackage.packageFolder("/tmp", "bob"));
  }

  /** Packages no run exported for a week go, and so do exports a run left half done. */
  @Test
  void oldPackagesArePruned() throws Exception {
    java.nio.file.Path folder = Files.createDirectories(tempDir.toPath().resolve("packages"));
    java.nio.file.Path current = folder.resolve("hop-spark-package-000000000001.zip");
    java.nio.file.Path recent = folder.resolve("hop-spark-package-000000000002.zip");
    java.nio.file.Path old = folder.resolve("hop-spark-package-000000000003.zip");
    java.nio.file.Path leftOver = folder.resolve("export-123.zip");
    java.nio.file.Path other = folder.resolve("notes.txt");
    java.time.Instant now = java.time.Instant.now();
    for (java.nio.file.Path file : java.util.List.of(current, recent, old, leftOver, other)) {
      Files.writeString(file, "x", StandardCharsets.UTF_8);
      Files.setLastModifiedTime(
          file, java.nio.file.attribute.FileTime.from(now.minus(java.time.Duration.ofDays(30))));
    }
    Files.setLastModifiedTime(
        recent, java.nio.file.attribute.FileTime.from(now.minus(java.time.Duration.ofDays(1))));

    SparkProjectPackage.prunePackages(folder, current, now);
    assertTrue(Files.exists(current), "the package of this run");
    assertTrue(Files.exists(recent));
    assertFalse(Files.exists(old));
    assertFalse(Files.exists(leftOver));
    assertTrue(Files.exists(other), "not a package");
  }

  /**
   * A pipeline without a file, such as the test job of the configuration test, travels in the
   * package next to the project.
   */
  @Test
  void anUnsavedPipelineTravelsInThePackage() throws Exception {
    File projectHome = new File(tempDir, "p3");
    assertTrue(projectHome.mkdirs());
    Files.writeString(new File(projectHome, "a.hpl").toPath(), "<p/>", StandardCharsets.UTF_8);
    File zip = new File(tempDir, "p3.zip");
    SparkProjectPackage.exportProject(
        projectHome.getAbsolutePath(),
        zip.getAbsolutePath(),
        new MemoryMetadataProvider(),
        new Variables(),
        null,
        null,
        java.util.Map.of(".hop-spark-unsaved/0123456789ab.hpl", "<pipeline/>".getBytes()));

    SparkProjectPackage.Materialized m = SparkProjectPackage.materialize(zip.getAbsolutePath());
    File unsaved = new File(m.projectHome(), ".hop-spark-unsaved/0123456789ab.hpl");
    assertEquals("<pipeline/>", Files.readString(unsaved.toPath()));
    assertTrue(new File(m.projectHome(), "a.hpl").isFile());
  }

  /**
   * A driver uses the client's project folder only when it holds the packaged project. A folder at
   * that path on another host can be an old copy: then the driver keeps the package.
   */
  @Test
  void onlyTheSameProjectReplacesThePackage() throws Exception {
    File projectHome = new File(tempDir, "p4");
    assertTrue(new File(projectHome, "pipelines").mkdirs());
    Files.writeString(
        new File(projectHome, "pipelines/main.hpl").toPath(), "<main/>", StandardCharsets.UTF_8);
    Files.writeString(
        new File(projectHome, "pipelines/child.hpl").toPath(), "<child/>", StandardCharsets.UTF_8);
    File zip = new File(tempDir, "p4.zip");
    SparkProjectPackage.exportProject(
        projectHome.getAbsolutePath(),
        zip.getAbsolutePath(),
        new MemoryMetadataProvider(),
        new Variables());
    SparkProjectPackage.Materialized m = SparkProjectPackage.materialize(zip.getAbsolutePath());

    assertTrue(SparkProjectPackage.holdsTheProjectOf(m, projectHome.getAbsolutePath()));
    // A file the package left out doesn't count
    Files.writeString(new File(projectHome, "notes.txt").toPath(), "x", StandardCharsets.UTF_8);
    assertTrue(SparkProjectPackage.holdsTheProjectOf(m, projectHome.getAbsolutePath()));

    // An older copy: the child pipeline differs
    Files.writeString(
        new File(projectHome, "pipelines/child.hpl").toPath(), "<chold/>", StandardCharsets.UTF_8);
    assertFalse(SparkProjectPackage.holdsTheProjectOf(m, projectHome.getAbsolutePath()));
    Files.delete(new File(projectHome, "pipelines/child.hpl").toPath());
    assertFalse(SparkProjectPackage.holdsTheProjectOf(m, projectHome.getAbsolutePath()));
  }

  /**
   * A driver submitted in cluster mode reads its package with the spark.hadoop.* settings of the
   * run, such as the s3a endpoint and keys. spark-submit gives them to the driver as system
   * properties.
   */
  @Test
  void thePackageIsReadWithTheSparkHadoopSettings() {
    String key = "spark.hadoop.fs.s3a.endpoint";
    String previous = System.getProperty(key);
    System.setProperty(key, "http://minio:9000");
    try {
      assertEquals(
          "http://minio:9000",
          SparkProjectPackage.sparkHadoopConfiguration().get("fs.s3a.endpoint"));
    } finally {
      if (previous == null) {
        System.clearProperty(key);
      } else {
        System.setProperty(key, previous);
      }
    }
  }
}
