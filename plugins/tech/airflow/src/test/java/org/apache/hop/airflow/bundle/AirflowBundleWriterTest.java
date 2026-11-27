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

package org.apache.hop.airflow.bundle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import org.apache.hop.airflow.engine.AirflowDagTasks;
import org.apache.hop.airflow.launcher.BundleManifest;
import org.apache.hop.airflow.launcher.HopAirflowBundle;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.vfs.HopVfs;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class AirflowBundleWriterTest {

  @TempDir Path temp;
  private Path project;

  @BeforeEach
  void setUp() throws Exception {
    project = temp.resolve("sales");
    Files.createDirectories(project.resolve("metadata/workflow-run-configuration"));
    Files.createDirectories(project.resolve(".git"));
    Files.createDirectories(project.resolve("env"));
    Files.writeString(project.resolve("main.hwf"), "<workflow/>");
    Files.writeString(project.resolve("project-config.json"), "{}");
    Files.writeString(project.resolve("metadata/workflow-run-configuration/local.json"), "{}");
    Files.writeString(project.resolve(".git/config"), "[core]");
    Files.writeString(project.resolve(".env"), "SECRET=1");
    Files.writeString(project.resolve("env/prod.json"), "{}");
  }

  private AirflowBundleWriter writer() {
    AirflowBundleWriter writer = new AirflowBundleWriter();
    writer.setProjectName("sales");
    writer.setProjectHome(project.toString());
    writer.setSchemaVersion("2026-10-30");
    writer.setHopVersion("2.21.0");
    return writer;
  }

  @Test
  void writesTheStubIntoTheProject() throws Exception {
    AirflowBundleWriter writer = writer();
    writer.setRunConfiguration("remote");
    writer.setWorkflowPatterns(List.of("main.hwf", "etl/**"));
    writer.setEnvironmentFiles(List.of("env/prod.json"));

    AirflowBundleWriter.Result result = writer.write();

    assertFalse(result.copied());
    Path stub = project.resolve(HopAirflowBundle.STUB_FILE_NAME);
    try (JarFile jar = new JarFile(stub.toFile())) {
      Attributes attributes = jar.getManifest().getMainAttributes();
      assertEquals(HopAirflowBundle.MAIN_CLASS, attributes.getValue(Attributes.Name.MAIN_CLASS));
      assertEquals(
          "/opt/hop/plugins/tech/airflow/launcher/hop-airflow-launcher.jar",
          attributes.getValue(Attributes.Name.CLASS_PATH));
    }
    // What the launcher reads
    BundleManifest bundle = BundleManifest.read(stub);
    assertEquals("sales", bundle.projectName());
    assertEquals("2026-10-30", bundle.schemaVersion());
    assertEquals("2.21.0", bundle.hopVersion());
    assertEquals("remote", bundle.runConfiguration());
    assertEquals("Basic", bundle.logLevel());
    assertEquals(List.of("main.hwf", "etl/**"), bundle.workflowPatterns());
    assertEquals(List.of("env/prod.json"), bundle.environmentFiles());
    assertEquals("sales__", bundle.dagIdPrefix());
  }

  @Test
  void pointsToTheWorkersHopInstallation() throws Exception {
    AirflowBundleWriter writer = writer();
    writer.setWorkerHopHome("/opt/apache hop/");
    assertEquals(
        "/opt/apache%20hop/plugins/tech/airflow/launcher/hop-airflow-launcher.jar",
        writer.launcherClassPath());

    writer.setWorkerHopHome("opt/hop");
    assertThrows(HopException.class, writer::launcherClassPath);
  }

  @Test
  void copiesTheProjectWithoutHiddenFiles() throws Exception {
    Files.writeString(project.resolve(HopAirflowBundle.STUB_FILE_NAME), "old");
    Path target = temp.resolve("dags/sales");
    AirflowBundleWriter writer = writer();
    writer.setTargetFolder(target.toString());

    AirflowBundleWriter.Result result = writer.write();

    assertTrue(result.copied());
    assertTrue(Files.isRegularFile(target.resolve("main.hwf")));
    assertTrue(Files.isRegularFile(target.resolve("project-config.json")));
    assertTrue(
        Files.isRegularFile(target.resolve("metadata/workflow-run-configuration/local.json")));
    assertFalse(Files.exists(target.resolve(".git")));
    assertFalse(Files.exists(target.resolve(".env")));
    assertEquals(
        "sales",
        BundleManifest.read(target.resolve(HopAirflowBundle.STUB_FILE_NAME)).projectName());
  }

  @Test
  void replacesAnEarlierBundle() throws Exception {
    Path target = temp.resolve("dags/sales");
    AirflowBundleWriter writer = writer();
    writer.setTargetFolder(target.toString());
    writer.write();
    Files.writeString(target.resolve("removed.hwf"), "<workflow/>");
    Files.createDirectories(target.resolve("old/folder"));
    Files.writeString(target.resolve("old/folder/gone.hpl"), "<pipeline/>");

    writer.write();

    assertFalse(Files.exists(target.resolve("old")));

    assertFalse(Files.exists(target.resolve("removed.hwf")));
    assertTrue(Files.isRegularFile(target.resolve("main.hwf")));
    assertTrue(Files.isRegularFile(target.resolve(AirflowBundleWriter.BUNDLE_MARKER)));
  }

  @Test
  void leavesAFolderAloneThatIsNoBundle() throws Exception {
    Path target = temp.resolve("documents");
    Files.createDirectories(target);
    Files.writeString(target.resolve("keep.txt"), "mine");
    AirflowBundleWriter writer = writer();
    writer.setTargetFolder(target.toString());

    assertThrows(HopException.class, writer::write);
    assertTrue(Files.isRegularFile(target.resolve("keep.txt")));
  }

  /** A Git DAG bundle has a stub too, but Hop did not copy it there: never empty it. */
  @Test
  void leavesAProjectWithACommittedStubAlone() throws Exception {
    Path other = temp.resolve("other-project");
    Files.createDirectories(other.resolve(".git"));
    Files.writeString(other.resolve(HopAirflowBundle.STUB_FILE_NAME), "committed");
    Files.writeString(other.resolve("load.hwf"), "<workflow/>");
    AirflowBundleWriter writer = writer();
    writer.setTargetFolder(other.toString());

    assertThrows(HopException.class, writer::write);
    assertTrue(Files.isRegularFile(other.resolve("load.hwf")));
    assertTrue(Files.isDirectory(other.resolve(".git")));
  }

  @Test
  void copiesIntoAFolderInsideTheProject() throws Exception {
    Path target = project.resolve("build/bundle");
    AirflowBundleWriter writer = writer();
    writer.setTargetFolder(target.toString());

    writer.write();

    assertTrue(Files.isRegularFile(target.resolve("main.hwf")));
    // The bundle is not copied into itself
    assertFalse(Files.exists(target.resolve("build/bundle")));
  }

  @Test
  void checksTheEnvironmentFiles() throws Exception {
    AirflowBundleWriter writer = writer();
    writer.setEnvironmentFiles(List.of("env/missing.json"));
    assertThrows(HopException.class, writer::write);

    writer.setEnvironmentFiles(List.of("/etc/passwd"));
    assertThrows(HopException.class, writer::write);

    // A file outside the project is not copied into the bundle
    Files.writeString(temp.resolve("shared.json"), "{}");
    writer.setEnvironmentFiles(List.of("../shared.json"));
    HopException e = assertThrows(HopException.class, writer::write);
    assertTrue(e.getMessage().contains("../shared.json"), e.getMessage());

    writer.setEnvironmentFiles(List.of("env/prod.json"));
    writer.setTargetFolder(temp.resolve("bundle").toString());
    writer.write();
  }

  /** --environments on the workers reads "x:" as a drive and "file:" as a URL. */
  @Test
  void refusesAProjectNameTheWorkersCannotPassEnvironmentFilesFor() {
    for (String name : List.of("x", "file", "a:b")) {
      AirflowBundleWriter writer = writer();
      writer.setProjectName(name);
      writer.setEnvironmentFiles(List.of("env/prod.json"));
      assertThrows(HopException.class, writer::write, name);
    }
  }

  @Test
  void refusesWorkflowsThatWouldGetTheSameDagId() throws Exception {
    Files.createDirectories(project.resolve("etl"));
    Files.writeString(project.resolve("etl/load.hwf"), "<workflow/>");
    Files.writeString(project.resolve("etl__load.hwf"), "<workflow/>");
    AirflowBundleWriter writer = writer();
    writer.setTargetFolder(temp.resolve("bundle").toString());

    HopException e = assertThrows(HopException.class, writer::write);
    assertTrue(e.getMessage().contains("sales__etl__load"), e.getMessage());

    // Only the workflows that become DAGs count
    writer.setWorkflowPatterns(List.of("main.hwf", "etl/**"));
    writer.write();
  }

  /** A publish that fails must leave the folder Airflow reads as it was. */
  @Test
  void checksEverythingBeforeItReplacesTheBundle() throws Exception {
    Path target = temp.resolve("dags/sales");
    AirflowBundleWriter writer = writer();
    writer.setTargetFolder(target.toString());
    writer.write();
    Files.writeString(project.resolve("added.hwf"), "<workflow/>");

    writer.setDagIdPrefix("my prefix");
    assertThrows(HopException.class, writer::write);
    writer.setDagIdPrefix(null);
    writer.setWorkerHopHome("opt/hop");
    assertThrows(HopException.class, writer::write);

    assertFalse(Files.exists(target.resolve("added.hwf")));
  }

  /** VFS keeps a '%' in a name encoded; the launcher reads the plain name. */
  @Test
  void checksTheDagIdsOfTheFileNamesTheLauncherReads() throws Exception {
    Files.writeString(project.resolve("100%.hwf"), "<workflow/>");
    Files.writeString(project.resolve("100_.hwf"), "<workflow/>");
    AirflowBundleWriter writer = writer();
    writer.setTargetFolder(temp.resolve("bundle").toString());

    HopException e = assertThrows(HopException.class, writer::write);
    assertTrue(e.getMessage().contains("100%.hwf"), e.getMessage());
    assertTrue(e.getMessage().contains("sales__100_:"), e.getMessage());
  }

  @Test
  void listsTheActionsThatDoNotEvaluate() throws Exception {
    AirflowBundleWriter writer = writer();
    writer.setNotEvaluatingActions(List.of("DUMMY", "ExternalPing"));

    writer.write();

    assertEquals(
        Set.of("SPECIAL", "DUMMY", "ExternalPing"),
        BundleManifest.read(project.resolve(HopAirflowBundle.STUB_FILE_NAME))
            .notEvaluatingActions());
  }

  /** The launcher reads the codes of AirflowDagTasks: they must be the values it knows. */
  @Test
  void writesTheTasksTheLauncherReads() throws Exception {
    Path stub = project.resolve(HopAirflowBundle.STUB_FILE_NAME);
    AirflowBundleWriter writer = writer();

    writer.write();
    assertEquals(HopAirflowBundle.TASKS_PER_ACTION, BundleManifest.read(stub).tasks());

    writer.setTasks(AirflowDagTasks.WHOLE_WORKFLOW);
    writer.write();
    assertTrue(BundleManifest.read(stub).wholeWorkflowTasks());

    writer.setTasks(AirflowDagTasks.PER_ACTION);
    writer.write();
    assertFalse(BundleManifest.read(stub).wholeWorkflowTasks());
  }

  @Test
  void looksUpTheTasksByCodeOrDescription() {
    assertEquals(AirflowDagTasks.WHOLE_WORKFLOW, AirflowDagTasks.lookup("whole_workflow"));
    assertEquals(
        AirflowDagTasks.PER_ACTION,
        AirflowDagTasks.lookup(AirflowDagTasks.PER_ACTION.getDescription()));
    assertNull(AirflowDagTasks.lookup(" "));
    assertThrows(IllegalArgumentException.class, () -> AirflowDagTasks.lookup("per-task"));
  }

  @Test
  void needsAProjectNameHopCanPassOn() {
    AirflowBundleWriter writer = writer();
    writer.setProjectName("a,b");
    assertThrows(HopException.class, writer::write);
  }

  @Test
  void readsTheSchemaVersionOfAnSdkJar() throws Exception {
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
    manifest.getMainAttributes().putValue(HopAirflowBundle.ATTRIBUTE_SCHEMA_VERSION, "2026-10-30");
    Path sdkJar = temp.resolve("airflow-sdk-1.0.0.jar");
    try (OutputStream out = Files.newOutputStream(sdkJar);
        JarOutputStream jar = new JarOutputStream(out, manifest)) {
      jar.flush();
    }

    assertEquals(
        "2026-10-30", AirflowBundleWriter.schemaVersionOf(HopVfs.getFileObject(sdkJar.toString())));
    assertEquals(
        BundleManifest.schemaVersionOf(sdkJar),
        AirflowBundleWriter.schemaVersionOf(HopVfs.getFileObject(sdkJar.toString())));
  }
}
