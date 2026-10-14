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
package org.apache.hop.lint;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.util.List;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.workflow.WorkflowMeta;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Enabling another project has to drop the findings of the one that was left (issue #8774).
 *
 * <p>The background project lint merges file by file, so the files of the previous project kept
 * their findings and the Lint Results view listed both projects.
 */
class ProjectSwitchLintTest {

  @TempDir File projectA;
  @TempDir File projectB;

  private final BackgroundLintService service = BackgroundLintService.getInstance();
  private final LintResultsManager manager = LintResultsManager.getInstance();

  @BeforeEach
  void setUp() {
    LinterConfigPlugin config = LinterConfigPlugin.getInstance();
    config.setLinterEnabled(true);
    config.saveToHopConfig();
    manager.clearResults();
  }

  @AfterEach
  void tearDown() {
    manager.clearResults();
  }

  @Test
  void switchingToAnotherProjectDropsTheFindingsOfThePreviousOne() {
    service.switchProject(projectA.getAbsolutePath());
    String file = new File(projectA, "a.hpl").getAbsolutePath();
    manager.updateResultsForFile(file, List.of(finding(file)));

    service.switchProject(projectB.getAbsolutePath());

    assertTrue(manager.getAllResults().isEmpty(), "project A's findings must not stay listed");
  }

  @Test
  void enablingTheSameProjectAgainKeepsItsFindings() {
    service.switchProject(projectA.getAbsolutePath());
    String file = new File(projectA, "a.hpl").getAbsolutePath();
    manager.updateResultsForFile(file, List.of(finding(file)));

    service.switchProject(projectA.getAbsolutePath());

    assertEquals(1, manager.getAllResults().size());
  }

  @Test
  void aPassStillRunningForThePreviousProjectDoesNotPublish() throws Exception {
    service.switchProject(projectA.getAbsolutePath());
    int generation = service.currentGeneration();
    File file = new File(projectA, "a.hpl");
    Files.writeString(file.toPath(), "<pipeline/>");

    service.switchProject(projectB.getAbsolutePath());
    boolean published =
        service.publish(file.getAbsolutePath(), List.of(finding(file.getPath())), generation);

    assertFalse(published);
    assertTrue(manager.getAllResults().isEmpty(), "a late pass must not put project A back");
    assertTrue(
        service.getTracker().needsLinting(file.getAbsolutePath()),
        "a dropped pass must not mark the file as checked");
  }

  @Test
  void switchingBackLintsTheUnchangedFilesAgain() throws Exception {
    service.switchProject(projectA.getAbsolutePath());
    File file = new File(projectA, "a.hpl");
    Files.writeString(file.toPath(), "<pipeline/>");
    service.getTracker().markChecked(file.getAbsolutePath());
    assertFalse(service.getTracker().needsLinting(file.getAbsolutePath()));

    service.switchProject(projectB.getAbsolutePath());
    service.switchProject(projectA.getAbsolutePath());

    assertTrue(
        service.getTracker().needsLinting(file.getAbsolutePath()),
        "project A would come back without findings");
  }

  /**
   * A file lint already on the pool when the project is switched, from an editor that was closed or
   * a file opened just before. It finishes afterwards and must not bring the previous project back.
   */
  @Test
  void aFileLintScheduledBeforeTheSwitchDoesNotReappear() throws Exception {
    service.switchProject(projectA.getAbsolutePath());
    String file = writeWorkflow(projectA);

    // Control: the same pass, still current, does publish. Without it, a file that cannot be
    // linted would pass the test below for the wrong reason.
    assertTrue(service.lintAndPublishFile(file, service.currentGeneration()));
    assertFalse(service.getTracker().needsLinting(file), "a current pass publishes");

    service.getTracker().invalidate(file);
    int scheduledBefore = service.currentGeneration();
    service.switchProject(projectB.getAbsolutePath());
    assertFalse(service.lintAndPublishFile(file, scheduledBefore));

    assertTrue(manager.getAllResults().isEmpty(), "project A's file must not reappear");
    assertTrue(
        service.getTracker().needsLinting(file), "a dropped pass must not mark the file checked");
  }

  /**
   * A workflow, not a pipeline: without Hop GUI there is no metadata provider, and a pipeline
   * refuses to load without one.
   */
  static String writeWorkflow(File folder) throws Exception {
    WorkflowMeta workflowMeta = new WorkflowMeta();
    workflowMeta.setName("example");
    File file = new File(folder, "example.hwf");
    Files.writeString(file.toPath(), workflowMeta.getXml(Variables.getADefaultVariableSpace()));
    return file.getAbsolutePath();
  }

  private static LintResult finding(String file) {
    return new LintResult("HOP-CHECK", "SQL", "ERROR", "SQL cannot be null or blank.", file);
  }
}
