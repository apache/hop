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

package org.apache.hop.setup.persist;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.hop.setup.HopEnvironmentApplyResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LocalRunConfigurationSeederTest {

  @TempDir Path temp;

  @Test
  void writesStandardLocalRunConfigurationsWhenTemplateHasNone() throws Exception {
    Path project = temp.resolve("default");
    Files.createDirectories(project);
    HopEnvironmentApplyResult result = new HopEnvironmentApplyResult();

    new LocalRunConfigurationSeeder().ensure(project.toString(), null, result);

    String pipeline =
        Files.readString(project.resolve(LocalRunConfigurationSeeder.PIPELINE_RELATIVE));
    String workflow =
        Files.readString(project.resolve(LocalRunConfigurationSeeder.WORKFLOW_RELATIVE));
    assertTrue(pipeline.contains("\"name\": \"local\""));
    assertTrue(pipeline.contains("\"defaultSelection\": true"));
    assertTrue(pipeline.contains("\"executionInfoLocationName\": \"local-audit\""));
    assertTrue(pipeline.contains("\"dataProfile\": \"first-last\""));
    assertTrue(workflow.contains("\"name\": \"local\""));
    assertTrue(workflow.contains("\"defaultSelection\": true"));
    assertTrue(
        Files.readString(project.resolve(LocalRunConfigurationSeeder.EXECUTION_INFO_RELATIVE))
            .contains("${HOP_AUDIT_FOLDER}/executions/"));
    assertTrue(
        Files.readString(project.resolve(LocalRunConfigurationSeeder.DATA_PROFILE_RELATIVE))
            .contains("first-last"));
    assertTrue(result.describe().contains("local pipeline run configuration"));
    assertTrue(result.describe().contains("local workflow run configuration"));
  }

  @Test
  void keepsTemplateRunConfigurationsAndDoesNotInventCompanions() throws Exception {
    Path template = temp.resolve("template");
    write(
        template, LocalRunConfigurationSeeder.PIPELINE_RELATIVE, "{ \"name\": \"kept-pipeline\" }");
    write(
        template, LocalRunConfigurationSeeder.WORKFLOW_RELATIVE, "{ \"name\": \"kept-workflow\" }");
    Path project = temp.resolve("default");
    Files.createDirectories(project);

    new LocalRunConfigurationSeeder()
        .ensure(project.toString(), template.toString(), new HopEnvironmentApplyResult());

    assertTrue(
        Files.readString(project.resolve(LocalRunConfigurationSeeder.PIPELINE_RELATIVE))
            .contains("kept-pipeline"));
    assertTrue(
        Files.readString(project.resolve(LocalRunConfigurationSeeder.WORKFLOW_RELATIVE))
            .contains("kept-workflow"));
    assertFalse(Files.exists(project.resolve(LocalRunConfigurationSeeder.EXECUTION_INFO_RELATIVE)));
    assertFalse(Files.exists(project.resolve(LocalRunConfigurationSeeder.DATA_PROFILE_RELATIVE)));
  }

  @Test
  void doesNotReplaceExistingRunConfigurations() throws Exception {
    Path project = temp.resolve("default");
    write(project, LocalRunConfigurationSeeder.PIPELINE_RELATIVE, "{ \"name\": \"mine\" }");
    write(project, LocalRunConfigurationSeeder.WORKFLOW_RELATIVE, "{ \"name\": \"mine-wf\" }");

    new LocalRunConfigurationSeeder()
        .ensure(project.toString(), null, new HopEnvironmentApplyResult());

    assertTrue(
        Files.readString(project.resolve(LocalRunConfigurationSeeder.PIPELINE_RELATIVE))
            .contains("mine"));
    assertTrue(
        Files.readString(project.resolve(LocalRunConfigurationSeeder.WORKFLOW_RELATIVE))
            .contains("mine-wf"));
    assertFalse(Files.exists(project.resolve(LocalRunConfigurationSeeder.EXECUTION_INFO_RELATIVE)));
  }

  private static void write(Path root, String relative, String content) throws Exception {
    Path file = root.resolve(relative);
    Files.createDirectories(file.getParent());
    Files.writeString(file, content);
  }
}
