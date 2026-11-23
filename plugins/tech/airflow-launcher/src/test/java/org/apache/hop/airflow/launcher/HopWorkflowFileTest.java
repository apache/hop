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

package org.apache.hop.airflow.launcher;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class HopWorkflowFileTest {

  @TempDir Path project;

  private static String workflow(String description, String start) {
    return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<workflow>\n  <name>w</name>\n"
        + "  <description>"
        + description
        + "</description>\n  <actions>\n    <action>\n      <name>Start</name>\n"
        + "      <type>SPECIAL</type>\n"
        + start
        + "    </action>\n  </actions>\n</workflow>\n";
  }

  @Test
  void scansTheMatchingWorkflowsAndSkipsHiddenFolders() throws Exception {
    Files.createDirectories(project.resolve("etl/daily"));
    Files.createDirectories(project.resolve(".git"));
    Files.createDirectories(project.resolve("lib"));
    for (String file :
        List.of("main.hwf", "etl/load.hwf", "etl/daily/orders.hwf", ".git/x.hwf", "lib/sub.hwf")) {
      Files.writeString(project.resolve(file), workflow(file, "      <repeat>N</repeat>\n"));
    }
    Files.writeString(project.resolve("etl/load.hpl"), "<pipeline/>");

    List<String> all =
        HopWorkflowFile.scan(project, List.of(), "p__").stream()
            .map(HopWorkflowFile::relativePath)
            .toList();
    assertEquals(List.of("etl/daily/orders.hwf", "etl/load.hwf", "lib/sub.hwf", "main.hwf"), all);

    List<HopWorkflowFile> matching =
        HopWorkflowFile.scan(project, List.of("main.hwf", "etl/**"), "p__");
    assertEquals(
        List.of("etl/daily/orders.hwf", "etl/load.hwf", "main.hwf"),
        matching.stream().map(HopWorkflowFile::relativePath).toList());
    assertEquals("p__etl__daily__orders", matching.get(0).dagId());
    assertEquals("etl/daily/orders.hwf", matching.get(0).description());
  }

  @Test
  void describesAnUnreadableWorkflow() throws Exception {
    Path file = project.resolve("broken.hwf");
    Files.writeString(file, "<workflow>");

    HopWorkflowFile workflow = HopWorkflowFile.read(file, "broken.hwf", "p__");

    assertEquals("p__broken", workflow.dagId());
    assertTrue(workflow.description().startsWith("Hop could not read"), workflow.description());
  }

  /** The stub lists the actions that don't evaluate, from the plugins of the Hop that wrote it. */
  @Test
  void neverFollowsAConditionalHopFromAnActionTheStubListsAsNotEvaluating() throws Exception {
    Path file = project.resolve("external.hwf");
    Files.writeString(
        file,
        "<workflow><name>w</name><actions>"
            + "<action><name>Start</name><type>SPECIAL</type></action>"
            + "<action><name>Ping</name><type>ExternalPing</type></action>"
            + "<action><name>Next</name><type>WRITE_TO_LOG</type></action>"
            + "</actions><hops>"
            + "<hop><from>Start</from><to>Ping</to><enabled>Y</enabled><evaluation>Y</evaluation>"
            + "<unconditional>Y</unconditional></hop>"
            + "<hop><from>Ping</from><to>Next</to><enabled>Y</enabled><evaluation>Y</evaluation>"
            + "<unconditional>N</unconditional></hop>"
            + "</hops></workflow>");

    HopWorkflowFile listed =
        HopWorkflowFile.scan(project, List.of(), "p__", Set.of("SPECIAL", "ExternalPing")).get(0);
    assertEquals(
        List.of("Ping"), listed.tasks().stream().map(ActionTasks.ActionTask::taskId).toList());

    HopWorkflowFile evaluating = HopWorkflowFile.read(file, "external.hwf", "p__");
    assertEquals(
        List.of("Ping", "Next"),
        evaluating.tasks().stream().map(ActionTasks.ActionTask::taskId).toList());
  }

  @Test
  void makesValidDagIds() {
    assertEquals("p__my_flow_v2", HopWorkflowFile.dagId("p__", "my flow+v2.hwf"));
  }
}
