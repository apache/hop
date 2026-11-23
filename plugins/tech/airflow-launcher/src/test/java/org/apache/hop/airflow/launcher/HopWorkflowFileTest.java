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

  @Test
  void makesValidDagIds() {
    assertEquals("p__my_flow_v2", HopWorkflowFile.dagId("p__", "my flow+v2.hwf"));
  }
}
