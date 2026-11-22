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
import static org.junit.jupiter.api.Assertions.assertNull;
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

  private static String start(
      String repeat, String type, String minutes, String hour, String interval) {
    return "      <repeat>"
        + repeat
        + "</repeat>\n      <schedulerType>"
        + type
        + "</schedulerType>\n      <intervalSeconds>0</intervalSeconds>\n"
        + "      <intervalMinutes>"
        + interval
        + "</intervalMinutes>\n      <hour>"
        + hour
        + "</hour>\n      <minutes>"
        + minutes
        + "</minutes>\n      <weekDay>1</weekDay>\n      <DayOfMonth>15</DayOfMonth>\n";
  }

  private String scheduleOf(String start) throws Exception {
    Path file = project.resolve("w.hwf");
    Files.writeString(file, workflow("d", start));
    return HopWorkflowFile.read(file, "w.hwf", "p__").schedule();
  }

  @Test
  void mapsTheStartSchedule() throws Exception {
    assertEquals("30 6 * * *", scheduleOf(start("Y", "2", "30", "6", "60")));
    assertEquals("30 6 * * 1", scheduleOf(start("Y", "3", "30", "6", "60")));
    assertEquals("30 6 15 * *", scheduleOf(start("Y", "4", "30", "6", "60")));
    assertEquals("*/15 * * * *", scheduleOf(start("Y", "1", "0", "12", "15")));
    assertEquals("0 * * * *", scheduleOf(start("Y", "1", "0", "12", "60")));
    assertEquals("0 */6 * * *", scheduleOf(start("Y", "1", "0", "12", "360")));
    assertEquals("0 0 * * *", scheduleOf(start("Y", "1", "0", "12", "1440")));
  }

  @Test
  void leavesOutWhatCronCannotExpress() throws Exception {
    // Not repeating, no scheduling, an interval of 7 minutes, a variable
    assertNull(scheduleOf(start("N", "2", "30", "6", "60")));
    assertNull(scheduleOf(start("Y", "0", "30", "6", "60")));
    assertNull(scheduleOf(start("Y", "1", "0", "12", "7")));
    assertNull(scheduleOf(start("Y", "2", "30", "${HOUR}", "60")));
  }

  @Test
  void scansTheMatchingWorkflowsAndSkipsHiddenFolders() throws Exception {
    Files.createDirectories(project.resolve("etl/daily"));
    Files.createDirectories(project.resolve(".git"));
    Files.createDirectories(project.resolve("lib"));
    for (String file :
        List.of("main.hwf", "etl/load.hwf", "etl/daily/orders.hwf", ".git/x.hwf", "lib/sub.hwf")) {
      Files.writeString(project.resolve(file), workflow(file, start("N", "0", "0", "0", "60")));
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
