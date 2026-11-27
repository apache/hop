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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.ktor.network.sockets.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.apache.airflow.sdk.CoordinatorServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class HopBundleMainTest {

  @TempDir Path folder;

  /** Without kotlin-argparser on the class path: the SDK's Server class still loads. */
  @Test
  void parsesTheCoordinatorArguments() {
    assertTrue(
        HopBundleMain.server(new String[] {"--comm=127.0.0.1:4000", "--logs=127.0.0.1:4001"})
            instanceof CoordinatorServer);
    assertThrows(
        IllegalArgumentException.class,
        () -> HopBundleMain.server(new String[] {"--comm=127.0.0.1:4000"}));
  }

  /** With TASKS_WHOLE_WORKFLOW a workflow whose hops fit Airflow still runs as one task. */
  @Test
  void runsAWorkflowAsOneTaskWhenTheBundleAsks() throws Exception {
    Path file = folder.resolve("two.hwf");
    Files.writeString(
        file,
        "<workflow><name>w</name><description>Two actions</description><actions>"
            + "<action><name>Start</name><type>SPECIAL</type></action>"
            + "<action><name>Log</name><type>WRITE_TO_LOG</type></action>"
            + "</actions><hops>"
            + "<hop><from>Start</from><to>Log</to><enabled>Y</enabled><evaluation>Y</evaluation>"
            + "<unconditional>Y</unconditional></hop>"
            + "</hops></workflow>");
    HopWorkflowFile perAction = HopWorkflowFile.read(file, "two.hwf", "p__");
    assertEquals(1, perAction.tasks().size());

    HopWorkflowFile whole = HopBundleMain.asOneTask(perAction);

    assertTrue(whole.tasks().isEmpty());
    assertTrue(
        whole.singleTaskReason().contains(HopAirflowBundle.TASKS_WHOLE_WORKFLOW),
        whole.singleTaskReason());
    assertEquals(perAction.dagId(), whole.dagId());
    assertEquals(perAction.relativePath(), whole.relativePath());
    assertEquals("Two actions", whole.description());
    assertEquals(perAction.version(), whole.version());
    assertEquals(
        "Hop workflow `two.hwf`\n\n" + HopAirflowBundle.DAG_DOC_VERSION + perAction.version(),
        HopBundleMain.dagDoc(whole));

    // A workflow that already runs as one task keeps its reason
    Path broken = folder.resolve("broken.hwf");
    Files.writeString(broken, "<workflow>");
    HopWorkflowFile unreadable = HopWorkflowFile.read(broken, "broken.hwf", "p__");
    assertEquals(unreadable, HopBundleMain.asOneTask(unreadable));
  }

  @Test
  void hasNoKotlinArgparser() {
    assertThrows(
        ClassNotFoundException.class, () -> Class.forName("com.xenomachina.argparser.ArgParser"));
  }

  @Test
  void parsesAddresses() {
    InetSocketAddress ipv4 = HopBundleMain.address("127.0.0.1:4000");
    assertEquals(4000, ipv4.getPort());
    assertEquals(4001, HopBundleMain.address("[::1]:4001").getPort());
    assertThrows(IllegalArgumentException.class, () -> HopBundleMain.address("localhost"));
  }

  @Test
  void passesOnTheResultWithoutStackTraces() throws Exception {
    Path result = folder.resolve("result.json");
    Files.writeString(
        result,
        """
        {"schemaVersion":1,"type":"WORKFLOW","name":"main","executionId":"e1",
         "success":false,"counters":{"errors":1},
         "failure":{"cause":{"message":"Table orders does not exist","stackTrace":"at x.y"}}}
        """);

    Map<String, Object> outcome = RunWorkflowTask.outcome(result, 1);

    assertEquals(1, outcome.get("exitCode"));
    assertEquals(false, outcome.get("success"));
    assertEquals("main", outcome.get("name"));
    assertEquals(Map.of("errors", 1), outcome.get("counters"));
    assertEquals("Table orders does not exist", outcome.get("error"));
    assertFalse(outcome.toString().contains("at x.y"));
  }

  @Test
  void reportsTheExitCodeWithoutAResultFile() throws Exception {
    Map<String, Object> outcome = RunWorkflowTask.outcome(folder.resolve("missing.json"), 2);

    assertEquals(Map.of("exitCode", 2, "success", false), outcome);
  }

  /**
   * The SDK returns once the outcome is sent, but a thread that Hop or a plugin leaves running
   * would keep the task JVM alive: the launcher exits itself.
   */
  @Test
  void exitsOnceServedEvenWithAThreadLeftRunning() throws Exception {
    CountDownLatch release = new CountDownLatch(1);
    Thread leftRunning =
        new Thread(
            () -> {
              try {
                release.await();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            });
    List<Integer> exits = new ArrayList<>();
    try {
      HopBundleMain.exitWhenServed(leftRunning::start, exits::add);

      assertTrue(leftRunning.isAlive());
      assertFalse(leftRunning.isDaemon());
      assertEquals(List.of(0), exits);

      HopBundleMain.exitWhenServed(
          () -> {
            throw new IllegalStateException("Two workflows would both become DAG x");
          },
          exits::add);
      assertEquals(List.of(0, 1), exits);
    } finally {
      release.countDown();
    }
  }
}
