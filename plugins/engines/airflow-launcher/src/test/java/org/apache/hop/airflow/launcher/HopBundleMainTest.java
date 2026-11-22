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
import java.util.Map;
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
}
