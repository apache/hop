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
 *
 */

package org.apache.hop.run;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.hop.core.Const;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Runs hop-run in a JVM of its own and sends it SIGTERM, the way docker stop or a Kubernetes pod
 * eviction does. The workflow repeats until it is stopped, so it only ends because of the signal.
 */
@Timeout(value = 3, unit = TimeUnit.MINUTES)
class HopRunSignalTest {

  private static final int SIGTERM_EXIT_CODE = 128 + 15;

  @TempDir Path tempDir;

  @Test
  void sigtermStopsTheRunningWorkflowBeforeTheJvmExits() throws Exception {
    assumeFalse(Const.isWindows(), "Windows has no SIGTERM");

    Path metadata = tempDir.resolve("metadata");
    Files.createDirectories(metadata.resolve("workflow-run-configuration"));
    Files.writeString(
        metadata.resolve("workflow-run-configuration/local.json"),
        """
        {
          "engineRunConfiguration": { "Local": { "safe_mode": false } },
          "name": "local"
        }
        """);
    Path config = Files.createDirectories(tempDir.resolve("config"));
    String workflow =
        new File(getClass().getResource("/org/apache/hop/run/run-until-stopped.hwf").toURI())
            .getPath();

    List<String> command = new ArrayList<>();
    command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
    command.add("-DHOP_CONFIG_FOLDER=" + config);
    command.add("-DHOP_AUTO_CREATE_CONFIG=Y");
    command.add("-DHOP_METADATA_FOLDER=" + metadata);
    command.add("-classpath");
    command.add(System.getProperty("java.class.path"));
    command.add(HopRun.class.getName());
    command.add("--file=" + workflow);
    command.add("--runconfig=local");
    command.add("--level=Basic");
    command.add("--shutdown-timeout=20");

    // A file, not a pipe: the JDK closes the pipe when the process exits, and the last lines,
    // the ones written while stopping, could get lost.
    Path outputFile = tempDir.resolve("hop-run.log");
    Process process =
        new ProcessBuilder(command)
            .directory(tempDir.toFile())
            .redirectErrorStream(true)
            .redirectOutput(outputFile.toFile())
            .start();

    try {
      // The first repetition is done: the workflow is running.
      long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(2);
      while (!Files.readString(outputFile).contains("Finished action [Dummy]")) {
        assertTrue(process.isAlive(), "hop-run ended early:\n" + Files.readString(outputFile));
        assertTrue(
            System.nanoTime() < deadline,
            "The workflow did not start in time:\n" + Files.readString(outputFile));
        Thread.sleep(100);
      }

      // On Unix Process.destroy() sends SIGTERM.
      long signalled = System.nanoTime();
      process.destroy();
      assertTrue(
          process.waitFor(60, TimeUnit.SECONDS),
          "hop-run did not exit:\n" + Files.readString(outputFile));
      long stopMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - signalled);

      String log = Files.readString(outputFile);
      assertEquals(SIGTERM_EXIT_CODE, process.exitValue(), log);
      assertTrue(
          log.contains("Termination signal received, stopping the run. Waiting up to 20 seconds"),
          log);
      assertTrue(log.contains("Stopping workflow 'run-until-stopped'"), log);
      assertTrue(log.contains("The run has stopped, hop-run exits."), log);
      assertTrue(stopMs < 20_000, "Stopping took " + stopMs + "ms:\n" + log);
    } finally {
      process.destroyForcibly();
    }
  }
}
