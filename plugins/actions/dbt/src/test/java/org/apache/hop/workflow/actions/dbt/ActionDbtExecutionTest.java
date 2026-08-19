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

package org.apache.hop.workflow.actions.dbt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.hop.core.Result;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

/**
 * Drives {@link ActionDbt#execute} against a stub dbt executable, so the process handling (exit
 * codes, timeout, stale artifacts) is covered without a real dbt install. Shell-script based, so it
 * is limited to the platforms the module's integration test targets anyway.
 */
@EnabledOnOs({OS.LINUX, OS.MAC})
class ActionDbtExecutionTest {

  private static final String RUN_RESULTS_OK =
      """
      {"results": [{"unique_id": "model.demo.customers", "status": "success",
        "execution_time": 0.2, "relation_name": "demo.main.customers"}],
       "elapsed_time": 0.3}
      """;

  private static final String RUN_RESULTS_FAILED_TEST =
      """
      {"results": [{"unique_id": "test.demo.not_null_customers_id", "status": "fail",
        "execution_time": 0.1, "message": "Got 3 results, expected 0."}],
       "elapsed_time": 0.2}
      """;

  @TempDir Path projectDir;

  private ActionDbt action;

  @BeforeAll
  static void initLogging() {
    HopLogStore.init();
  }

  @BeforeEach
  void setUp() throws HopException {
    action = new ActionDbt("dbt run");
    action.setDbtProjectName("demo");
    action.setOperation(DbtOperation.RUN.getCode());

    DbtProject project = new DbtProject();
    project.setName("demo");
    project.setProjectDirectory(projectDir.toString());

    MemoryMetadataProvider provider = new MemoryMetadataProvider();
    provider.getSerializer(DbtProject.class).save(project);
    action.setMetadataProvider(provider);
  }

  /** Writes a stub dbt on disk and points the project at it. */
  private void stubDbt(String body) throws IOException, HopException {
    Path script = projectDir.resolve("fake-dbt.sh");
    Files.writeString(script, "#!/bin/sh\n" + body);
    assertTrue(script.toFile().setExecutable(true));

    MemoryMetadataProvider provider = (MemoryMetadataProvider) action.getMetadataProvider();
    DbtProject project = provider.getSerializer(DbtProject.class).load("demo");
    project.setDbtExecutable(script.toString());
    provider.getSerializer(DbtProject.class).save(project);
  }

  @Test
  void successfulRunReportsSuccess() throws Exception {
    stubDbt(
        "mkdir -p target\n"
            + "cat > target/run_results.json <<'JSON'\n"
            + RUN_RESULTS_OK
            + "JSON\n"
            + "exit 0\n");

    Result result = action.execute(new Result(), 0);

    assertTrue(result.getResult());
    assertEquals(0, result.getNrErrors());
  }

  @Test
  void failedNodeRoutesToTheErrorHopEvenWhenDbtExitsZero() throws Exception {
    stubDbt(
        "mkdir -p target\n"
            + "cat > target/run_results.json <<'JSON'\n"
            + RUN_RESULTS_FAILED_TEST
            + "JSON\n"
            + "exit 0\n");

    Result result = action.execute(new Result(), 0);

    assertFalse(result.getResult());
    assertEquals(1, result.getNrErrors());
  }

  @Test
  void previousRunResultsAreNotReportedWhenDbtDiesEarly() throws Exception {
    Path runResults = projectDir.resolve("target").resolve("run_results.json");
    Files.createDirectories(runResults.getParent());
    Files.writeString(runResults, RUN_RESULTS_OK);

    stubDbt("echo 'Compilation Error' >&2\nexit 2\n");

    Result result = action.execute(new Result(), 0);

    assertFalse(result.getResult());
    assertFalse(
        Files.exists(runResults), "the previous run's artifacts must not survive into this run");
  }

  @Test
  void timeoutKillsDbtAndFailsTheAction() throws Exception {
    stubDbt("sleep 60\n");
    action.setTimeout("1");

    long startedAt = System.currentTimeMillis();
    Result result = action.execute(new Result(), 0);
    long elapsedMs = System.currentTimeMillis() - startedAt;

    assertFalse(result.getResult());
    assertEquals(1, result.getNrErrors());
    assertTrue(
        elapsedMs < 30_000, "the action should have been cut short, took " + elapsedMs + "ms");
  }

  @Test
  void invalidTimeoutFailsBeforeStartingDbt() throws Exception {
    stubDbt("exit 0\n");
    action.setTimeout("soon");

    Result result = action.execute(new Result(), 0);

    assertFalse(result.getResult());
  }
}
