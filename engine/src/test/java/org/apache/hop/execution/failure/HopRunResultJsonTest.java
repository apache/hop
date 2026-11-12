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

package org.apache.hop.execution.failure;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.failure.FailureCategory;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.run.HopCommandRun;
import org.apache.hop.run.HopRunResult;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

/** The file hop-run writes with --result-json. */
@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class HopRunResultJsonTest {

  /** hop run, minus the command line: the run is set up by the test. */
  static class ResultCommand extends HopCommandRun {
    ResultCommand(String filename, Path resultFile) {
      this.filename = filename;
      this.resultJsonFile = resultFile == null ? null : resultFile.toString();
    }

    void ran(LocalPipelineEngine pipeline, boolean withoutError) {
      this.pipelineEngine = pipeline;
      this.realFilename = filename;
      setFinishedWithoutError(withoutError);
    }

    void failedWith(Exception e) {
      this.runError = e;
    }

    void write() {
      writeResultJson();
    }
  }

  @BeforeAll
  static void beforeAll() throws Exception {
    HopEnvironment.init();
    PluginRegistry.getInstance()
        .registerPluginClass(
            ExecutionFailurePipelineTest.FailingMeta.class.getName(),
            TransformPluginType.class,
            Transform.class);
  }

  private static HopRunResult read(Path file) throws Exception {
    return HopJson.newMapper().readValue(Files.readString(file), HopRunResult.class);
  }

  @Test
  void failedPipeline(@TempDir Path folder) throws Exception {
    LocalPipelineEngine pipeline =
        new ExecutionFailurePipelineTest()
            .pipelineWith(ExecutionFailurePipelineTest.Mode.RUN_THROWS);
    ExecutionFailurePipelineTest.run(pipeline);

    Path file = folder.resolve("result.json");
    ResultCommand command = new ResultCommand("/projects/sales/orders.hpl", file);
    command.ran(pipeline, false);
    command.write();

    HopRunResult result = read(file);
    assertEquals(HopRunResult.SCHEMA_VERSION, result.getSchemaVersion());
    assertEquals(HopRunResult.TYPE_PIPELINE, result.getType());
    assertEquals("/projects/sales/orders.hpl", result.getFilename());
    assertEquals("failure-test", result.getName());
    assertEquals(pipeline.getLogChannelId(), result.getExecutionId());
    assertFalse(result.isSuccess());
    assertTrue(result.getCounters().get("errors") > 0);
    assertNotNull(result.getStartDate());
    assertEquals(FailureCategory.CONNECTIVITY, result.getFailure().getCategory());
    assertEquals("Failing", result.getFailure().getRootCause().getComponent().getName());
    assertEquals(1, result.getFailures().size());
    assertNull(result.getError());
  }

  @Test
  void succeedingPipeline(@TempDir Path folder) throws Exception {
    LocalPipelineEngine pipeline =
        new ExecutionFailurePipelineTest().pipelineWith(ExecutionFailurePipelineTest.Mode.SUCCEED);
    ExecutionFailurePipelineTest.run(pipeline);

    Path file = folder.resolve("result.json");
    ResultCommand command = new ResultCommand("orders.hpl", file);
    command.ran(pipeline, true);
    command.write();

    HopRunResult result = read(file);
    assertTrue(result.isSuccess());
    assertNull(result.getFailure());
    assertTrue(result.getFailures().isEmpty());
    assertEquals(0L, result.getCounters().get("errors"));
  }

  /** hop-run fails before there is a run: the file says why. */
  @Test
  void noRun(@TempDir Path folder) throws Exception {
    Path file = folder.resolve("result.json");
    ResultCommand command = new ResultCommand("/projects/sales/missing.hpl", file);
    command.failedWith(
        new HopException("Unable to load the pipeline", new FileNotFoundException("missing.hpl")));
    command.write();

    HopRunResult result = read(file);
    assertFalse(result.isSuccess());
    assertEquals(HopRunResult.TYPE_PIPELINE, result.getType());
    assertNull(result.getExecutionId());
    assertEquals(FileNotFoundException.class.getName(), result.getError().getRootCauseClass());
    assertTrue(result.getError().isRedacted());
  }

  @Test
  void nothingIsWrittenWithoutTheOption(@TempDir Path folder) {
    ResultCommand command = new ResultCommand("orders.hpl", null);
    command.write();
    try (var files = Files.list(folder)) {
      assertEquals(0, files.count());
    } catch (Exception e) {
      throw new AssertionError(e);
    }
  }
}
