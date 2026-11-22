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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.airflow.sdk.Client;
import org.apache.airflow.sdk.Context;
import org.apache.airflow.sdk.Task;

/**
 * The one task of a workflow DAG: runs the workflow as {@code hop run} would. The DAG run conf
 * becomes parameters; the outcome becomes the task's return value (XCom).
 */
public class RunWorkflowTask implements Task {

  private static final ObjectMapper JSON = new ObjectMapper();

  @Override
  public void execute(Context context, Client client) throws Exception {
    BundleManifest bundle = HopBundleMain.bundle();
    HopWorkflowFile workflow = HopBundleMain.workflow(context.ti.dagId);
    if (bundle == null || workflow == null) {
      throw new IllegalStateException("No Hop workflow for DAG " + context.ti.dagId);
    }
    Path resultFile = Files.createTempFile("hop-airflow-result-", ".json");
    try {
      List<String> args = HopCommandLine.build(bundle, workflow, context.dagRun.conf, resultFile);
      HopRuntime runtime = new HopRuntime(HopRuntime.findHopHome());
      runtime.applySystemProperties(System.getenv());
      int exitCode = runtime.execute(args);

      Map<String, Object> outcome = outcome(resultFile, exitCode);
      client.setXCom(outcome);
      if (exitCode != 0) {
        throw new IllegalStateException(
            "Hop workflow "
                + workflow.relativePath()
                + " failed (exit code "
                + exitCode
                + ")"
                + (outcome.get("error") == null ? "" : ": " + outcome.get("error")));
      }
    } finally {
      Files.deleteIfExists(resultFile);
    }
  }

  /**
   * The parts of Hop's result file that are worth passing on: no stack traces, which can be long
   * and hold details of the systems involved.
   */
  @SuppressWarnings("unchecked")
  static Map<String, Object> outcome(Path resultFile, int exitCode) throws Exception {
    Map<String, Object> outcome = new LinkedHashMap<>();
    outcome.put("exitCode", exitCode);
    if (!Files.isRegularFile(resultFile) || Files.size(resultFile) == 0) {
      outcome.put("success", exitCode == 0);
      return outcome;
    }
    Map<String, Object> result = JSON.readValue(resultFile.toFile(), Map.class);
    for (String key :
        List.of(
            "success",
            "stopped",
            "type",
            "name",
            "executionId",
            "runConfiguration",
            "startDate",
            "endDate",
            "counters")) {
      if (result.get(key) != null) {
        outcome.put(key, result.get(key));
      }
    }
    String error = message(result.get("failure"));
    if (error == null) {
      error = message(result.get("error"));
    }
    if (error != null) {
      outcome.put("error", error);
    }
    return outcome;
  }

  /** The message of a failure ({@code cause.message}) or of a cause ({@code message}). */
  @SuppressWarnings("unchecked")
  private static String message(Object value) {
    if (!(value instanceof Map<?, ?> map)) {
      return null;
    }
    Object cause = map.get("cause");
    if (cause instanceof Map<?, ?> causeMap && causeMap.get("message") != null) {
      return String.valueOf(causeMap.get("message"));
    }
    return map.get("message") == null ? null : String.valueOf(map.get("message"));
  }
}
