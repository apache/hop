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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class HopCommandLineTest {

  private static final Path FOLDER = Path.of("/bundles/sales/versions/abc");
  private static final Path RESULT = Path.of("/tmp/result.json");
  private static final HopWorkflowFile WORKFLOW =
      new HopWorkflowFile("etl/main.hwf", "sales__etl__main", "");

  private static BundleManifest bundle(Path folder, List<String> environmentFiles) {
    return new BundleManifest(
        folder, "2026-10-30", "", "sales", "local", "Basic", List.of(), environmentFiles, "s__");
  }

  @Test
  void registersTheProjectAndRunsTheWorkflow() {
    assertEquals(
        List.of(
            "--project-locations",
            "sales=/bundles/sales/versions/abc",
            "run",
            "--project",
            "sales",
            "--runconfig",
            "local",
            "--level",
            "Basic",
            "--file",
            "/bundles/sales/versions/abc/etl/main.hwf",
            "--result-json",
            "/tmp/result.json"),
        HopCommandLine.build(
            bundle(FOLDER, List.of()), WORKFLOW, null, Map.of(), RESULT, List.of()));
  }

  @Test
  void registersWorkerProjectsBeforeTheBundleProject() {
    List<String> args =
        HopCommandLine.build(
            bundle(FOLDER, List.of()),
            WORKFLOW,
            null,
            Map.of(),
            RESULT,
            List.of("default=/opt/hop/config/projects/default"));

    assertEquals(
        List.of(
            "--project-locations",
            "default=/opt/hop/config/projects/default,sales=/bundles/sales/versions/abc"),
        args.subList(0, 2));
  }

  @Test
  void registersTheEnvironmentFiles() {
    List<String> args =
        HopCommandLine.build(
            bundle(FOLDER, List.of("env/prod.json", "env/secrets.json")),
            WORKFLOW,
            null,
            null,
            RESULT,
            List.of());

    assertEquals(
        List.of(
            "--project-locations",
            "sales=/bundles/sales/versions/abc",
            "--environments",
            "airflow=sales:/bundles/sales/versions/abc/env/prod.json;"
                + "/bundles/sales/versions/abc/env/secrets.json",
            "run",
            "--environment",
            "airflow"),
        args.subList(0, 7));
  }

  @Test
  void runsOneActionWithTheStateOfTheTasksBefore() {
    List<String> args =
        HopCommandLine.build(
            bundle(FOLDER, List.of()),
            WORKFLOW,
            new HopCommandLine.ActionRun(
                "Load orders", Path.of("/tmp/in.json"), Path.of("/tmp/out.json")),
            Map.of(),
            RESULT,
            List.of());

    int command = args.indexOf("airflow-action");
    assertEquals(-1, args.indexOf("run"));
    int action = args.indexOf("--startaction");
    assertEquals(
        List.of(
            "--startaction",
            "Load orders",
            "--state-in",
            "/tmp/in.json",
            "--state-out",
            "/tmp/out.json"),
        args.subList(action, action + 6));
    assertEquals(true, command > 0 && command < action);
  }

  @Test
  void runsTheFirstActionWithoutState() {
    List<String> args =
        HopCommandLine.build(
            bundle(FOLDER, List.of()),
            WORKFLOW,
            new HopCommandLine.ActionRun("Load orders", null, Path.of("/tmp/out.json")),
            Map.of(),
            RESULT,
            List.of());

    assertEquals(-1, args.indexOf("--state-in"));
    assertEquals("/tmp/out.json", args.get(args.indexOf("--state-out") + 1));
  }

  /** hop run removes the quotes and splits on the separator only, so values arrive as they are. */
  @Test
  void passesTheRunConfAsParameters() {
    Map<String, Object> conf = new LinkedHashMap<>();
    conf.put("TABLES", "orders,customers");
    conf.put("QUOTED", "\"x\"");
    conf.put("COUNT", 3);
    conf.put("LIST", List.of("a", "b"));
    conf.put("EMPTY", null);
    conf.put(" ", "ignored");

    List<String> args =
        HopCommandLine.build(bundle(FOLDER, List.of()), WORKFLOW, null, conf, RESULT, List.of());

    int first = args.indexOf("--parameters");
    assertEquals(
        List.of(
            "--parameters",
            "TABLES=\"orders,customers\"",
            "--parameters",
            "QUOTED=\"\"x\"\"",
            "--parameters",
            "COUNT=\"3\"",
            "--parameters",
            "LIST=\"[\"a\",\"b\"]\"",
            "--parameters",
            "EMPTY=\"\"",
            "--parameters-separator",
            HopCommandLine.PARAMETER_SEPARATOR),
        args.subList(first, first + 12));
  }

  @Test
  void refusesAFolderHopWouldSplit() {
    BundleManifest bundle = bundle(Path.of("/bundles/a,b"), List.of());

    assertThrows(
        IllegalArgumentException.class,
        () -> HopCommandLine.build(bundle, WORKFLOW, null, Map.of(), RESULT, List.of()));
  }
}
