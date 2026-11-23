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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

/**
 * The arguments of the {@code hop} command that runs one workflow of a bundle, as {@code hop run}
 * would on the command line. The project, and the environment when the stub names files for it, are
 * registered in memory, so nothing is written to the Hop configuration of the worker.
 */
public final class HopCommandLine {

  /**
   * Separates parameters given with {@code -p}. Each parameter has its own {@code -p}, so this only
   * has to be a character no value contains: a value with a comma stays one value.
   */
  static final String PARAMETER_SEPARATOR = "\u001F";

  private static final ObjectMapper JSON = new ObjectMapper();

  /** The subcommand that runs one action; see AirflowActionCommand in the Airflow plugin. */
  static final String ACTION_COMMAND = "airflow-action";

  /**
   * One action to run instead of the whole workflow.
   *
   * @param actionName the action
   * @param stateIn the state of the tasks before it, or null
   * @param stateOut where Hop writes the state the action passes on
   */
  public record ActionRun(String actionName, Path stateIn, Path stateOut) {}

  private HopCommandLine() {}

  /**
   * @param bundle the stub of the bundle
   * @param workflow the workflow to run
   * @param action the only action to run, or null to run the whole workflow
   * @param conf the DAG run conf: each key becomes a parameter, or a variable when the workflow
   *     does not declare it, as {@code hop run -p} does
   * @param resultFile where Hop writes the result of the run
   * @param workerProjects other projects to register, as {@code name=folder} entries
   */
  public static List<String> build(
      BundleManifest bundle,
      HopWorkflowFile workflow,
      ActionRun action,
      Map<String, Object> conf,
      Path resultFile,
      List<String> workerProjects) {
    String project = bundle.projectName();
    String folder = bundle.folder().toString();
    // --project-locations and --environments split on commas
    requireNo(folder, ",", "project folder");
    requireNo(project, ",=", "project name");

    List<String> args = new ArrayList<>();
    // The bundle's project last: it wins over a worker project with the same name
    List<String> locations = new ArrayList<>(workerProjects);
    locations.add(project + "=" + folder);
    args.add("--project-locations");
    args.add(String.join(",", locations));
    if (!bundle.environmentFiles().isEmpty()) {
      StringJoiner files = new StringJoiner(";");
      for (String file : bundle.environmentFiles()) {
        String path = bundle.folder().resolve(file).toString();
        requireNo(path, ",;", "environment file");
        files.add(path);
      }
      args.add("--environments");
      args.add(HopAirflowBundle.ENVIRONMENT_NAME + "=" + project + ":" + files);
    }

    args.add(action == null ? "run" : ACTION_COMMAND);
    if (bundle.environmentFiles().isEmpty()) {
      args.add("--project");
      args.add(project);
    } else {
      args.add("--environment");
      args.add(HopAirflowBundle.ENVIRONMENT_NAME);
    }
    args.add("--runconfig");
    args.add(bundle.runConfiguration());
    args.add("--level");
    args.add(bundle.logLevel());
    args.add("--file");
    args.add(bundle.folder().resolve(workflow.relativePath()).toString());
    if (action != null) {
      args.add("--startaction");
      args.add(action.actionName());
      if (action.stateIn() != null) {
        args.add("--state-in");
        args.add(action.stateIn().toString());
      }
      args.add("--state-out");
      args.add(action.stateOut().toString());
    }
    if (conf != null && !conf.isEmpty()) {
      for (Map.Entry<String, Object> entry : conf.entrySet()) {
        if (entry.getKey() == null || entry.getKey().isBlank()) {
          continue;
        }
        // hop run removes the quotes again, so a value that starts and ends with one keeps it.
        args.add("--parameters");
        args.add(entry.getKey() + "=\"" + text(entry.getValue()) + "\"");
      }
      args.add("--parameters-separator");
      args.add(PARAMETER_SEPARATOR);
    }
    args.add("--result-json");
    args.add(resultFile.toString());
    return args;
  }

  /** Text stays as it is; numbers, booleans, lists and objects become JSON. */
  static String text(Object value) {
    if (value == null) {
      return "";
    }
    if (value instanceof String string) {
      return string;
    }
    try {
      return JSON.writeValueAsString(value);
    } catch (JsonProcessingException e) {
      return String.valueOf(value);
    }
  }

  private static void requireNo(String value, String characters, String what) {
    for (char c : characters.toCharArray()) {
      if (value.indexOf(c) >= 0) {
        throw new IllegalArgumentException(
            "The " + what + " '" + value + "' contains '" + c + "', which Hop cannot pass on");
      }
    }
  }
}
