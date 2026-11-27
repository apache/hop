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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * An {@value HopAirflowBundle#DAG_METADATA_KEY} metadata object of the project: one DAG of a
 * workflow, with its schedule and other settings. Read with Jackson from the metadata folder of the
 * bundle: listing the DAGs must not start Hop, so the values are taken as they are, without
 * variables.
 *
 * @param name the name of the object
 * @param file the file of the object, relative to the project, for messages
 * @param workflow the workflow, relative to the project with forward slashes, or null
 * @param dagId the DAG id the object asks for, or null for the default DAG id of the workflow
 * @param config the DAG settings, by the key of the Airflow SDK, in the type it takes
 * @param tags the tags of the DAG, after "hop" and the project name
 * @param documentation the documentation of the DAG, or null
 * @param tasks {@link HopAirflowBundle#TASKS_PER_ACTION}, {@link
 *     HopAirflowBundle#TASKS_WHOLE_WORKFLOW}, or null for the tasks of the bundle
 * @param parameters the parameter values a run gets unless its conf sets them
 * @param version the version of the object, see {@link HopAirflowBundle#DAG_DOC_SETTINGS}
 * @param error what is wrong with the object, or null: its DAG runs no workflow but fails
 */
public record HopDagSettings(
    String name,
    String file,
    String workflow,
    String dagId,
    Map<String, Object> config,
    List<String> tags,
    String documentation,
    String tasks,
    Map<String, String> parameters,
    String version,
    String error) {

  private static final ObjectMapper JSON = new ObjectMapper();
  private static final String PROJECT_HOME = "${PROJECT_HOME}";
  private static final String DEFAULT_METADATA_FOLDER = "metadata";

  /** The values of a setting that Airflow also has a configuration default for. */
  static final String AIRFLOW_DEFAULT = "AIRFLOW_DEFAULT";

  static final String YES = "YES";
  static final String NO = "NO";

  /** Airflow allows only these trigger rules in a DAG that fails fast. */
  static final List<String> FAIL_FAST_TRIGGER_RULES =
      List.of("all_success", "all_done_setup_success");

  /** True when the DAG fails fast: Airflow then allows only the default trigger rule. */
  public boolean failFast() {
    return Boolean.TRUE.equals(config.get("fail_fast"));
  }

  /** The same settings with an error, for a DAG that only fails. */
  HopDagSettings withError(String newError) {
    return new HopDagSettings(
        name,
        file,
        workflow,
        dagId,
        Map.of(),
        tags,
        documentation,
        tasks,
        Map.of(),
        version,
        newError);
  }

  /**
   * The objects in the {@value HopAirflowBundle#DAG_METADATA_KEY} folder of the project's metadata,
   * sorted by file name. An object Hop can't use comes back with an {@link #error()}.
   */
  public static List<HopDagSettings> read(Path project) throws IOException {
    Path folder = metadataFolder(project);
    List<HopDagSettings> settings = new ArrayList<>();
    if (folder == null) {
      return settings;
    }
    Path typeFolder = folder.resolve(HopAirflowBundle.DAG_METADATA_KEY);
    if (!Files.isDirectory(typeFolder)) {
      return settings;
    }
    List<Path> files = new ArrayList<>();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(typeFolder, "*.json")) {
      stream.forEach(files::add);
    }
    for (Path file : files.stream().sorted().toList()) {
      String relative = project.relativize(file).toString().replace('\\', '/');
      settings.add(read(file, relative));
    }
    return settings;
  }

  /**
   * The metadata folder of the project: {@code metadataBaseFolder} of {@code project-config.json}
   * when it is in the project, else {@value #DEFAULT_METADATA_FOLDER}. Null for a folder outside
   * the project, which a bundle does not hold.
   */
  static Path metadataFolder(Path project) throws IOException {
    Path configFile = project.resolve("project-config.json");
    String folder = PROJECT_HOME + "/" + DEFAULT_METADATA_FOLDER;
    if (Files.isRegularFile(configFile)) {
      folder = JSON.readTree(configFile.toFile()).path("metadataBaseFolder").asText(folder);
    }
    String relative = projectRelative(folder);
    if (relative == null) {
      System.out.println(
          "The metadata folder of the project, "
              + folder
              + ", is not a folder in the project: Airflow DAG settings are not read");
      return null;
    }
    return relative.isEmpty() ? project : project.resolve(relative);
  }

  static HopDagSettings read(Path file, String relative) {
    String fileName = file.getFileName().toString();
    String name = fileName.substring(0, fileName.length() - ".json".length());
    String version = null;
    try {
      byte[] content = Files.readAllBytes(file);
      version = HopWorkflowFile.version(content);
      return of(JSON.readTree(content), name, relative, version);
    } catch (Exception e) {
      return new HopDagSettings(
          name,
          relative,
          null,
          null,
          Map.of(),
          List.of(),
          null,
          null,
          Map.of(),
          version,
          "Hop could not read Airflow DAG " + relative + ": " + e.getMessage());
    }
  }

  static HopDagSettings of(JsonNode node, String fileName, String relative, String version) {
    String name = text(node, "name");
    if (name == null) {
      name = fileName;
    }
    List<String> errors = new ArrayList<>();
    String workflowText = text(node, "workflow");
    String workflow = null;
    if (workflowText == null) {
      errors.add("it names no workflow");
    } else {
      workflow = projectRelative(workflowText);
      if (workflow == null || !workflow.endsWith(".hwf")) {
        errors.add(
            "workflow "
                + workflowText
                + " is not a .hwf file in the project. Use a path relative to the project, or one"
                + " that starts with "
                + PROJECT_HOME);
        workflow = null;
      }
    }

    Map<String, Object> config = new LinkedHashMap<>();
    String schedule = text(node, "schedule");
    if (schedule != null) {
      config.put("schedule", schedule);
    }
    date(node, "start_date", "start date", config, errors);
    date(node, "end_date", "end date", config, errors);
    flag(node, "catchup", "catchup", "catchup", config, errors);
    flag(
        node,
        "paused_on_creation",
        "is_paused_upon_creation",
        "paused on creation",
        config,
        errors);
    count(node, "max_active_runs", "max_active_runs", "max active runs", 1, config, errors);
    count(node, "max_active_tasks", "max_active_tasks", "max active tasks", 1, config, errors);
    count(
        node,
        "max_consecutive_failed_runs",
        "max_consecutive_failed_dag_runs",
        "max consecutive failed runs",
        0,
        config,
        errors);
    String timeout = text(node, "run_timeout_seconds");
    if (timeout != null) {
      Long seconds = number(timeout);
      if (seconds == null || seconds < 1) {
        errors.add("run timeout " + timeout + " is not a number of seconds above 0");
      } else {
        config.put("dagrun_timeout", Duration.ofSeconds(seconds));
      }
    }
    if (node.path("fail_fast").asBoolean(false)) {
      config.put("fail_fast", true);
    }
    String displayName = text(node, "display_name");
    if (displayName != null) {
      config.put("dag_display_name", displayName);
    }

    List<String> tags = new ArrayList<>();
    String tagsText = text(node, "tags");
    if (tagsText != null) {
      for (String tag : tagsText.split(",")) {
        if (!tag.isBlank() && !tags.contains(tag.trim())) {
          tags.add(tag.trim());
        }
      }
    }

    String tasks = text(node, "tasks");
    if (tasks != null
        && !HopAirflowBundle.TASKS_PER_ACTION.equals(tasks)
        && !HopAirflowBundle.TASKS_WHOLE_WORKFLOW.equals(tasks)) {
      errors.add(
          "tasks "
              + tasks
              + " is not "
              + HopAirflowBundle.TASKS_PER_ACTION
              + " or "
              + HopAirflowBundle.TASKS_WHOLE_WORKFLOW
              + ": a newer Hop wrote it, use that Hop on the workers");
    }

    Map<String, String> parameters = new LinkedHashMap<>();
    for (JsonNode parameter : node.path("parameters")) {
      String parameterName = text(parameter, "name");
      if (parameterName != null) {
        parameters.put(parameterName, parameter.path("value").asText(""));
      }
    }

    String error =
        errors.isEmpty()
            ? null
            : "Airflow DAG " + name + " (" + relative + "): " + String.join("; ", errors);
    return new HopDagSettings(
        name,
        relative,
        workflow,
        text(node, "dag_id"),
        error == null ? config : Map.of(),
        tags,
        text(node, "documentation"),
        tasks,
        error == null ? parameters : Map.of(),
        version,
        error);
  }

  /**
   * A path in the project, relative to it with forward slashes: as it is, or after {@value
   * #PROJECT_HOME}. Empty for the project itself; null for any other variable, an absolute path or
   * a path that leaves the project.
   */
  public static String projectRelative(String path) {
    String relative = path.trim().replace('\\', '/');
    if (relative.startsWith(PROJECT_HOME)) {
      relative = relative.substring(PROJECT_HOME.length());
      if (!relative.isEmpty() && !relative.startsWith("/")) {
        return null;
      }
      relative = relative.replaceFirst("^/+", "");
    } else if (!WorkerProjects.isPlainRelativePath(relative)) {
      return null;
    }
    if (relative.contains("${")) {
      return null;
    }
    List<String> parts = new ArrayList<>();
    for (String part : relative.split("/")) {
      if (part.isEmpty() || part.equals(".")) {
        continue;
      }
      if (part.equals("..")) {
        if (parts.isEmpty()) {
          return null;
        }
        parts.remove(parts.size() - 1);
      } else {
        parts.add(part);
      }
    }
    return String.join("/", parts);
  }

  /**
   * A date, or a date and time, in UTC: {@code 2026-01-31}, {@code 2026-01-31 06:00} or {@code
   * 2026-01-31T06:00:00}. The Airflow Java SDK cannot give a schedule a time zone, and a fixed
   * offset would move the runs by an hour at each change to or from summer time.
   */
  static OffsetDateTime parseUtc(String text) {
    String value = text.trim().replace(' ', 'T');
    try {
      if (!value.contains("T")) {
        return LocalDate.parse(value).atStartOfDay().atOffset(ZoneOffset.UTC);
      }
      return LocalDateTime.parse(value, DateTimeFormatter.ISO_LOCAL_DATE_TIME)
          .atOffset(ZoneOffset.UTC);
    } catch (DateTimeParseException e) {
      return null;
    }
  }

  private static void date(
      JsonNode node, String key, String what, Map<String, Object> config, List<String> errors) {
    String text = text(node, key);
    if (text == null) {
      return;
    }
    OffsetDateTime date = parseUtc(text);
    if (date == null) {
      errors.add(
          what
              + " "
              + text
              + " is not a date or date and time in UTC, such as 2026-01-31 or 2026-01-31 06:00");
    } else {
      config.put(key, date);
    }
  }

  private static void flag(
      JsonNode node,
      String key,
      String airflowKey,
      String what,
      Map<String, Object> config,
      List<String> errors) {
    String text = text(node, key);
    if (text == null || AIRFLOW_DEFAULT.equals(text)) {
      return;
    }
    if (YES.equals(text)) {
      config.put(airflowKey, true);
    } else if (NO.equals(text)) {
      config.put(airflowKey, false);
    } else {
      errors.add(what + " " + text + " is not " + AIRFLOW_DEFAULT + ", " + YES + " or " + NO);
    }
  }

  private static void count(
      JsonNode node,
      String key,
      String airflowKey,
      String what,
      int minimum,
      Map<String, Object> config,
      List<String> errors) {
    String text = text(node, key);
    if (text == null) {
      return;
    }
    Long value = number(text);
    if (value == null || value < minimum || value > Integer.MAX_VALUE) {
      errors.add(what + " " + text + " is not a whole number of at least " + minimum);
    } else {
      config.put(airflowKey, value.intValue());
    }
  }

  private static Long number(String text) {
    try {
      return Long.parseLong(text.trim());
    } catch (NumberFormatException e) {
      return null;
    }
  }

  /** The text of a field, or null when it is missing or blank. */
  private static String text(JsonNode node, String key) {
    JsonNode value = node.get(key);
    if (value == null || value.isNull()) {
      return null;
    }
    String text = value.asText();
    return text.isBlank() ? null : text.trim();
  }
}
