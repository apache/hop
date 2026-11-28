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
import java.util.regex.Pattern;

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

  /** Presets that Airflow expands to a cron expression. */
  static final List<String> CRON_PRESETS =
      List.of("@hourly", "@daily", "@weekly", "@monthly", "@quarterly", "@yearly");

  /** Schedules that are no cron expression, and aliases Airflow keeps as they are. */
  static final List<String> OTHER_SCHEDULES =
      List.of("@once", "@continuous", "@midnight", "@annually");

  static final String CONTINUOUS = "@continuous";

  /** Airflow refuses a DAG with a longer tag. */
  static final int TAG_MAX_LENGTH = 100;

  /** One value of a cron field: a number, {@code *}, {@code ?} or a three-letter name. */
  private static final String CRON_VALUE = "(\\d+|\\*|\\?|[A-Za-z]{3})";

  /**
   * One element of a cron field, as the Airflow Java SDK checks it: a value or a range, maybe with
   * a step, and croniter's {@code L}, {@code W} and {@code #}.
   */
  private static final Pattern CRON_ELEMENT =
      Pattern.compile(
          CRON_VALUE + "(-" + CRON_VALUE + ")?([/#]\\d+)?[LWlw]*|[Ll](-\\d+)?|[Ll][Ww]");

  private static final List<String> CRON_FIELDS =
      List.of("minutes", "hours", "days", "months", "weekdays", "seconds");

  /** The lowest and highest number of each field of {@link #CRON_FIELDS}. */
  private static final int[][] CRON_RANGES = {{0, 59}, {0, 23}, {1, 31}, {1, 12}, {0, 7}, {0, 59}};

  private static final List<String> MONTHS =
      List.of("JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC");
  private static final List<String> WEEKDAYS =
      List.of("SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT");

  /**
   * The settings of when a DAG runs. An object with an error keeps those it could read, so its
   * scheduled runs still come, and fail with the error, instead of stopping without a trace.
   */
  static final List<String> WHEN_KEYS =
      List.of("schedule", "start_date", "end_date", "catchup", "is_paused_upon_creation");

  /** True when the DAG fails fast: Airflow then allows only the default trigger rule. */
  public boolean failFast() {
    return Boolean.TRUE.equals(config.get("fail_fast"));
  }

  /**
   * The same settings with an error, for a DAG that only fails. The settings of when it runs stay,
   * see {@link #WHEN_KEYS}.
   */
  HopDagSettings withError(String newError) {
    return new HopDagSettings(
        name,
        file,
        workflow,
        dagId,
        whenToRun(config),
        tags,
        documentation,
        tasks,
        Map.of(),
        version,
        newError);
  }

  /**
   * The settings of {@link #WHEN_KEYS} of a DAG, without what Airflow would then refuse: catchup
   * without a start date, or a {@value #CONTINUOUS} schedule with more than one active run.
   */
  static Map<String, Object> whenToRun(Map<String, Object> config) {
    Map<String, Object> when = new LinkedHashMap<>();
    for (String key : WHEN_KEYS) {
      if (config.containsKey(key)) {
        when.put(key, config.get(key));
      }
    }
    if (!when.containsKey("start_date")) {
      when.remove("catchup");
    }
    if (CONTINUOUS.equals(when.get("schedule"))) {
      when.put("max_active_runs", 1);
    }
    return when;
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
      String problem = scheduleProblem(schedule);
      if (problem == null) {
        config.put("schedule", schedule);
      } else {
        errors.add("schedule " + schedule + " " + problem);
      }
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
    if (Boolean.TRUE.equals(config.get("catchup"))
        && config.containsKey("schedule")
        && !config.containsKey("start_date")) {
      errors.add("catchup needs a start date, the date Airflow catches up from");
    }
    if (CONTINUOUS.equals(config.get("schedule"))
        && !Integer.valueOf(1).equals(config.get("max_active_runs"))) {
      errors.add(
          "Airflow runs a " + CONTINUOUS + " schedule one run at a time: set max active runs to 1");
    }

    List<String> tags = new ArrayList<>();
    String tagsText = text(node, "tags");
    if (tagsText != null) {
      for (String tag : tagsText.split(",")) {
        String trimmed = tag.trim();
        if (trimmed.length() > TAG_MAX_LENGTH) {
          errors.add("tag " + trimmed + " is longer than " + TAG_MAX_LENGTH + " characters");
        } else if (!trimmed.isEmpty() && !tags.contains(trimmed)) {
          tags.add(trimmed);
        }
      }
    }

    String tasks = text(node, "tasks");
    if (HopAirflowBundle.TASKS_BUNDLE_DEFAULT.equals(tasks)) {
      tasks = null;
    }
    if (tasks != null
        && !HopAirflowBundle.TASKS_PER_ACTION.equals(tasks)
        && !HopAirflowBundle.TASKS_WHOLE_WORKFLOW.equals(tasks)) {
      errors.add(
          "tasks "
              + tasks
              + " is not "
              + HopAirflowBundle.TASKS_BUNDLE_DEFAULT
              + ", "
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
        error == null ? config : whenToRun(config),
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
   * What is wrong with a schedule, or null when Airflow takes it: one of {@link #CRON_PRESETS} or
   * {@link #OTHER_SCHEDULES}, or a cron expression of five fields (or six, with seconds last). The
   * Airflow Java SDK refuses any other schedule while it lists the DAGs, which fails every DAG of
   * the bundle. The numbers are checked against the range of their field as well, as croniter does
   * when Airflow schedules the DAG.
   */
  static String scheduleProblem(String schedule) {
    if (schedule.startsWith("@")) {
      return CRON_PRESETS.contains(schedule) || OTHER_SCHEDULES.contains(schedule)
          ? null
          : "is not one of the presets "
              + String.join(", ", CRON_PRESETS)
              + ", "
              + String.join(", ", OTHER_SCHEDULES);
    }
    String[] fields = schedule.split("\\s+");
    if (fields.length < 5 || fields.length > 6) {
      return "is not a cron expression of 5 fields, such as 0 6 * * *, or a preset, such as"
          + " @daily";
    }
    for (int i = 0; i < fields.length; i++) {
      for (String element : fields[i].split(",", -1)) {
        if (!CRON_ELEMENT.matcher(element).matches() || !inRange(element, i)) {
          return "is not a cron expression: "
              + fields[i]
              + " is not a value, range or step of "
              + CRON_FIELDS.get(i)
              + " ("
              + CRON_RANGES[i][0]
              + "-"
              + CRON_RANGES[i][1]
              + ")";
        }
      }
    }
    return null;
  }

  /** True when the numbers and names of one element of a cron field fit that field. */
  private static boolean inRange(String element, int field) {
    // The step, and the n of L-n or #n, are no values of the field
    String values = element.replaceAll("[/#]\\d+$", "").replaceAll("^[Ll]-\\d+$", "");
    for (String value : values.split("-")) {
      // L and W after a day number, or L and LW on their own
      String plain = value.replaceAll("(?<=\\d)[LWlw]+$", "");
      if (plain.isEmpty() || plain.equals("*") || plain.equals("?") || plain.matches("[LWlw]+")) {
        continue;
      }
      if (Character.isLetter(plain.charAt(0))) {
        String upper = plain.toUpperCase();
        if (!(field == 3 && MONTHS.contains(upper)) && !(field == 4 && WEEKDAYS.contains(upper))) {
          return false;
        }
        continue;
      }
      if (plain.length() > 2) {
        return false;
      }
      int number = Integer.parseInt(plain);
      if (number < CRON_RANGES[field][0] || number > CRON_RANGES[field][1]) {
        return false;
      }
    }
    return true;
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
