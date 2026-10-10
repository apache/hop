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
package org.apache.hop.lint;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Renders lint results in machine-readable formats so a CI job can act on them.
 *
 * <p>Text output is fine for a developer reading a terminal, but it is not something a build system
 * can turn into pull-request annotations. SARIF is the format GitHub code scanning and most review
 * tooling ingest; JSON is there for everything else.
 */
public final class LintReportWriter {

  private static final String SARIF_VERSION = "2.1.0";
  private static final String SARIF_SCHEMA =
      "https://raw.githubusercontent.com/oasis-tcs/sarif-spec/master/Schemata/sarif-schema-2.1.0.json";
  private static final String TOOL_NAME = "Apache Hop Lint";

  private LintReportWriter() {}

  public static String render(
      LintReportFormat format, List<LintResult> results, String toolVersion, Path baseDirectory)
      throws JsonProcessingException {
    switch (format) {
      case JSON:
        return renderJson(results, baseDirectory);
      case SARIF:
        return renderSarif(results, toolVersion, baseDirectory);
      case TEXT:
      default:
        return renderText(results, true);
    }
  }

  // ---------------------------------------------------------------- text

  public static String renderText(List<LintResult> results) {
    return renderText(results, true);
  }

  /**
   * @param summary whether to open with the totals; {@code --quiet} reports the findings only
   */
  public static String renderText(List<LintResult> results, boolean summary) {
    StringBuilder out = new StringBuilder();
    if (results.isEmpty()) {
      return "No lint issues found.\n";
    }

    if (summary) {
      out.append("Lint Results Summary:\n");
      out.append("===================\n");
      out.append("Total Issues: ").append(results.size()).append('\n');
      out.append("Errors: ").append(countBySeverity(results, "ERROR")).append('\n');
      out.append("Warnings: ").append(countBySeverity(results, "WARNING")).append('\n');
      out.append("Info: ").append(countBySeverity(results, "INFO")).append("\n\n");
    }

    for (String severity : new String[] {"ERROR", "WARNING", "INFO"}) {
      List<LintResult> group =
          results.stream().filter(r -> severity.equals(r.getSeverity())).toList();
      if (group.isEmpty()) {
        continue;
      }
      out.append('[').append(severity).append("]\n");
      for (LintResult result : group) {
        out.append("  ").append(result).append('\n');
      }
      out.append('\n');
    }
    return out.toString();
  }

  // ---------------------------------------------------------------- json

  private static String renderJson(List<LintResult> results, Path baseDirectory)
      throws JsonProcessingException {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode root = mapper.createObjectNode();

    ObjectNode summary = root.putObject("summary");
    summary.put("total", results.size());
    summary.put("errors", countBySeverity(results, "ERROR"));
    summary.put("warnings", countBySeverity(results, "WARNING"));
    summary.put("info", countBySeverity(results, "INFO"));

    ArrayNode findings = root.putArray("findings");
    for (LintResult result : results) {
      ObjectNode finding = findings.addObject();
      finding.put("ruleId", result.getRuleId());
      finding.put("ruleName", result.getRuleName());
      finding.put("severity", result.getSeverity());
      finding.put("message", result.getMessage());
      finding.put("file", relativise(result.getFileName(), baseDirectory));
      finding.put("origin", result.getOrigin().name());
      if (result.getSource() != null) {
        ObjectNode source = finding.putObject("source");
        source.put("kind", result.getSource().getKind().name());
        source.put("name", result.getSource().getName());
      }
      putTags(finding.putObject("tags"), result.getRuleDetails());
    }
    return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(root) + "\n";
  }

  // --------------------------------------------------------------- sarif

  private static String renderSarif(
      List<LintResult> results, String toolVersion, Path baseDirectory)
      throws JsonProcessingException {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode root = mapper.createObjectNode();
    root.put("$schema", SARIF_SCHEMA);
    root.put("version", SARIF_VERSION);

    ObjectNode run = root.putArray("runs").addObject();
    ObjectNode driver = run.putObject("tool").putObject("driver");
    driver.put("name", TOOL_NAME);
    driver.put("informationUri", "https://hop.apache.org");
    if (toolVersion != null && !toolVersion.isBlank()) {
      driver.put("version", toolVersion);
    }

    // SARIF wants each rule declared once, then referenced by index from every result. The rule is
    // described by the first finding of that id which carries the rule's details, so a finding
    // without them, such as a remark checked without the native rules, cannot hide the tags.
    Map<String, LintResult> describing = new LinkedHashMap<>();
    for (LintResult result : results) {
      LintResult current = describing.get(result.getRuleId());
      if (current == null
          || (LintRuleDetails.NONE.equals(current.getRuleDetails())
              && !LintRuleDetails.NONE.equals(result.getRuleDetails()))) {
        describing.put(result.getRuleId(), result);
      }
    }
    Map<String, Integer> ruleIndex = new LinkedHashMap<>();
    ArrayNode rules = driver.putArray("rules");
    describing.forEach(
        (id, result) -> {
          ruleIndex.put(id, rules.size());
          ObjectNode rule = rules.addObject();
          rule.put("id", id);
          // The id is the only stable per-rule name available: Hop's own verify remarks all share
          // one rule id while carrying the transform name as their result name, so using that
          // here would label the rule after whichever transform happened to be reported first.
          rule.put("name", id);
          rule.putObject("shortDescription")
              .put("text", result.getRuleName() != null ? result.getRuleName() : id);
          LintRuleDetails details = result.getRuleDetails();
          if (!details.description().isBlank()) {
            rule.putObject("fullDescription").put("text", details.description());
          }
          if (details.helpUri() != null && !details.helpUri().isBlank()) {
            rule.put("helpUri", details.helpUri());
          }
          rule.putObject("defaultConfiguration").put("level", sarifLevel(result.getSeverity()));
          if (details.hasTags()) {
            ObjectNode properties = rule.putObject("properties");
            // GitHub code scanning and Azure DevOps read tags as a flat list of strings, so a
            // key with several values becomes several key:value entries.
            ArrayNode flat = properties.putArray("tags");
            LintRuleDetails.flatTags(details.tags()).forEach(flat::add);
            putTags(properties.putObject("hopTags"), details);
          }
        });

    ArrayNode sarifResults = run.putArray("results");
    for (LintResult result : results) {
      ObjectNode entry = sarifResults.addObject();
      entry.put("ruleId", result.getRuleId());
      entry.put("ruleIndex", ruleIndex.get(result.getRuleId()));
      entry.put("level", sarifLevel(result.getSeverity()));
      entry.putObject("message").put("text", messageFor(result));

      ObjectNode artifact =
          entry
              .putArray("locations")
              .addObject()
              .putObject("physicalLocation")
              .putObject("artifactLocation");
      artifact.put("uri", relativise(result.getFileName(), baseDirectory));
      artifact.put("uriBaseId", "%SRCROOT%");
    }

    return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(root) + "\n";
  }

  /**
   * Name the transform or action in the message. SARIF locates findings by file and line, and a
   * lint rule has no line number to give, so without this every finding in a pipeline would be
   * indistinguishable on a pull request.
   */
  private static String messageFor(LintResult result) {
    if (result.getSource() == null || result.getSource().getName() == null) {
      return result.getMessage();
    }
    return result.getSource().getName() + ": " + result.getMessage();
  }

  /**
   * Write a rule's tags as an object of arrays. Every value is an array, also a single one, so a
   * consumer reads one shape whatever the pack wrote.
   */
  private static void putTags(ObjectNode target, LintRuleDetails details) {
    details
        .tags()
        .forEach(
            (key, values) -> {
              ArrayNode array = target.putArray(key);
              values.forEach(array::add);
            });
  }

  private static String sarifLevel(String severity) {
    if ("ERROR".equalsIgnoreCase(severity)) {
      return "error";
    }
    if ("WARNING".equalsIgnoreCase(severity)) {
      return "warning";
    }
    return "note";
  }

  private static String relativise(String fileName, Path baseDirectory) {
    if (fileName == null) {
      return "";
    }
    if (baseDirectory == null) {
      return fileName;
    }
    try {
      // Normalised on both sides, as in LintPolicy.relativise.
      Path path = Paths.get(fileName).normalize();
      Path base = baseDirectory.normalize();
      if (path.isAbsolute() && path.startsWith(base)) {
        // Reports are read on other machines and SARIF wants a URI: always use forward slashes.
        return base.relativize(path).toString().replace('\\', '/');
      }
    } catch (Exception ignored) {
      // Not a real path (metadata findings use labels like "connection: prod"); pass it through.
    }
    return fileName;
  }

  private static long countBySeverity(List<LintResult> results, String severity) {
    return results.stream().filter(r -> severity.equals(r.getSeverity())).count();
  }
}
