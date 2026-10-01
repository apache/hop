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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Builds the dbt CLI argument vector and environment additions from already-resolved inputs. Pure
 * and side-effect free so it can be unit-tested without spawning a process. The action resolves Hop
 * variables/secrets and then hands plain strings to this builder.
 */
public final class DbtCommandBuilder {

  /** JSON number grammar, so only values dbt can read back as numbers are emitted unquoted. */
  private static final Pattern JSON_NUMBER =
      Pattern.compile("-?(0|[1-9]\\d*)(\\.\\d+)?([eE][+-]?\\d+)?");

  private String executable = "dbt";
  private DbtOperation operation = DbtOperation.RUN;
  private String projectDir;
  private String profilesDir;
  private String target;
  private String select;
  private String exclude;
  private String threads;
  private boolean fullRefresh;
  private final Map<String, String> vars = new LinkedHashMap<>();
  private final Map<String, String> env = new LinkedHashMap<>();

  public DbtCommandBuilder executable(String executable) {
    if (notBlank(executable)) {
      this.executable = executable.trim();
    }
    return this;
  }

  public DbtCommandBuilder operation(DbtOperation operation) {
    if (operation != null) {
      this.operation = operation;
    }
    return this;
  }

  public DbtCommandBuilder projectDir(String projectDir) {
    this.projectDir = trimToNull(projectDir);
    return this;
  }

  public DbtCommandBuilder profilesDir(String profilesDir) {
    this.profilesDir = trimToNull(profilesDir);
    return this;
  }

  public DbtCommandBuilder target(String target) {
    this.target = trimToNull(target);
    return this;
  }

  public DbtCommandBuilder select(String select) {
    this.select = trimToNull(select);
    return this;
  }

  public DbtCommandBuilder exclude(String exclude) {
    this.exclude = trimToNull(exclude);
    return this;
  }

  public DbtCommandBuilder threads(String threads) {
    this.threads = trimToNull(threads);
    return this;
  }

  public DbtCommandBuilder fullRefresh(boolean fullRefresh) {
    this.fullRefresh = fullRefresh;
    return this;
  }

  public DbtCommandBuilder var(String name, String value) {
    if (notBlank(name)) {
      vars.put(name.trim(), value == null ? "" : value);
    }
    return this;
  }

  public DbtCommandBuilder envVar(String name, String value) {
    if (notBlank(name)) {
      env.put(name.trim(), value == null ? "" : value);
    }
    return this;
  }

  /** The argv passed to {@link ProcessBuilder} (executable first, then dbt tokens/flags). */
  public List<String> buildCommand() {
    return build(false);
  }

  /**
   * The same argv as {@link #buildCommand()} but with the {@code --vars} values replaced by {@code
   * ***}. Var values are resolved through Hop's variable and secret resolvers, so the real argv
   * must never reach the log.
   */
  public List<String> buildLoggableCommand() {
    return build(true);
  }

  private List<String> build(boolean maskVars) {
    List<String> cmd = new ArrayList<>();
    cmd.add(executable);
    cmd.addAll(operation.getCliTokens());
    if (projectDir != null) {
      cmd.add("--project-dir");
      cmd.add(projectDir);
    }
    if (profilesDir != null) {
      cmd.add("--profiles-dir");
      cmd.add(profilesDir);
    }
    if (target != null) {
      cmd.add("--target");
      cmd.add(target);
    }
    if (select != null) {
      cmd.add("--select");
      cmd.add(select);
    }
    if (exclude != null) {
      cmd.add("--exclude");
      cmd.add(exclude);
    }
    if (threads != null) {
      cmd.add("--threads");
      cmd.add(threads);
    }
    if (fullRefresh && operation.supportsFullRefresh()) {
      cmd.add("--full-refresh");
    }
    if (!vars.isEmpty()) {
      cmd.add("--vars");
      cmd.add(toInlineYaml(vars, maskVars));
    }
    return cmd;
  }

  /** Environment additions to overlay on the inherited environment (secrets + OpenLineage). */
  public Map<String, String> buildEnv() {
    return new LinkedHashMap<>(env);
  }

  /**
   * Renders vars as a JSON object string, which dbt accepts as the {@code --vars} value (JSON is
   * valid YAML). Built directly (no shell) so no extra quoting is needed by the caller.
   */
  static String toInlineYaml(Map<String, String> vars) {
    return toInlineYaml(vars, false);
  }

  static String toInlineYaml(Map<String, String> vars, boolean maskValues) {
    StringBuilder sb = new StringBuilder("{");
    boolean first = true;
    for (Map.Entry<String, String> e : vars.entrySet()) {
      if (!first) {
        sb.append(", ");
      }
      first = false;
      sb.append(quote(e.getKey()))
          .append(": ")
          .append(maskValues ? quote("***") : renderValue(e.getValue()));
    }
    return sb.append("}").toString();
  }

  /**
   * Renders one var value as JSON. dbt vars are typed: a model reading {@code var('year')} for an
   * arithmetic comparison needs a number, not a string. Values that are JSON literals ({@code
   * true}/{@code false}/{@code null} or a JSON number) or that already look like a JSON object or
   * array are passed through verbatim; everything else is emitted as a quoted string. Note that
   * only a *valid* JSON number is passed through, so a zero-padded value like {@code 007} stays a
   * string.
   */
  static String renderValue(String value) {
    if (value == null) {
      return quote("");
    }
    String trimmed = value.trim();
    if (trimmed.isEmpty()) {
      return quote(value);
    }
    if ("true".equals(trimmed) || "false".equals(trimmed) || "null".equals(trimmed)) {
      return trimmed;
    }
    if (JSON_NUMBER.matcher(trimmed).matches()) {
      return trimmed;
    }
    char first = trimmed.charAt(0);
    if (first == '{' || first == '[') {
      return trimmed;
    }
    return quote(value);
  }

  private static String quote(String s) {
    StringBuilder sb = new StringBuilder("\"");
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      switch (c) {
        case '"' -> sb.append("\\\"");
        case '\\' -> sb.append("\\\\");
        case '\n' -> sb.append("\\n");
        case '\r' -> sb.append("\\r");
        case '\t' -> sb.append("\\t");
        default -> sb.append(c);
      }
    }
    return sb.append("\"").toString();
  }

  private static boolean notBlank(String s) {
    return s != null && !s.isBlank();
  }

  private static String trimToNull(String s) {
    return notBlank(s) ? s.trim() : null;
  }
}
