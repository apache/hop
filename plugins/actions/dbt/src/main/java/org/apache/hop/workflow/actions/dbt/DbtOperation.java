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

import java.util.List;

/**
 * The dbt Core sub-command this action runs. The {@link #getCliTokens()} are the argv tokens that
 * follow the executable (some operations, like source-freshness, are two tokens).
 *
 * <p>This deliberately targets the stable dbt CLI contract rather than dbt's Python API: dbt is
 * mid-transition from 1.x to Fusion / Core 2.0 and the CLI is the durable seam.
 */
public enum DbtOperation {
  RUN("run", List.of("run")),
  BUILD("build", List.of("build")),
  TEST("test", List.of("test")),
  SEED("seed", List.of("seed")),
  SNAPSHOT("snapshot", List.of("snapshot")),
  COMPILE("compile", List.of("compile")),
  SOURCE_FRESHNESS("source-freshness", List.of("source", "freshness"));

  private final String code;
  private final List<String> cliTokens;

  DbtOperation(String code, List<String> cliTokens) {
    this.code = code;
    this.cliTokens = cliTokens;
  }

  /** Stable code persisted in the action XML (do not rename without a migration). */
  public String getCode() {
    return code;
  }

  public List<String> getCliTokens() {
    return cliTokens;
  }

  /** Whether {@code --full-refresh} is a meaningful flag for this operation. */
  public boolean supportsFullRefresh() {
    return this == RUN || this == BUILD || this == SEED;
  }

  /** Resolves a persisted code, falling back to {@link #RUN} for anything unrecognised. */
  public static DbtOperation fromCode(String code) {
    DbtOperation op = fromNullableCode(code);
    return op == null ? RUN : op;
  }

  /** Resolves a persisted code, or {@code null} when it names no known operation. */
  public static DbtOperation fromNullableCode(String code) {
    if (code == null || code.isBlank()) {
      return null;
    }
    for (DbtOperation op : values()) {
      if (op.code.equalsIgnoreCase(code.trim())) {
        return op;
      }
    }
    return null;
  }
}
