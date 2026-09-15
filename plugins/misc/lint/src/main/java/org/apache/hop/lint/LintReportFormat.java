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

import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;

/** Output formats the CLI can render lint results in. */
public enum LintReportFormat {

  /** Human-readable summary, the default for interactive runs. */
  TEXT("text"),

  /** Flat JSON, for scripting and for tools with their own ingestion format. */
  JSON("json"),

  /**
   * SARIF 2.1.0, the format GitHub code scanning, Azure DevOps and most review tooling consume.
   * This is what makes lint findings show up as annotations on a pull request.
   */
  SARIF("sarif");

  private final String id;

  LintReportFormat(String id) {
    this.id = id;
  }

  public String getId() {
    return id;
  }

  public static LintReportFormat parse(String value) {
    if (value == null || value.trim().isEmpty()) {
      return TEXT;
    }
    String normalised = value.trim().toLowerCase(Locale.ROOT);
    for (LintReportFormat format : values()) {
      if (format.id.equals(normalised)) {
        return format;
      }
    }
    throw new IllegalArgumentException(
        "Unknown output format '"
            + value
            + "'. Supported formats: "
            + Arrays.stream(values())
                .map(LintReportFormat::getId)
                .collect(Collectors.joining(", ")));
  }
}
