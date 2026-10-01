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

/** High-level category for grouping lint results by Hop artifact type. */
public enum LintFileCategory {
  PIPELINE("Pipelines", ".hpl"),
  WORKFLOW("Workflows", ".hwf"),
  METADATA("Metadata", null),
  OTHER("Other", null);

  private final String label;
  private final String extension;

  LintFileCategory(String label, String extension) {
    this.label = label;
    this.extension = extension;
  }

  public String getLabel() {
    return label;
  }

  public static LintFileCategory fromFileName(String fileName) {
    if (fileName == null || fileName.isEmpty()) {
      return OTHER;
    }

    String lower = fileName.toLowerCase();

    if (lower.endsWith(".hpl")) {
      return PIPELINE;
    }
    if (lower.endsWith(".hwf")) {
      return WORKFLOW;
    }
    if (lower.startsWith("connection:")
        || lower.startsWith("metadata:")
        || lower.contains("/metadata/")
        || lower.endsWith(".json")
        || lower.endsWith(".xml")) {
      return METADATA;
    }

    return OTHER;
  }
}
