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
package org.apache.hop.pipeline.transforms.chunker.document;

/** Document format hint for structure-aware chunking. */
public enum ContentType {
  /** Detect from text heuristics or {@link ContentTypeResolver#fromSourceType(String)}. */
  AUTO("Auto"),

  /** Unstructured plain text (paragraph fallback only). */
  PLAIN("Plain"),

  /** Markdown with {@code #} headings. */
  MARKDOWN("Markdown"),

  /** AsciiDoc with {@code =} headings. */
  ASCIIDOC("AsciiDoc"),

  /** Hop pipeline (.hpl) XML. */
  PIPELINE("Pipeline"),

  /** Hop workflow (.hwf) XML. */
  WORKFLOW("Workflow"),

  /** Hop project metadata JSON (connections, run configs, …). */
  METADATA("Metadata");

  private final String description;

  ContentType(String description) {
    this.description = description;
  }

  public String getDescription() {
    return description;
  }

  public static ContentType fromString(String value) {
    if (value == null || value.isBlank()) {
      return AUTO;
    }
    for (ContentType type : values()) {
      if (type.name().equalsIgnoreCase(value) || type.description.equalsIgnoreCase(value)) {
        return type;
      }
    }
    return AUTO;
  }
}
