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

package org.apache.hop.lineage.model;

import java.util.List;
import java.util.Objects;
import lombok.Getter;

/**
 * The documentation of a pipeline or workflow, as {@code hop doc} writes it. Carried on a {@link
 * LineageEventKind#DOCUMENTATION} event of that pipeline or workflow.
 */
@Getter
public final class DocumentationLineagePayload implements LineagePayload {

  /** The title of the documentation. */
  private final String title;

  /** The documentation, as Markdown text. */
  private final String markdown;

  /** The project the pipeline or workflow is part of, or null. */
  private final String project;

  /** The transforms of the pipeline or the actions of the workflow, which it documents as well. */
  private final List<String> parts;

  public DocumentationLineagePayload(
      String title, String markdown, String project, List<String> parts) {
    this.title = Objects.requireNonNull(title, "title");
    this.markdown = Objects.requireNonNull(markdown, "markdown");
    this.project = project;
    this.parts = parts == null ? List.of() : List.copyOf(parts);
  }
}
