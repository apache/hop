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

import java.util.ArrayList;
import java.util.List;

/**
 * Tree node produced by {@link DocumentParser} implementations. Each node may carry a section
 * title, body text, and nested child sections.
 */
public final class DocumentNode {

  private final String title;
  private final String body;
  private final int startPosition;
  private final int bodyStartPosition;
  private final List<DocumentNode> children;

  /**
   * A node whose body starts at {@code startPosition}: a document root, or a body that is not a
   * slice of the source text, such as a summary built by a Hop pipeline or metadata parser.
   */
  public DocumentNode(String title, String body, int startPosition, List<DocumentNode> children) {
    this(title, body, startPosition, startPosition, children);
  }

  /**
   * @param startPosition where the section starts in the source text, its heading included
   * @param bodyStartPosition where {@code body} starts in the source text
   */
  public DocumentNode(
      String title,
      String body,
      int startPosition,
      int bodyStartPosition,
      List<DocumentNode> children) {
    this.title = title != null ? title : "";
    this.body = body != null ? body : "";
    this.startPosition = Math.max(0, startPosition);
    this.bodyStartPosition = Math.max(this.startPosition, bodyStartPosition);
    this.children = children != null ? List.copyOf(children) : List.of();
  }

  public static DocumentNode leaf(String title, String body, int startPosition) {
    return new DocumentNode(title, body, startPosition, List.of());
  }

  public static DocumentNode root(String body) {
    return new DocumentNode("", body, 0, List.of());
  }

  public String getTitle() {
    return title;
  }

  public String getBody() {
    return body;
  }

  public int getStartPosition() {
    return startPosition;
  }

  public int getBodyStartPosition() {
    return bodyStartPosition;
  }

  public List<DocumentNode> getChildren() {
    return children;
  }

  /** Depth-first flattening into sections with breadcrumb paths (root title omitted when empty). */
  public List<DocumentSection> flattenSections() {
    List<DocumentSection> sections = new ArrayList<>();
    flattenSections(new ArrayList<>(), sections);
    return sections;
  }

  private void flattenSections(List<String> path, List<DocumentSection> sections) {
    List<String> currentPath = path;
    if (!title.isEmpty()) {
      currentPath = new ArrayList<>(path);
      currentPath.add(title);
    }

    if (!body.isBlank()) {
      int leadingWhitespace = 0;
      while (Character.isWhitespace(body.charAt(leadingWhitespace))) {
        leadingWhitespace++;
      }
      sections.add(
          new DocumentSection(
              List.copyOf(currentPath),
              body.strip(),
              startPosition,
              bodyStartPosition + leadingWhitespace));
    }

    for (DocumentNode child : children) {
      child.flattenSections(currentPath, sections);
    }
  }

  /** A document section with breadcrumb path and body text. */
  public static final class DocumentSection {
    private final List<String> path;
    private final String body;
    private final int startPosition;
    private final int bodyStartPosition;

    public DocumentSection(List<String> path, String body, int startPosition) {
      this(path, body, startPosition, startPosition);
    }

    /**
     * @param startPosition where the section starts in the source text, its heading included
     * @param bodyStartPosition where {@code body} starts in the source text
     */
    public DocumentSection(
        List<String> path, String body, int startPosition, int bodyStartPosition) {
      this.path = path != null ? List.copyOf(path) : List.of();
      this.body = body != null ? body : "";
      this.startPosition = startPosition;
      this.bodyStartPosition = bodyStartPosition;
    }

    public List<String> getPath() {
      return path;
    }

    public String getBody() {
      return body;
    }

    public int getStartPosition() {
      return startPosition;
    }

    public int getBodyStartPosition() {
      return bodyStartPosition;
    }

    public String breadcrumb() {
      return String.join(" > ", path);
    }
  }
}
