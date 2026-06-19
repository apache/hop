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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import lombok.Getter;

/**
 * Approximate hierarchical view of fields inside semi-structured files (JSON, YAML, XML), built
 * from path locators. This is a <b>structural hint</b> for sinks—not a full schema of the on-disk
 * document.
 */
@Getter
public final class FileIoSchemaNode {

  /** Local segment name (e.g. {@code store}, {@code book}, {@code [*]}). */
  private final String segment;

  /** Full path from the synthetic root (syntax-specific). */
  private final String fullPath;

  /**
   * {@code object}, {@code array}, or {@code value}—rough classification for nested vs leaf
   * segments.
   */
  private final String kind;

  private final List<FileIoSchemaNode> children;

  public FileIoSchemaNode(
      String segment, String fullPath, String kind, List<FileIoSchemaNode> children) {
    this.segment = segment == null ? "" : segment;
    this.fullPath = fullPath == null ? "" : fullPath;
    this.kind = kind == null ? "value" : kind;
    this.children =
        children == null ? List.of() : Collections.unmodifiableList(new ArrayList<>(children));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FileIoSchemaNode that = (FileIoSchemaNode) o;
    return segment.equals(that.segment)
        && fullPath.equals(that.fullPath)
        && kind.equals(that.kind)
        && children.equals(that.children);
  }

  @Override
  public int hashCode() {
    return Objects.hash(segment, fullPath, kind, children);
  }
}
