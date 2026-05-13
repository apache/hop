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
 * Logical schema of data read from or written to a file for {@link FileIoLineagePayload}. Tabular
 * columns describe the actual fields consumed or produced; the structure-roots tree carries an
 * optional merged hierarchy derived from path locators (JSON/YAML/XML).
 */
@Getter
public final class FileIoContentSchema {

  /**
   * Format hint for sinks: {@code csv}, {@code delimited}, {@code json}, {@code yaml}, {@code xml},
   * {@code parquet}, etc.
   */
  private final String formatHint;

  private final List<FileIoTabularColumn> columns;
  private final List<FileIoSchemaNode> structureRoots;

  public FileIoContentSchema(
      String formatHint, List<FileIoTabularColumn> columns, List<FileIoSchemaNode> structureRoots) {
    this.formatHint = formatHint == null ? "" : formatHint;
    this.columns =
        columns == null ? List.of() : Collections.unmodifiableList(new ArrayList<>(columns));
    this.structureRoots =
        structureRoots == null
            ? List.of()
            : Collections.unmodifiableList(new ArrayList<>(structureRoots));
  }

  /**
   * Tabular columns plus {@link FileIoSchemaTrees#mergePaths(List)} for any path-based locators
   * (JSON/YAML/XML).
   */
  public static FileIoContentSchema tabularWithMergedTree(
      String formatHint, List<FileIoTabularColumn> columns) {
    List<FileIoTabularColumn> cols = columns == null ? List.of() : columns;
    return new FileIoContentSchema(formatHint, cols, FileIoSchemaTrees.mergePaths(cols));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FileIoContentSchema that = (FileIoContentSchema) o;
    return formatHint.equals(that.formatHint)
        && columns.equals(that.columns)
        && structureRoots.equals(that.structureRoots);
  }

  @Override
  public int hashCode() {
    return Objects.hash(formatHint, columns, structureRoots);
  }
}
