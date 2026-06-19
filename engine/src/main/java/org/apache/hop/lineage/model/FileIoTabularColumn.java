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

import java.util.Objects;
import lombok.Getter;

/**
 * One logical column as read from or written to a file, including optional source locator
 * (JSONPath, XPath, etc.) when the physical layout is hierarchical.
 */
@Getter
public final class FileIoTabularColumn {

  /** Column or file header name (Hop field name). */
  private final String name;

  /** Hop type description (e.g. , ). */
  private final String hopTypeName;

  private final int length;
  private final int precision;

  /**
   * Locator within the file when applicable (JSONPath, XPath, dotted YAML path). Null or empty when
   * unknown or not used (e.g. positional CSV).
   */
  private final String sourcePath;

  private final FileIoPathSyntax pathSyntax;

  /**
   * True when the Hop transform treats this as a repeating / array-valued path (e.g. JSON Input
   * repeat flag).
   */
  private final boolean repeated;

  public FileIoTabularColumn(
      String name,
      String hopTypeName,
      int length,
      int precision,
      String sourcePath,
      FileIoPathSyntax pathSyntax,
      boolean repeated) {
    this.name = name == null ? "" : name;
    this.hopTypeName = hopTypeName;
    this.length = length;
    this.precision = precision;
    this.sourcePath = sourcePath;
    this.pathSyntax = pathSyntax == null ? FileIoPathSyntax.NONE : pathSyntax;
    this.repeated = repeated;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FileIoTabularColumn that = (FileIoTabularColumn) o;
    return length == that.length
        && precision == that.precision
        && repeated == that.repeated
        && name.equals(that.name)
        && Objects.equals(hopTypeName, that.hopTypeName)
        && Objects.equals(sourcePath, that.sourcePath)
        && pathSyntax == that.pathSyntax;
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, hopTypeName, length, precision, sourcePath, pathSyntax, repeated);
  }
}
