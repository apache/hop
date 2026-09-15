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

/** Identifies the Hop element a lint result applies to (for navigation and deduplication). */
public final class LintSourceRef {

  public enum Kind {
    PIPELINE,
    WORKFLOW,
    TRANSFORM,
    ACTION,
    HOP,
    METADATA,
    FILE
  }

  private final Kind kind;
  private final String name;

  private LintSourceRef(Kind kind, String name) {
    this.kind = kind;
    this.name = name;
  }

  public static LintSourceRef pipeline(String name) {
    return new LintSourceRef(Kind.PIPELINE, name);
  }

  public static LintSourceRef workflow(String name) {
    return new LintSourceRef(Kind.WORKFLOW, name);
  }

  public static LintSourceRef transform(String name) {
    return new LintSourceRef(Kind.TRANSFORM, name);
  }

  public static LintSourceRef action(String name) {
    return new LintSourceRef(Kind.ACTION, name);
  }

  public static LintSourceRef hop(String name) {
    return new LintSourceRef(Kind.HOP, name);
  }

  public static LintSourceRef metadata(String name) {
    return new LintSourceRef(Kind.METADATA, name);
  }

  public static LintSourceRef file(String name) {
    return new LintSourceRef(Kind.FILE, name);
  }

  public Kind getKind() {
    return kind;
  }

  public String getName() {
    return name;
  }

  public boolean hasName() {
    return name != null && !name.isEmpty();
  }
}
