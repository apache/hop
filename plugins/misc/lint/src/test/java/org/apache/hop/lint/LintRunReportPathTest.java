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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.jupiter.api.Test;

/**
 * How a SARIF report records the configuration and baseline files of a run: never with a local
 * path.
 *
 * @see <a href="https://github.com/apache/hop/issues/9500">#9500</a>
 */
class LintRunReportPathTest {

  private static final Path TARGET = Paths.get("/work/sales/pipelines").toAbsolutePath();

  @Test
  void aFileInTheTargetIsRelativeToIt() {
    assertEquals(
        "hop-lint.yml", LintRun.reportPath(TARGET.resolve("hop-lint.yml").toString(), TARGET));
  }

  @Test
  void aFileAboveTheTargetIsRelativeWithDotDot() {
    assertEquals(
        "../hop-lint.yml",
        LintRun.reportPath(TARGET.getParent().resolve("hop-lint.yml").toString(), TARGET));
    assertEquals(
        "../../shared/baseline.json",
        LintRun.reportPath(
            TARGET.getParent().getParent().resolve("shared/baseline.json").toString(), TARGET));
  }

  /** The Run Linter action may be given a VFS URL for a local file. */
  @Test
  void aLocalFileUrlIsReadAsTheFile() {
    assertEquals(
        "../baseline.json",
        LintRun.reportPath(TARGET.getParent().resolve("baseline.json").toUri().toString(), TARGET));
  }

  /** A remote location names no local folder, so it is kept as written. */
  @Test
  void aRemoteLocationIsKept() {
    assertEquals(
        "s3://lint/sales/baseline.json",
        LintRun.reportPath("s3://lint/sales/baseline.json", TARGET));
  }

  /** hop lint records the baseline as typed, which is relative to the working directory. */
  @Test
  void aRelativeNameIsResolvedFromTheWorkingDirectory() {
    Path baseline = Paths.get("baseline.json").toAbsolutePath();
    assertEquals(
        TARGET.relativize(baseline).toString().replace('\\', '/'),
        LintRun.reportPath("baseline.json", TARGET));
  }

  @Test
  void noFileIsEmpty() {
    assertEquals("", LintRun.reportPath(null, TARGET));
    assertEquals("", LintRun.reportPath("", TARGET));
  }

  @Test
  void withoutATargetOnlyTheFileNameIsKept() {
    assertEquals("baseline.json", LintRun.reportPath("/work/shared/baseline.json", null));
  }
}
