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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The baseline is what lets the linter be adopted on a project that already exists: record today's
 * findings, then fail only on what is added afterwards.
 */
public class LintBaselineTest {

  private static final Path ROOT = Paths.get("/projects/sales").toAbsolutePath();

  private LintResult finding(String ruleId, String relativePath, String sourceName) {
    return new LintResult(
        ruleId,
        ruleId,
        "ERROR",
        "message with a value that may change",
        ROOT.resolve(relativePath).toString(),
        sourceName == null ? null : LintSourceRef.transform(sourceName),
        LintResult.Origin.LINT);
  }

  @Test
  public void recordedFindingsAreHiddenAndNewOnesReported(@TempDir Path dir) throws IOException {
    Path baselineFile = dir.resolve("baseline.json");
    List<LintResult> existing =
        List.of(
            finding("DB-001", "metadata/rdbms/OLD.json", "OLD"),
            finding("TRANS-002", "pipelines/a.hpl", "Orphan"));

    LintBaseline.write(baselineFile, existing, ROOT);
    LintBaseline baseline = LintBaseline.read(baselineFile);

    assertEquals(0, baseline.filter(existing, ROOT).size());

    List<LintResult> afterAChange = new ArrayList<>(existing);
    afterAChange.add(finding("DB-001", "metadata/rdbms/NEW.json", "NEW"));

    List<LintResult> fresh = baseline.filter(afterAChange, ROOT);
    assertEquals(1, fresh.size());
    assertTrue(fresh.get(0).getFileName().endsWith("NEW.json"));
  }

  /**
   * Matching ignores the message, which embeds values that change for the same underlying problem —
   * a rotated password is not a new finding.
   */
  @Test
  public void aChangedMessageIsNotANewFinding(@TempDir Path dir) throws IOException {
    Path baselineFile = dir.resolve("baseline.json");
    LintBaseline.write(baselineFile, List.of(finding("DB-001", "a.json", "A")), ROOT);

    LintResult sameProblemDifferentValue =
        new LintResult(
            "DB-001",
            "DB-001",
            "ERROR",
            "a completely different message",
            ROOT.resolve("a.json").toString(),
            LintSourceRef.transform("A"),
            LintResult.Origin.LINT);

    assertTrue(
        LintBaseline.read(baselineFile).filter(List.of(sameProblemDifferentValue), ROOT).isEmpty());
  }

  /** Counts are kept, so a second occurrence in an already-accepted file is still reported. */
  @Test
  public void aSecondOccurrenceInTheSameFileIsReported(@TempDir Path dir) throws IOException {
    Path baselineFile = dir.resolve("baseline.json");
    LintBaseline.write(baselineFile, List.of(finding("BEST-003", "a.hpl", "Dummy 1")), ROOT);

    List<LintResult> now =
        List.of(finding("BEST-003", "a.hpl", "Dummy 1"), finding("BEST-003", "a.hpl", "Dummy 2"));

    List<LintResult> fresh = LintBaseline.read(baselineFile).filter(now, ROOT);

    assertEquals(1, fresh.size());
    assertEquals("Dummy 2", fresh.get(0).getSource().getName());
  }

  /** Fixed findings leave entries behind; reporting the count lets a team prune the file. */
  @Test
  public void staleEntriesAreCounted(@TempDir Path dir) throws IOException {
    Path baselineFile = dir.resolve("baseline.json");
    LintBaseline.write(
        baselineFile,
        List.of(finding("DB-001", "a.json", "A"), finding("DB-001", "b.json", "B")),
        ROOT);

    LintBaseline baseline = LintBaseline.read(baselineFile);
    List<LintResult> afterFixingOne = List.of(finding("DB-001", "a.json", "A"));

    assertEquals(1, baseline.countStaleEntries(afterFixingOne, ROOT));
  }

  /** Absolute paths would make a baseline written on a laptop useless in CI. */
  @Test
  public void baselineStoresProjectRelativePaths(@TempDir Path dir) throws IOException {
    Path baselineFile = dir.resolve("baseline.json");
    LintBaseline.write(baselineFile, List.of(finding("DB-001", "metadata/a.json", "A")), ROOT);

    String contents = Files.readString(baselineFile, StandardCharsets.UTF_8);

    assertTrue(contents.contains("metadata/a.json"));
    assertFalse(contents.contains(ROOT.toString()), "absolute path leaked into the baseline");
  }

  @Test
  public void anUnknownFormatVersionIsRejected(@TempDir Path dir) throws IOException {
    Path baselineFile = dir.resolve("baseline.json");
    Files.writeString(baselineFile, "{\"version\": 99, \"findings\": {}}", StandardCharsets.UTF_8);

    assertThrows(IOException.class, () -> LintBaseline.read(baselineFile));
  }

  @Test
  public void anEmptyBaselineHidesNothing(@TempDir Path dir) throws IOException {
    Path baselineFile = dir.resolve("baseline.json");
    LintBaseline.write(baselineFile, List.of(), ROOT);

    List<LintResult> results = List.of(finding("DB-001", "a.json", "A"));

    assertEquals(1, LintBaseline.read(baselineFile).filter(results, ROOT).size());
  }
}
