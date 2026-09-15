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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class LintCanvasOverlayHelperTest {

  @Test
  public void indexesResultsByTransformAndActionName() {
    List<LintResult> results =
        Arrays.asList(
            new LintResult(
                "TRANS-001",
                "Rule 1",
                "WARNING",
                "msg1",
                "a.hpl",
                LintSourceRef.transform("Table Input"),
                LintResult.Origin.LINT),
            new LintResult(
                "TRANS-002",
                "Rule 2",
                "ERROR",
                "msg2",
                "a.hpl",
                LintSourceRef.transform("Table Input"),
                LintResult.Origin.LINT),
            new LintResult(
                "WF-001",
                "Rule 3",
                "WARNING",
                "msg3",
                "b.hwf",
                LintSourceRef.action("Start"),
                LintResult.Origin.LINT),
            new LintResult(
                "DOC-001",
                "Rule 4",
                "ERROR",
                "msg4",
                "a.hpl",
                LintSourceRef.pipeline("a.hpl"),
                LintResult.Origin.LINT));

    Map<String, List<LintResult>> byTransform =
        LintCanvasOverlayHelper.indexByElementName(results, LintSourceRef.Kind.TRANSFORM);
    Map<String, List<LintResult>> byAction =
        LintCanvasOverlayHelper.indexByElementName(results, LintSourceRef.Kind.ACTION);

    assertEquals(1, byTransform.size());
    assertEquals(2, byTransform.get("Table Input").size());
    assertEquals(1, byAction.size());
    assertEquals("Start", byAction.keySet().iterator().next());
  }

  @Test
  public void picksWorstSeverityAcrossResults() {
    List<LintResult> warningsOnly =
        Collections.singletonList(
            new LintResult(
                "TRANS-001",
                "Rule 1",
                "WARNING",
                "msg",
                "a.hpl",
                LintSourceRef.transform("A"),
                LintResult.Origin.LINT));

    assertEquals("WARNING", LintCanvasOverlayHelper.worstSeverity(warningsOnly));
    assertNull(LintCanvasOverlayHelper.worstSeverity(Collections.emptyList()));

    List<LintResult> mixed =
        Arrays.asList(
            warningsOnly.get(0),
            new LintResult(
                "TRANS-002",
                "Rule 2",
                "ERROR",
                "msg",
                "a.hpl",
                LintSourceRef.transform("A"),
                LintResult.Origin.LINT));
    assertEquals("ERROR", LintCanvasOverlayHelper.worstSeverity(mixed));
  }

  @Test
  public void ignoresResultsWithoutMatchingSourceKind() {
    List<LintResult> results =
        Collections.singletonList(new LintResult("DOC-001", "Doc", "ERROR", "msg", "a.hpl"));

    Map<String, List<LintResult>> indexed =
        LintCanvasOverlayHelper.indexByElementName(results, LintSourceRef.Kind.TRANSFORM);
    assertTrue(indexed.isEmpty());
  }
}
