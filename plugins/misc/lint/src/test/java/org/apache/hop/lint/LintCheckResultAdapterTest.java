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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import org.apache.hop.pipeline.PipelineMeta;
import org.junit.jupiter.api.Test;

public class LintCheckResultAdapterTest {

  @Test
  public void deduplicatorPrefersLintOverNativeForSameBucket() {
    LintResult lint =
        new LintResult(
            "TRANS-002",
            "Orphaned Transform",
            "WARNING",
            "Transform is not used",
            "/tmp/test.hpl",
            LintSourceRef.transform("Unused"),
            LintResult.Origin.LINT);
    LintResult nativeResult =
        new LintResult(
            "HOP-CHECK",
            "Unused",
            "WARNING",
            "Transform is not used in the pipeline",
            "/tmp/test.hpl",
            LintSourceRef.transform("Unused"),
            LintResult.Origin.HOP_NATIVE);

    List<LintResult> deduped =
        LintResultDeduplicator.deduplicate(Arrays.asList(lint, nativeResult));
    assertEquals(1, deduped.size());
    assertEquals(LintResult.Origin.LINT, deduped.get(0).getOrigin());
  }

  @Test
  public void severityFailOnThreshold() {
    assertTrue(LintSeverity.meetsFailOnThreshold("ERROR", LintSeverity.FailOn.ERROR));
    assertTrue(LintSeverity.meetsFailOnThreshold("ERROR", LintSeverity.FailOn.WARNING));
    assertTrue(LintSeverity.meetsFailOnThreshold("WARNING", LintSeverity.FailOn.WARNING));
    assertTrue(!LintSeverity.meetsFailOnThreshold("WARNING", LintSeverity.FailOn.ERROR));
  }

  @Test
  public void pipelineLevelLintHasNoTransformSource() {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("My Pipeline");
    LintResult doc =
        new LintResult(
            "DOC-001",
            "Pipeline Description Required",
            "WARNING",
            "Missing description",
            "/tmp/test.hpl",
            LintSourceRef.pipeline("My Pipeline"),
            LintResult.Origin.LINT);

    org.apache.hop.core.ICheckResult check =
        LintCheckResultAdapter.toCheckResult(doc, pipelineMeta);

    org.junit.jupiter.api.Assertions.assertNotNull(check);
    org.junit.jupiter.api.Assertions.assertNull(check.getSourceInfo());
  }
}
