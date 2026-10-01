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
import java.util.Map;
import org.junit.jupiter.api.Test;

public class LintResultGroupingTest {

  @Test
  public void categorizesPipelineWorkflowAndMetadataFiles() {
    assertEquals(LintFileCategory.PIPELINE, LintFileCategory.fromFileName("extract.hpl"));
    assertEquals(LintFileCategory.WORKFLOW, LintFileCategory.fromFileName("main.hwf"));
    assertEquals(LintFileCategory.METADATA, LintFileCategory.fromFileName("connection: BDW"));
    assertEquals(
        LintFileCategory.METADATA, LintFileCategory.fromFileName("metadata/rdbms/local.json"));
    assertEquals(LintFileCategory.OTHER, LintFileCategory.fromFileName("readme.txt"));
  }

  @Test
  public void groupsResultsByCategoryAndFile() {
    List<LintResult> results =
        Arrays.asList(
            new LintResult("TRANS-001", "Rule 1", "ERROR", "msg1", "a.hpl"),
            new LintResult("TRANS-002", "Rule 2", "WARNING", "msg2", "a.hpl"),
            new LintResult("WF-001", "Rule 3", "WARNING", "msg3", "b.hwf"),
            new LintResult("DB-001", "Rule 4", "ERROR", "msg4", "connection: prod"));

    Map<LintFileCategory, Map<String, List<LintResult>>> grouped =
        LintResultGrouping.byCategoryAndFile(results);

    assertEquals(2, grouped.get(LintFileCategory.PIPELINE).get("a.hpl").size());
    assertEquals(1, grouped.get(LintFileCategory.WORKFLOW).get("b.hwf").size());
    assertEquals(1, grouped.get(LintFileCategory.METADATA).get("connection: prod").size());
    assertTrue(grouped.containsKey(LintFileCategory.PIPELINE));
    // TRANS-001 and DB-001 are both ERROR.
    assertEquals(2, LintResultGrouping.countBySeverity(results, "ERROR"));
    assertEquals(2, LintResultGrouping.countBySeverity(results, "WARNING"));
  }
}
