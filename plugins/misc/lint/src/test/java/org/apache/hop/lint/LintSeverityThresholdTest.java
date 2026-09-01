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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * The build verdict is decided by severity thresholds, so the mapping has to be exact — an
 * off-by-one here is a build that passes when it should not.
 */
public class LintSeverityThresholdTest {

  @Test
  public void failOnErrorIgnoresWarningsAndInfo() {
    LintSeverity.FailOn threshold = LintSeverity.parseFailOn("ERROR");

    assertTrue(LintSeverity.meetsFailOnThreshold("ERROR", threshold));
    assertFalse(LintSeverity.meetsFailOnThreshold("WARNING", threshold));
    assertFalse(LintSeverity.meetsFailOnThreshold("INFO", threshold));
  }

  @Test
  public void failOnWarningAlsoCatchesErrors() {
    LintSeverity.FailOn threshold = LintSeverity.parseFailOn("WARNING");

    assertTrue(LintSeverity.meetsFailOnThreshold("ERROR", threshold));
    assertTrue(LintSeverity.meetsFailOnThreshold("WARNING", threshold));
    assertFalse(LintSeverity.meetsFailOnThreshold("INFO", threshold));
  }

  @Test
  public void failOnNoneNeverFails() {
    LintSeverity.FailOn threshold = LintSeverity.parseFailOn("NONE");

    assertFalse(LintSeverity.meetsFailOnThreshold("ERROR", threshold));
    assertFalse(LintSeverity.meetsFailOnThreshold("WARNING", threshold));
  }

  /** An unrecognised severity must not be treated as harmless. */
  @Test
  public void unknownSeveritiesDoNotSlipPastAnErrorThreshold() {
    LintSeverity.FailOn threshold = LintSeverity.parseFailOn("WARNING");

    assertFalse(LintSeverity.meetsFailOnThreshold(null, threshold));
    assertFalse(LintSeverity.meetsFailOnThreshold("", threshold));
  }
}
