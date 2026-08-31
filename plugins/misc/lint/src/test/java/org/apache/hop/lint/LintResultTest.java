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

import org.junit.jupiter.api.Test;

/** Unit tests for LintResult class */
public class LintResultTest {

  @Test
  public void testConstructor() {
    LintResult result =
        new LintResult("TEST-001", "Test Rule", "ERROR", "Test message", "test.hpl");

    assertEquals("TEST-001", result.getRuleId());
    assertEquals("Test Rule", result.getRuleName());
    assertEquals("ERROR", result.getSeverity());
    assertEquals("Test message", result.getMessage());
    assertEquals("test.hpl", result.getFileName());
  }

  @Test
  public void testGetters() {
    LintResult result =
        new LintResult("DB-001", "Database Rule", "WARNING", "Warning message", "workflow.hwf");

    assertEquals("DB-001", result.getRuleId());
    assertEquals("Database Rule", result.getRuleName());
    assertEquals("WARNING", result.getSeverity());
    assertEquals("Warning message", result.getMessage());
    assertEquals("workflow.hwf", result.getFileName());
  }

  @Test
  public void testToString() {
    LintResult result =
        new LintResult("TRANS-001", "Transform Rule", "ERROR", "Error message", "pipeline.hpl");

    String expected = "[ERROR] TRANS-001 - Transform Rule: Error message (File: pipeline.hpl)";
    assertEquals(expected, result.toString());
  }

  @Test
  public void testToStringWithConnectionFile() {
    LintResult result =
        new LintResult("DB-001", "Database Rule", "WARNING", "Warning message", "connection: BDW");

    String expected = "[WARNING] DB-001 - Database Rule: Warning message (File: connection: BDW)";
    assertEquals(expected, result.toString());
  }

  @Test
  public void testNullValues() {
    LintResult result = new LintResult(null, null, null, null, null);

    assertNull(result.getRuleId());
    assertNull(result.getRuleName());
    assertNull(result.getSeverity());
    assertNull(result.getMessage());
    assertNull(result.getFileName());
  }

  @Test
  public void testEmptyValues() {
    LintResult result = new LintResult("", "", "", "", "");

    assertEquals("", result.getRuleId());
    assertEquals("", result.getRuleName());
    assertEquals("", result.getSeverity());
    assertEquals("", result.getMessage());
    assertEquals("", result.getFileName());
  }
}
