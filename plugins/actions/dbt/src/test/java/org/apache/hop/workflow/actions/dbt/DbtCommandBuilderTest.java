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

package org.apache.hop.workflow.actions.dbt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class DbtCommandBuilderTest {

  @Test
  void buildsRunCommandWithCoreFlags() {
    List<String> cmd =
        new DbtCommandBuilder()
            .executable("dbt")
            .operation(DbtOperation.RUN)
            .projectDir("/proj")
            .profilesDir("/profiles")
            .target("prod")
            .select("tag:nightly")
            .exclude("model_x")
            .threads("4")
            .fullRefresh(true)
            .buildCommand();

    assertEquals(
        List.of(
            "dbt",
            "run",
            "--project-dir",
            "/proj",
            "--profiles-dir",
            "/profiles",
            "--target",
            "prod",
            "--select",
            "tag:nightly",
            "--exclude",
            "model_x",
            "--threads",
            "4",
            "--full-refresh"),
        cmd);
  }

  @Test
  void sourceFreshnessIsTwoTokens() {
    List<String> cmd =
        new DbtCommandBuilder().operation(DbtOperation.SOURCE_FRESHNESS).buildCommand();
    assertEquals(List.of("dbt", "source", "freshness"), cmd);
  }

  @Test
  void fullRefreshIgnoredForTest() {
    List<String> cmd =
        new DbtCommandBuilder().operation(DbtOperation.TEST).fullRefresh(true).buildCommand();
    assertFalse(cmd.contains("--full-refresh"));
  }

  @Test
  void varsRenderedAsJsonObject() {
    List<String> cmd =
        new DbtCommandBuilder().var("region", "eu").var("year", "2026").buildCommand();
    int idx = cmd.indexOf("--vars");
    assertTrue(idx >= 0);
    // dbt vars are typed: 2026 must arrive as a number, not as the string "2026".
    assertEquals("{\"region\": \"eu\", \"year\": 2026}", cmd.get(idx + 1));
  }

  @Test
  void varValuesKeepTheirJsonType() {
    assertEquals("true", DbtCommandBuilder.renderValue("true"));
    assertEquals("false", DbtCommandBuilder.renderValue(" false "));
    assertEquals("null", DbtCommandBuilder.renderValue("null"));
    assertEquals("42", DbtCommandBuilder.renderValue("42"));
    assertEquals("-1.5e3", DbtCommandBuilder.renderValue("-1.5e3"));
    assertEquals("[1, 2]", DbtCommandBuilder.renderValue("[1, 2]"));
    assertEquals("{\"a\": 1}", DbtCommandBuilder.renderValue("{\"a\": 1}"));
  }

  @Test
  void varValuesThatOnlyLookNumericStayStrings() {
    // Zero-padded and separator-carrying values are not valid JSON numbers, and a warehouse
    // identifier like 007 must not be handed to dbt as the number 7.
    assertEquals("\"007\"", DbtCommandBuilder.renderValue("007"));
    assertEquals("\"1_000\"", DbtCommandBuilder.renderValue("1_000"));
    assertEquals("\"2026-01-31\"", DbtCommandBuilder.renderValue("2026-01-31"));
    assertEquals("\"\"", DbtCommandBuilder.renderValue(null));
  }

  @Test
  void loggableCommandMasksVarValues() {
    DbtCommandBuilder builder =
        new DbtCommandBuilder().var("region", "eu").var("api_token", "very-secret");

    int idx = builder.buildLoggableCommand().indexOf("--vars");
    assertEquals(
        "{\"region\": \"***\", \"api_token\": \"***\"}",
        builder.buildLoggableCommand().get(idx + 1));
    // ... while the argv actually handed to dbt is untouched.
    assertEquals(
        "{\"region\": \"eu\", \"api_token\": \"very-secret\"}",
        builder.buildCommand().get(builder.buildCommand().indexOf("--vars") + 1));
  }

  @Test
  void blankOptionalsAreOmitted() {
    List<String> cmd =
        new DbtCommandBuilder().operation(DbtOperation.SEED).select("  ").target("").buildCommand();
    assertEquals(List.of("dbt", "seed"), cmd);
  }

  @Test
  void envAdditionsCollected() {
    var env =
        new DbtCommandBuilder()
            .envVar("OPENLINEAGE_URL", "http://marquez:5000/api/v1/lineage")
            .envVar("DBT_PASSWORD", "secret")
            .buildEnv();
    assertEquals("http://marquez:5000/api/v1/lineage", env.get("OPENLINEAGE_URL"));
    assertEquals("secret", env.get("DBT_PASSWORD"));
  }
}
