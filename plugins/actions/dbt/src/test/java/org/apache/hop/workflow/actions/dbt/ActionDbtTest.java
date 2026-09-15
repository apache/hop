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
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class ActionDbtTest {

  @BeforeAll
  static void initLogging() {
    // The action logs through Hop's central log store, which is not initialised by default.
    HopLogStore.init();
  }

  /** An action whose lineage settings come from a stand-in for the sink's variable space. */
  private static ActionDbt actionWithLineageSettings(String... nameValuePairs) {
    Variables lineage = new Variables();
    for (int i = 0; i < nameValuePairs.length; i += 2) {
      lineage.setVariable(nameValuePairs[i], nameValuePairs[i + 1]);
    }
    ActionDbt action =
        new ActionDbt("nightly models") {
          @Override
          IVariables lineageVariables() {
            return lineage;
          }
        };
    configure(action);
    return action;
  }

  private static ActionDbt configuredAction() {
    ActionDbt action = new ActionDbt("nightly models");
    configure(action);
    return action;
  }

  private static void configure(ActionDbt action) {
    action.setDbtProjectName("analytics");
    action.setOperation(DbtOperation.BUILD.getCode());
    action.setSelect("tag:nightly");
    action.setExclude("model_x");
    action.setTarget("prod");
    action.setThreads("8");
    action.setTimeout("900");
    action.setFullRefresh(true);
    action.setEmitOpenLineage(true);
    action.getVars().add(new DbtNameValue("region", "eu"));
    action.getEnvVars().add(new DbtNameValue("DBT_PASSWORD", "${vault:secret/dbt:password}"));
  }

  @Test
  void cloneDoesNotShareVarLists() {
    ActionDbt original = configuredAction();
    ActionDbt copy = (ActionDbt) original.clone();

    assertNotSame(original.getVars(), copy.getVars());
    assertNotSame(original.getVars().get(0), copy.getVars().get(0));
    assertNotSame(original.getEnvVars(), copy.getEnvVars());

    copy.getVars().clear();
    copy.getVars().add(new DbtNameValue("region", "us"));
    copy.getEnvVars().clear();

    assertEquals(1, original.getVars().size());
    assertEquals("eu", original.getVars().get(0).getValue());
    assertEquals(1, original.getEnvVars().size());
  }

  @Test
  void openLineageEnvUsesTheHopSinkSettings() {
    ActionDbt action =
        actionWithLineageSettings(
            "HOP_LINEAGE_OPENLINEAGE_URL", "http://marquez:5000/api/v1/lineage",
            "HOP_LINEAGE_OPENLINEAGE_NAMESPACE", "warehouse",
            "HOP_LINEAGE_OPENLINEAGE_API_KEY", "s3cret");

    DbtCommandBuilder builder = new DbtCommandBuilder();
    action.applyOpenLineageEnv(builder);
    Map<String, String> env = builder.buildEnv();

    // Hop's variable holds the full endpoint; OpenLineage wants the root and the path apart,
    // because its client appends the endpoint to whatever OPENLINEAGE_URL says.
    assertEquals("http://marquez:5000", env.get("OPENLINEAGE_URL"));
    assertEquals("api/v1/lineage", env.get("OPENLINEAGE_ENDPOINT"));
    assertEquals("warehouse", env.get("OPENLINEAGE_NAMESPACE"));
    assertEquals("s3cret", env.get("OPENLINEAGE_API_KEY"));
  }

  @Test
  void collectorUrlIsSplitIntoRootAndEndpoint() {
    assertEquals(
        "http://marquez:5000", ActionDbt.collectorBaseUrl("http://marquez:5000/api/v1/lineage"));
    assertEquals(
        "api/v1/lineage", ActionDbt.collectorEndpoint("http://marquez:5000/api/v1/lineage"));

    // A bare host has no endpoint of its own, so dbt's default applies.
    assertEquals(
        "https://collector.example.com",
        ActionDbt.collectorBaseUrl("https://collector.example.com"));
    assertEquals("", ActionDbt.collectorEndpoint("https://collector.example.com"));
    assertEquals("", ActionDbt.collectorEndpoint("https://collector.example.com/"));

    // Anything that is not a URL is handed over untouched rather than mangled.
    assertEquals("not a url", ActionDbt.collectorBaseUrl("not a url"));
    assertEquals("", ActionDbt.collectorEndpoint("not a url"));
  }

  @Test
  void collectorEndpointKeepsANestedPath() {
    assertEquals(
        "http://gateway:8080",
        ActionDbt.collectorBaseUrl("http://gateway:8080/lineage/api/v1/lineage"));
    assertEquals(
        "lineage/api/v1/lineage",
        ActionDbt.collectorEndpoint("http://gateway:8080/lineage/api/v1/lineage"));
  }

  @Test
  void openLineageEnvNeverPassesAnUnresolvedVariable() {
    // Hop leaves an undefined ${VAR} in place, so reading these through resolve() would hand dbt
    // the literal expression as its collector URL.
    ActionDbt action = actionWithLineageSettings();

    DbtCommandBuilder builder = new DbtCommandBuilder();
    action.applyOpenLineageEnv(builder);
    Map<String, String> env = builder.buildEnv();

    assertNull(env.get("OPENLINEAGE_URL"));
    assertNull(env.get("OPENLINEAGE_ENDPOINT"));
    assertNull(env.get("OPENLINEAGE_API_KEY"));
    assertEquals("hop", env.get("OPENLINEAGE_NAMESPACE"));
    for (String value : env.values()) {
      assertFalse(value.contains("${"), "unresolved variable leaked into the dbt env: " + value);
    }
  }

  @Test
  void checkReportsMissingProjectReference() throws HopException {
    ActionDbt action = new ActionDbt("dbt");
    List<ICheckResult> remarks = new ArrayList<>();

    action.check(remarks, null, null, null);

    assertEquals(1, remarks.size());
    assertEquals(ICheckResult.TYPE_RESULT_ERROR, remarks.get(0).getType());
  }

  @Test
  void checkReportsNonNumericThreadsAndTimeout() throws HopException {
    ActionDbt action = configuredAction();
    action.setThreads("many");
    action.setTimeout("soon");
    List<ICheckResult> remarks = new ArrayList<>();

    action.check(remarks, null, null, null);

    assertEquals(
        2, remarks.stream().filter(r -> r.getType() == ICheckResult.TYPE_RESULT_ERROR).count());
  }

  @Test
  void checkAcceptsVariableDrivenThreads() throws HopException {
    ActionDbt action = configuredAction();
    action.setThreads("${DBT_THREADS}");
    action.setTimeout("${DBT_TIMEOUT}");
    List<ICheckResult> remarks = new ArrayList<>();

    action.check(remarks, null, null, null);

    assertTrue(remarks.stream().noneMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_ERROR));
  }
}
