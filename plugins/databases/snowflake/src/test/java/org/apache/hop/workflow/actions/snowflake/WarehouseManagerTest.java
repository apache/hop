/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.workflow.actions.snowflake;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.Result;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.workflow.WorkflowMeta;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class WarehouseManagerTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private WarehouseManager newAction(int managementAction) {
    WarehouseManager action = new WarehouseManager("whm");
    action.setManagementActionById(managementAction);
    action.setVariable("WAREHOUSE", "HOP_WH");
    action.setWarehouseName("${WAREHOUSE}");
    return action;
  }

  @Test
  void warehouseNameIsResolvedForEveryAction() {
    assertTrue(
        newAction(WarehouseManager.MANAGEMENT_ACTION_CREATE)
            .getCreateSQL()
            .contains(" HOP_WH WITH "));
    assertTrue(
        newAction(WarehouseManager.MANAGEMENT_ACTION_ALTER).getAlterSQL().contains(" HOP_WH SET "));
    assertTrue(
        newAction(WarehouseManager.MANAGEMENT_ACTION_DROP).getDropSQL().contains(" HOP_WH;"));
    assertTrue(
        newAction(WarehouseManager.MANAGEMENT_ACTION_RESUME)
            .getResumeSQL()
            .contains(" HOP_WH RESUME;"));
    assertTrue(
        newAction(WarehouseManager.MANAGEMENT_ACTION_SUSPEND)
            .getSuspendSQL()
            .contains(" HOP_WH SUSPEND;"));
  }

  @Test
  void warehouseNameMustBeOneIdentifier() {
    WarehouseManager action = newAction(WarehouseManager.MANAGEMENT_ACTION_CREATE);
    action.setConnection("snowflake");
    assertTrue(action.validate());

    // Quoted names can hold any character
    action.setVariable("WAREHOUSE", "\"My \"\"Warehouse\"\"; 1\"");
    assertTrue(action.validate());
    action.setVariable("WAREHOUSE", "_WH$1");
    assertTrue(action.validate());

    // Empty after resolving
    action.setVariable("WAREHOUSE", "");
    assertFalse(action.validate());
    action.setWarehouseName(null);
    assertFalse(action.validate());

    // Extra statements, spaces or an unbalanced quote are refused
    action.setWarehouseName("${WAREHOUSE}");
    action.setVariable("WAREHOUSE", "X WITH WAREHOUSE_SIZE='XSMALL'; DROP DATABASE prod; --");
    assertFalse(action.validate());
    action.setVariable("WAREHOUSE", "MY WH");
    assertFalse(action.validate());
    action.setVariable("WAREHOUSE", "\"a\"; DROP DATABASE prod; --\"");
    assertFalse(action.validate());
    action.setVariable("WAREHOUSE", "1WH");
    assertFalse(action.validate());
  }

  @Test
  void failedValidationCountsOneError() throws Exception {
    // No connection: validate() fails
    WarehouseManager action = newAction(WarehouseManager.MANAGEMENT_ACTION_RESUME);

    Result result = new Result();
    action.execute(result, 0);

    assertFalse(result.isResult());
    assertEquals(1, result.getNrErrors());
  }

  @Test
  void failedStatementCountsOneError() throws Exception {
    // The connection doesn't exist in the workflow: the statement can't run
    WarehouseManager action = newAction(WarehouseManager.MANAGEMENT_ACTION_RESUME);
    action.setConnection("missing");
    action.setParentWorkflowMeta(new WorkflowMeta());

    Result result = new Result();
    action.execute(result, 0);

    assertFalse(result.isResult());
    assertEquals(1, result.getNrErrors());
  }
}
