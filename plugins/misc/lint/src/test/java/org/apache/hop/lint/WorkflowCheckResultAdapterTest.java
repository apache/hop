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

import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.junit.jupiter.api.Test;

public class WorkflowCheckResultAdapterTest {

  @Test
  public void workflowLevelLintHasNoActionSource() {
    WorkflowMeta workflowMeta = new WorkflowMeta();
    workflowMeta.setName("My Workflow");
    LintResult doc =
        new LintResult(
            "DOC-WF-001",
            "Workflow Description Required",
            "WARNING",
            "Missing description",
            "/tmp/test.hwf",
            LintSourceRef.workflow("My Workflow"),
            LintResult.Origin.LINT);

    org.apache.hop.core.ICheckResult check =
        WorkflowCheckResultAdapter.toCheckResult(doc, workflowMeta);

    org.junit.jupiter.api.Assertions.assertNotNull(check);
    assertNull(check.getSourceInfo());
  }

  @Test
  public void actionLintMapsToActionMetaSource() {
    WorkflowMeta workflowMeta = new WorkflowMeta();
    // ActionMeta delegates setName() to its IAction, so it needs a real action behind it.
    ActionMeta actionMeta = new ActionMeta(new org.apache.hop.workflow.actions.start.ActionStart());
    actionMeta.setName("Start");
    workflowMeta.addAction(actionMeta);

    LintResult result =
        new LintResult(
            "SEC-003",
            "Hardcoded password",
            "ERROR",
            "msg",
            "/tmp/test.hwf",
            LintSourceRef.action("Start"),
            LintResult.Origin.LINT);

    org.apache.hop.core.ICheckResult check =
        WorkflowCheckResultAdapter.toCheckResult(result, workflowMeta);

    org.junit.jupiter.api.Assertions.assertNotNull(check);
    org.junit.jupiter.api.Assertions.assertEquals(actionMeta.getAction(), check.getSourceInfo());
  }
}
