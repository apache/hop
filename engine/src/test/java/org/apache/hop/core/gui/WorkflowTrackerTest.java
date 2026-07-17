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

package org.apache.hop.core.gui;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Date;
import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.apache.hop.workflow.ActionResult;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.action.IAction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(RestoreHopEnvironmentExtension.class)
class WorkflowTrackerTest {

  // Number of workflow trackers should be limited by HOP_MAX_JOB_TRACKER_SIZE
  @Test
  void testAddJobTracker() {
    final String old = System.getProperty(Const.HOP_MAX_WORKFLOW_TRACKER_SIZE);

    final int maxTestSize = 30;
    try {
      System.setProperty(Const.HOP_MAX_WORKFLOW_TRACKER_SIZE, Integer.toString(maxTestSize));

      WorkflowMeta workflowMeta = mock(WorkflowMeta.class);
      WorkflowTracker workflowTracker = new WorkflowTracker(workflowMeta);

      for (int n = 0; n < maxTestSize * 2; n++) {
        workflowTracker.addWorkflowTracker(mock(WorkflowTracker.class));
      }

      assertTrue(
          workflowTracker.getTotalNumberOfItems() <= maxTestSize,
          "More JobTrackers than allowed were added");
    } finally {
      if (old == null) {
        System.clearProperty(Const.HOP_MAX_WORKFLOW_TRACKER_SIZE);
      } else {
        System.setProperty(Const.HOP_MAX_WORKFLOW_TRACKER_SIZE, old);
      }
    }
  }

  @Test
  void findJobTracker_EntryNameIsNull() {
    WorkflowTracker workflowTracker = createTracker();
    workflowTracker.addWorkflowTracker(createTracker());

    ActionMeta actionMeta = createActionMeta(null);

    assertNull(workflowTracker.findWorkflowTracker(actionMeta));
  }

  @Test
  void findJobTracker_EntryNameNotFound() {
    WorkflowTracker workflowTracker = createTracker();
    for (int i = 0; i < 3; i++) {
      workflowTracker.addWorkflowTracker(createTracker(Integer.toString(i)));
    }

    ActionMeta copy = createActionMeta("not match");
    assertNull(workflowTracker.findWorkflowTracker(copy));
  }

  @Test
  void findJobTracker_EntryNameFound() {
    WorkflowTracker workflowTracker = createTracker();
    WorkflowTracker[] children =
        new WorkflowTracker[] {createTracker("0"), createTracker("1"), createTracker("2")};
    for (WorkflowTracker child : children) {
      workflowTracker.addWorkflowTracker(child);
    }

    ActionMeta actionMeta = createActionMeta("1");
    assertEquals(children[1], workflowTracker.findWorkflowTracker(actionMeta));
  }

  /**
   * A tracker is sent to clients that are not running the workflow themselves, so everything the
   * workflow metrics view shows has to survive the round trip. See issue #3685.
   */
  @Test
  void xmlRoundTripKeepsActionResults() throws Exception {
    WorkflowTracker<WorkflowMeta> tracker = new WorkflowTracker<>((WorkflowMeta) null);
    tracker.setWorkflowName("a-workflow");
    tracker.setWorkflowFilename("/tmp/a-workflow.hwf");

    // The start of an action carries no result yet, which is how the metrics view recognizes it.
    ActionResult start = new ActionResult();
    start.setActionName("an action");
    start.setComment("Start of action");
    start.setReason("Started by a hop");
    start.setLogDate(new Date());
    tracker.addWorkflowTracker(new WorkflowTracker<>((WorkflowMeta) null, start));

    Result result = new Result();
    result.setResult(true);
    result.setEntryNr(1);
    result.setElapsedTimeMillis(1234L);

    ActionResult end = new ActionResult();
    end.setActionName("an action");
    end.setComment("Action finished");
    end.setReason("Followed unconditional link");
    end.setLogDate(new Date());
    end.setActionFilename("/tmp/an-action.txt");
    end.setLogChannelId("channel-1");
    end.setBytesRead(11L);
    end.setBytesWritten(22L);
    end.setResult(result);
    tracker.addWorkflowTracker(new WorkflowTracker<>((WorkflowMeta) null, end));

    String xml = tracker.getXml();
    WorkflowTracker<WorkflowMeta> copy =
        WorkflowTracker.fromXml(
            XmlHandler.getSubNode(XmlHandler.loadXmlString(xml), WorkflowTracker.XML_TAG));

    assertEquals(xml, copy.getXml(), "The XML should match after rebuilding from XML");
    assertEquals("a-workflow", copy.getWorkflowName());
    assertEquals("/tmp/a-workflow.hwf", copy.getWorkflowFilename());
    assertEquals(2, copy.nrWorkflowTrackers());

    ActionResult copiedStart = copy.getWorkflowTracker(0).getActionResult();
    assertEquals("an action", copiedStart.getActionName());
    assertEquals("Start of action", copiedStart.getComment());
    assertEquals("Started by a hop", copiedStart.getReason());
    assertNull(copiedStart.getResult(), "A start of action must stay without a result");
    assertEquals(start.getLogDate().toString(), copiedStart.getLogDate().toString());

    ActionResult copiedEnd = copy.getWorkflowTracker(1).getActionResult();
    assertEquals("Action finished", copiedEnd.getComment());
    assertEquals("Followed unconditional link", copiedEnd.getReason());
    assertEquals("/tmp/an-action.txt", copiedEnd.getActionFilename());
    assertEquals("channel-1", copiedEnd.getLogChannelId());
    assertEquals(11L, copiedEnd.getBytesRead());
    assertEquals(22L, copiedEnd.getBytesWritten());
    assertTrue(copiedEnd.getResult().isResult());
    assertEquals(1, copiedEnd.getResult().getEntryNr());
    assertEquals(1234L, copiedEnd.getResult().getElapsedTimeMillis());
  }

  /** Child workflows are nested in the tracker and the metrics view shows them that way. */
  @Test
  void xmlRoundTripKeepsChildWorkflows() throws Exception {
    WorkflowTracker<WorkflowMeta> tracker = new WorkflowTracker<>((WorkflowMeta) null);
    tracker.setWorkflowName("parent");

    WorkflowTracker<WorkflowMeta> child = new WorkflowTracker<>((WorkflowMeta) null);
    child.setWorkflowName("child");
    ActionResult childResult = new ActionResult();
    childResult.setActionName("child action");
    child.addWorkflowTracker(new WorkflowTracker<>((WorkflowMeta) null, childResult));
    tracker.addWorkflowTracker(child);

    String xml = tracker.getXml();
    WorkflowTracker<WorkflowMeta> copy =
        WorkflowTracker.fromXml(
            XmlHandler.getSubNode(XmlHandler.loadXmlString(xml), WorkflowTracker.XML_TAG));

    assertEquals(xml, copy.getXml(), "The XML should match after rebuilding from XML");
    assertEquals(1, copy.nrWorkflowTrackers());

    WorkflowTracker copiedChild = copy.getWorkflowTracker(0);
    assertEquals("child", copiedChild.getWorkflowName());
    assertEquals(1, copiedChild.nrWorkflowTrackers());
    assertEquals(
        "child action", copiedChild.getWorkflowTracker(0).getActionResult().getActionName());
    assertEquals(copy, copiedChild.getParentWorkflowTracker(), "The child should know its parent");
  }

  private static WorkflowTracker createTracker() {
    return createTracker(null);
  }

  private static WorkflowTracker createTracker(String actionName) {
    WorkflowMeta workflowMeta = mock(WorkflowMeta.class);
    WorkflowTracker workflowTracker = new WorkflowTracker(workflowMeta);
    if (actionName != null) {
      ActionResult result = mock(ActionResult.class);
      when(result.getActionName()).thenReturn(actionName);
      workflowTracker.setActionResult(result);
    }
    return workflowTracker;
  }

  private static ActionMeta createActionMeta(String actionName) {
    IAction action = mock(IAction.class);
    when(action.getName()).thenReturn(actionName);

    return new ActionMeta(action);
  }
}
