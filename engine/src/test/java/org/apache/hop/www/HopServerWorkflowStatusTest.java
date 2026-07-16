/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.www;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopRuntimeException;
import org.apache.hop.core.gui.WorkflowTracker;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.server.HttpUtil;
import org.apache.hop.workflow.ActionResult;
import org.apache.hop.workflow.WorkflowMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.w3c.dom.Node;

@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class HopServerWorkflowStatusTest {

  @BeforeAll
  static void setUpBeforeClass() throws HopException {
    HopEnvironment.init();
  }

  @Test
  void testStaticFinal() {
    assertEquals("workflow-status", HopServerWorkflowStatus.XML_TAG);
  }

  @Test
  void testNoDate() throws HopException {
    String workflowName = "testNullDate";
    String id = UUID.randomUUID().toString();
    String status = Pipeline.STRING_FINISHED;
    HopServerWorkflowStatus js = new HopServerWorkflowStatus(workflowName, id, status);
    String resultXML = js.getXml();
    Node newJobStatus =
        XmlHandler.getSubNode(XmlHandler.loadXmlString(resultXML), HopServerWorkflowStatus.XML_TAG);

    assertEquals(
        resultXML,
        HopServerWorkflowStatus.fromXml(resultXML).getXml(),
        "The XML document should match after rebuilding from XML");
    assertEquals(
        1,
        XmlHandler.countNodes(newJobStatus, "log_date"),
        "There should be one \"log_date\" node in the XML");
    assertFalse(
        Utils.isEmpty(XmlHandler.getTagValue(newJobStatus, "log_date")),
        "The \"log_date\" node should have a null value");
  }

  @Test
  void testWithDate() throws HopException {
    String workflowName = "testWithDate";
    String id = UUID.randomUUID().toString();
    String status = Pipeline.STRING_FINISHED;
    Date logDate = new Date();
    HopServerWorkflowStatus js = new HopServerWorkflowStatus(workflowName, id, status);
    js.setLogDate(logDate);
    String resultXML = js.getXml();
    Node newJobStatus =
        XmlHandler.getSubNode(XmlHandler.loadXmlString(resultXML), HopServerWorkflowStatus.XML_TAG);

    assertEquals(
        resultXML,
        HopServerWorkflowStatus.fromXml(resultXML).getXml(),
        "The XML document should match after rebuilding from XML");
    assertEquals(
        1,
        XmlHandler.countNodes(newJobStatus, "log_date"),
        "There should be one \"log_date\" node in the XML");
    assertEquals(
        XmlHandler.date2string(logDate),
        XmlHandler.getTagValue(newJobStatus, "log_date"),
        "The \"log_date\" node should match the original value");
  }

  /**
   * The tracker is what a client needs to show the metrics of a workflow running on a server, so it
   * has to travel along with the status. See issue #3685.
   */
  @Test
  void trackerSurvivesXmlRoundTrip() throws HopException {
    HopServerWorkflowStatus status =
        new HopServerWorkflowStatus(
            "tracked", UUID.randomUUID().toString(), Pipeline.STRING_RUNNING);

    WorkflowTracker<WorkflowMeta> tracker = new WorkflowTracker<>((WorkflowMeta) null);
    tracker.setWorkflowName("tracked");
    ActionResult actionResult = new ActionResult();
    actionResult.setActionName("an action");
    actionResult.setComment("Start of action");
    tracker.addWorkflowTracker(new WorkflowTracker<>((WorkflowMeta) null, actionResult));
    status.setWorkflowTracker(tracker);

    String xml = status.getXml();
    HopServerWorkflowStatus copy = HopServerWorkflowStatus.fromXml(xml);

    assertEquals(xml, copy.getXml(), "The XML document should match after rebuilding from XML");
    assertEquals("tracked", copy.getWorkflowTracker().getWorkflowName());
    assertEquals(1, copy.getWorkflowTracker().nrWorkflowTrackers());
    assertEquals(
        "an action",
        copy.getWorkflowTracker().getWorkflowTracker(0).getActionResult().getActionName());
  }

  /** A status without a tracker, as sent by an older server, rebuilds without one. */
  @Test
  void statusWithoutTrackerRoundTrips() throws HopException {
    HopServerWorkflowStatus status =
        new HopServerWorkflowStatus("plain", UUID.randomUUID().toString(), Pipeline.STRING_RUNNING);

    HopServerWorkflowStatus copy = HopServerWorkflowStatus.fromXml(status.getXml());

    assertNull(copy.getWorkflowTracker());
  }

  /**
   * The status is also served as JSON. The tracker refers back to its parent and holds a lock, so
   * it is left out there rather than breaking the whole document.
   */
  @Test
  void trackerIsLeftOutOfJson() throws Exception {
    HopServerWorkflowStatus status =
        new HopServerWorkflowStatus(
            "tracked", UUID.randomUUID().toString(), Pipeline.STRING_RUNNING);

    WorkflowTracker<WorkflowMeta> tracker = new WorkflowTracker<>((WorkflowMeta) null);
    tracker.setWorkflowName("tracked");
    WorkflowTracker<WorkflowMeta> child = new WorkflowTracker<>((WorkflowMeta) null);
    child.setParentWorkflowTracker(tracker);
    tracker.addWorkflowTracker(child);
    status.setWorkflowTracker(tracker);

    String json = HopJson.newMapper().writerWithDefaultPrettyPrinter().writeValueAsString(status);

    assertFalse(json.contains("workflowTracker"), "The tracker should not be part of the JSON");
    assertEquals(
        "tracked",
        HopJson.newMapper().readValue(json, HopServerWorkflowStatus.class).getWorkflowName());
  }

  @Test
  void testSerialization() throws HopException {
    //  Add Result
    List<String> attributes =
        Arrays.asList(
            "WorkflowName",
            "Id",
            "StatusDescription",
            "ErrorDescription",
            "LogDate",
            "LoggingString",
            "FirstLoggingLineNr",
            "LastLoggingLineNr");

    Map<String, IFieldLoadSaveValidator<?>> attributeMap = new HashMap<>();
    attributeMap.put("LoggingString", new LoggingStringLoadSaveValidator());

    HopServerWorkflowStatusLoadSaveTester tester =
        new HopServerWorkflowStatusLoadSaveTester(
            HopServerWorkflowStatus.class, attributes, attributeMap);

    tester.testSerialization();
  }

  public static class LoggingStringLoadSaveValidator implements IFieldLoadSaveValidator<String> {

    @Override
    public String getTestObject() {
      try {
        return HttpUtil.encodeBase64ZippedString(UUID.randomUUID().toString());
      } catch (IOException e) {
        throw new HopRuntimeException(e);
      }
    }

    @Override
    public boolean validateTestObject(String testObject, Object actual) {
      try {
        return HttpUtil.decodeBase64ZippedString(testObject).equals(actual);
      } catch (IOException e) {
        throw new HopRuntimeException(e);
      }
    }
  }
}
