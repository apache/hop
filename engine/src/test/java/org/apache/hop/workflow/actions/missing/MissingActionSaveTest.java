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

package org.apache.hop.workflow.actions.missing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * A workflow referencing an action plugin which isn't installed must survive a load/save round
 * trip: the {@link MissingAction} placeholder keeps the XML of the original action untouched and
 * writes it back out again, so that saving the file doesn't destroy the configuration of that
 * action for anyone who does have the plugin installed.
 *
 * <p>This is the workflow counterpart of {@code MissingTransformSaveTest}.
 */
@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class MissingActionSaveTest {

  private static final String MISSING_PLUGIN_ID = "ThisActionPluginIsNotInstalled";
  private static final String ACTION_NAME = "Action with a missing plugin";

  private static final String WORKFLOW_XML =
      """
      <workflow>
        <name>missing-plugin-workflow</name>
        <actions>
          <action>
            <name>Action with a missing plugin</name>
            <type>ThisActionPluginIsNotInstalled</type>
            <description>Send the nightly report</description>
            <server>my-precious-server</server>
            <port>2525</port>
            <destination>ops@example.org</destination>
            <subject>Nightly report</subject>
            <attachments>
              <attachment>report.pdf</attachment>
            </attachments>
            <parallel>N</parallel>
            <xloc>240</xloc>
            <yloc>128</yloc>
          </action>
        </actions>
      </workflow>
      """;

  private WorkflowMeta loadWorkflow(String xml) throws Exception {
    IVariables variables = new Variables();
    IHopMetadataProvider metadataProvider = new MemoryMetadataProvider();
    return new WorkflowMeta(
        new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)),
        metadataProvider,
        variables);
  }

  /** A workflow referencing an unknown action plugin still loads, using a placeholder. */
  @Test
  void unknownActionPluginLoadsAsMissingPlaceholder() throws Exception {
    WorkflowMeta workflowMeta = loadWorkflow(WORKFLOW_XML);

    assertEquals(1, workflowMeta.nrActions());
    ActionMeta actionMeta = workflowMeta.getAction(0);

    assertEquals(ACTION_NAME, actionMeta.getName());
    assertTrue(actionMeta.isMissing());
    MissingAction missing = assertInstanceOf(MissingAction.class, actionMeta.getAction());

    // The placeholder keeps pointing at the plugin which is missing, so the action can be restored
    // once that plugin is installed.
    assertEquals(MISSING_PLUGIN_ID, missing.getMissingPluginId());
    assertEquals(MISSING_PLUGIN_ID, missing.getPluginId());
  }

  /** The XML of the action we can't de-serialize is kept, element by element. */
  @Test
  void theOriginalXmlOfTheActionIsPreserved() throws Exception {
    WorkflowMeta workflowMeta = loadWorkflow(WORKFLOW_XML);

    MissingAction missing = (MissingAction) workflowMeta.getAction(0).getAction();

    assertEquals(
        List.of(
            "<name>Action with a missing plugin</name>",
            "<type>ThisActionPluginIsNotInstalled</type>",
            "<description>Send the nightly report</description>",
            "<server>my-precious-server</server>",
            "<port>2525</port>",
            "<destination>ops@example.org</destination>",
            "<subject>Nightly report</subject>",
            "<attachments>\n"
                + "        <attachment>report.pdf</attachment>\n"
                + "      </attachments>",
            "<parallel>N</parallel>",
            "<xloc>240</xloc>",
            "<yloc>128</yloc>"),
        missing.getPreservedXml());
  }

  /** Saving the workflow writes the settings of the missing plugin back out. */
  @Test
  void savingAWorkflowWithAMissingActionPluginKeepsItsConfiguration() throws Exception {
    WorkflowMeta workflowMeta = loadWorkflow(WORKFLOW_XML);

    String savedXml = workflowMeta.getXml(new Variables());

    assertTrue(savedXml.contains("<name>" + ACTION_NAME + "</name>"));
    assertTrue(savedXml.contains("<type>" + MISSING_PLUGIN_ID + "</type>"));
    assertTrue(savedXml.contains("<description>Send the nightly report</description>"));
    assertTrue(savedXml.contains("<xloc>240</xloc>"));

    // Everything the plugin itself owned is still there.
    assertTrue(savedXml.contains("<server>my-precious-server</server>"));
    assertTrue(savedXml.contains("<port>2525</port>"));
    assertTrue(savedXml.contains("<destination>ops@example.org</destination>"));
    assertTrue(savedXml.contains("<subject>Nightly report</subject>"));
    assertTrue(savedXml.contains("<attachment>report.pdf</attachment>"));
  }

  /**
   * The elements which the workflow serializes itself are not duplicated: what {@link ActionMeta}
   * writes wins, since that is where renames and moves on the canvas end up.
   */
  @Test
  void theActionOwnedElementsAreNotDuplicated() throws Exception {
    WorkflowMeta workflowMeta = loadWorkflow(WORKFLOW_XML);
    workflowMeta.getAction(0).setName("Renamed while the plugin was missing");
    workflowMeta.getAction(0).setLocation(500, 600);

    String savedXml = actionElement(workflowMeta.getXml(new Variables()));

    assertEquals(1, countOccurrences(savedXml, "<name>"));
    assertEquals(1, countOccurrences(savedXml, "<type>"));
    assertEquals(1, countOccurrences(savedXml, "<xloc>"));
    assertEquals(1, countOccurrences(savedXml, "<parallel>"));

    assertTrue(savedXml.contains("<name>Renamed while the plugin was missing</name>"));
    assertTrue(savedXml.contains("<xloc>500</xloc>"));
    assertTrue(savedXml.contains("<yloc>600</yloc>"));
    // ...and the settings of the missing plugin came along.
    assertTrue(savedXml.contains("<server>my-precious-server</server>"));
  }

  /** Repeated load/save round trips are stable: nothing is lost and nothing is added. */
  @Test
  void theRoundTripIsStable() throws Exception {
    String savedOnce = loadWorkflow(WORKFLOW_XML).getXml(new Variables());
    String savedTwice = loadWorkflow(savedOnce).getXml(new Variables());
    String savedThrice = loadWorkflow(savedTwice).getXml(new Variables());

    assertEquals(stripDates(savedOnce), stripDates(savedTwice));
    assertEquals(stripDates(savedTwice), stripDates(savedThrice));
    assertTrue(savedThrice.contains("<server>my-precious-server</server>"));
  }

  /** Copy/paste of an action with a missing plugin keeps its settings as well. */
  @Test
  void theLegacyActionXmlRoundTripKeepsTheConfiguration() throws Exception {
    ActionMeta actionMeta = loadWorkflow(WORKFLOW_XML).getAction(0);

    String copied = actionMeta.getXml();
    assertTrue(copied.contains("<server>my-precious-server</server>"));
    assertEquals(1, countOccurrences(copied, "<name>"));
    assertEquals(1, countOccurrences(copied, "<type>"));

    ActionMeta pasted =
        new ActionMeta(
            org.apache.hop.core.xml.XmlHandler.getSubNode(
                org.apache.hop.core.xml.XmlHandler.loadXmlString(copied), ActionMeta.XML_TAG),
            new MemoryMetadataProvider(),
            new Variables());

    assertTrue(pasted.isMissing());
    MissingAction missing = (MissingAction) pasted.getAction();
    assertEquals(MISSING_PLUGIN_ID, missing.getMissingPluginId());
    assertEquals(MISSING_PLUGIN_ID, missing.getPluginId());
    assertTrue(pasted.getXml().contains("<server>my-precious-server</server>"));
  }

  /** The workflow reports the missing plugins so the user can be warned about them. */
  @Test
  void theMissingPluginIsReported() throws Exception {
    WorkflowMeta workflowMeta = loadWorkflow(WORKFLOW_XML);

    assertTrue(workflowMeta.hasMissingPlugins());
  }

  /** The {@code <action>} element of a serialized workflow, without the surrounding workflow. */
  private static String actionElement(String workflowXml) {
    return workflowXml.substring(workflowXml.indexOf("<action>"), workflowXml.indexOf("</action>"));
  }

  /** The created/modified dates change on every save, they are not what these tests are about. */
  private static String stripDates(String xml) {
    return xml.replaceAll("<(created|modified)_date>[^<]*</(created|modified)_date>", "");
  }

  private static int countOccurrences(String xml, String tag) {
    int count = 0;
    int index = xml.indexOf(tag);
    while (index >= 0) {
      count++;
      index = xml.indexOf(tag, index + tag.length());
    }
    return count;
  }
}
