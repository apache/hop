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

package org.apache.hop.workflow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.listeners.IContentChangedListener;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.plugins.ActionPluginType;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.actions.ActionFake;
import org.apache.hop.workflow.actions.dummy.ActionDummy;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

class WorkflowMetaTest {
  private static final String WORKFLOW_META_NAME = "workflowName";

  private WorkflowMeta workflowMeta;
  private IVariables variables;
  private IContentChangedListener listener;

  @BeforeEach
  void setUp() throws HopPluginException {
    workflowMeta = new WorkflowMeta();
    workflowMeta.setNameSynchronizedWithFilename(false);
    // prepare
    listener = mock(IContentChangedListener.class);
    workflowMeta.addContentChangedListener(listener);
    workflowMeta.setName(WORKFLOW_META_NAME);

    variables = new Variables();

    PluginRegistry registry = PluginRegistry.getInstance();
    registry.registerPluginClass(ActionFake.class.getName(), ActionPluginType.class, Action.class);
  }

  @Test
  void testPathExist() {
    assertTrue(testPath("je1-je4"));
  }

  @Test
  void testPathNotExist() {
    assertFalse(testPath("je2-je4"));
  }

  private boolean testPath(String branch) {
    ActionDummy je1 = new ActionDummy();
    je1.setName("je1");

    ActionDummy je2 = new ActionDummy();
    je2.setName("je2");

    WorkflowHopMeta hop = new WorkflowHopMeta(new ActionMeta(je1), new ActionMeta(je2));
    workflowMeta.addWorkflowHop(hop);

    ActionDummy je3 = new ActionDummy();
    je3.setName("je3");
    hop = new WorkflowHopMeta(new ActionMeta(je1), new ActionMeta(je3));
    workflowMeta.addWorkflowHop(hop);

    ActionDummy je4 = new ActionDummy();
    je4.setName("je4");
    hop = new WorkflowHopMeta(new ActionMeta(je3), new ActionMeta(je4));
    workflowMeta.addWorkflowHop(hop);

    if (branch.equals("je1-je4")) {
      return workflowMeta.isPathExist(je1, je4);
    } else if (branch.equals("je2-je4")) {
      return workflowMeta.isPathExist(je2, je4);
    } else {
      return false;
    }
  }

  @Test
  void testContentChangeListener() {
    workflowMeta.setChanged();
    workflowMeta.setChanged(true);

    verify(listener, times(2)).contentChanged(same(workflowMeta));

    workflowMeta.clearChanged();
    workflowMeta.setChanged(false);

    verify(listener, times(2)).contentSafe(same(workflowMeta));

    workflowMeta.removeContentChangedListener(listener);
    workflowMeta.setChanged();
    workflowMeta.setChanged(true);

    verifyNoMoreInteractions(listener);
  }

  @Test
  void testEquals_oneNameNull() {
    assertFalse(testEquals(null, null));
  }

  @Test
  void testEquals_secondNameNull() {
    workflowMeta.setName(null);
    assertFalse(testEquals(WORKFLOW_META_NAME, null));
  }

  @Test
  void testEquals_sameFilename() {
    String newFilename = "Filename";
    workflowMeta.setFilename(newFilename);
    assertFalse(testEquals(null, newFilename));
  }

  @Test
  void testEquals_difFilenameSameName() {
    workflowMeta.setFilename("Filename");
    assertFalse(testEquals(WORKFLOW_META_NAME, "OtherFileName"));
  }

  @Test
  void testEquals_sameFilenameSameName() {
    String newFilename = "Filename";
    workflowMeta.setFilename(newFilename);
    assertTrue(testEquals(WORKFLOW_META_NAME, newFilename));
  }

  @Test
  void testEquals_sameFilenameDifName() {
    String newFilename = "Filename";
    workflowMeta.setFilename(newFilename);
    assertFalse(testEquals("OtherName", newFilename));
  }

  private boolean testEquals(String name, String filename) {
    WorkflowMeta workflowMeta2 = new WorkflowMeta();
    workflowMeta2.setNameSynchronizedWithFilename(false);
    workflowMeta2.setName(name);
    workflowMeta2.setFilename(filename);
    return workflowMeta.equals(workflowMeta2);
  }

  @Test
  void testLoadXml() throws HopException {
    String directory = "/home/admin";
    Node workflowNode = Mockito.mock(Node.class);
    NodeList nodeList =
        new NodeList() {
          final Node node = Mockito.mock(Node.class);

          {
            Mockito.when(node.getNodeName()).thenReturn("directory");
            Node child = Mockito.mock(Node.class);
            Mockito.when(node.getFirstChild()).thenReturn(child);
            Mockito.when(child.getNodeValue()).thenReturn(directory);
          }

          @Override
          public Node item(int index) {
            return node;
          }

          @Override
          public int getLength() {
            return 1;
          }
        };

    Mockito.when(workflowNode.getChildNodes()).thenReturn(nodeList);

    WorkflowMeta meta = new WorkflowMeta();

    meta.loadXml(workflowNode, null, Mockito.mock(IHopMetadataProvider.class), new Variables());
    IWorkflowEngine<WorkflowMeta> workflow = new LocalWorkflowEngine(meta);
    assertNotNull(workflow);
    workflow.setInternalHopVariables();
  }

  @Test
  void testAddRemoveJobEntryCopySetUnsetParent() {
    ActionMeta actionCopy = mock(ActionMeta.class);
    workflowMeta.addAction(actionCopy);
    workflowMeta.removeAction(0);
    verify(actionCopy, times(1)).setParentWorkflowMeta(workflowMeta);
    verify(actionCopy, times(1)).setParentWorkflowMeta(null);
  }

  @Test
  void testHasLoop_simpleLoop() {
    // main->2->3->main
    WorkflowMeta workflowMetaSpy = spy(workflowMeta);
    ActionMeta actionCopyMain = createAction("mainTransform");
    ActionMeta actionCopy2 = createAction("transform2");
    ActionMeta actionCopy3 = createAction("transform3");
    when(workflowMetaSpy.findNrPrevActions(actionCopyMain)).thenReturn(1);
    when(workflowMetaSpy.findPrevAction(actionCopyMain, 0)).thenReturn(actionCopy2);
    when(workflowMetaSpy.findNrPrevActions(actionCopy2)).thenReturn(1);
    when(workflowMetaSpy.findPrevAction(actionCopy2, 0)).thenReturn(actionCopy3);
    when(workflowMetaSpy.findNrPrevActions(actionCopy3)).thenReturn(1);
    when(workflowMetaSpy.findPrevAction(actionCopy3, 0)).thenReturn(actionCopyMain);
    assertTrue(workflowMetaSpy.hasLoop(actionCopyMain));
  }

  @Test
  void testHasLoop_loopInPrevTransforms() {
    // main->2->3->4->3
    WorkflowMeta workflowMetaSpy = spy(workflowMeta);
    ActionMeta actionCopyMain = createAction("mainTransform");
    ActionMeta actionCopy2 = createAction("transform2");
    ActionMeta actionCopy3 = createAction("transform3");
    ActionMeta actionCopy4 = createAction("transform4");
    when(workflowMetaSpy.findNrPrevActions(actionCopyMain)).thenReturn(1);
    when(workflowMetaSpy.findPrevAction(actionCopyMain, 0)).thenReturn(actionCopy2);
    when(workflowMetaSpy.findNrPrevActions(actionCopy2)).thenReturn(1);
    when(workflowMetaSpy.findPrevAction(actionCopy2, 0)).thenReturn(actionCopy3);
    when(workflowMetaSpy.findNrPrevActions(actionCopy3)).thenReturn(1);
    when(workflowMetaSpy.findPrevAction(actionCopy3, 0)).thenReturn(actionCopy4);
    when(workflowMetaSpy.findNrPrevActions(actionCopy4)).thenReturn(1);
    when(workflowMetaSpy.findPrevAction(actionCopy4, 0)).thenReturn(actionCopy3);
    // check no StackOverflow error
    assertFalse(workflowMetaSpy.hasLoop(actionCopyMain));
  }

  private ActionMeta createAction(String name) {
    IAction action = mock(IAction.class);
    ActionMeta actionMeta = new ActionMeta(action);
    when(actionMeta.getName()).thenReturn(name);
    return actionMeta;
  }

  @Test
  void testSetInternalHopVariablesWithFilename() {
    // Issue #2430: Internal.Workflow.Filename.Folder / .Name / Internal.Workflow.Name
    // must be populated when a filename is set (design-time and runtime API path).
    WorkflowMeta meta = new WorkflowMeta();
    meta.setNameSynchronizedWithFilename(true);
    meta.setFilename("/tmp/parameters_and_variables/0007-internal-variables.hwf");

    IVariables vars = new Variables();
    meta.setInternalHopVariables(vars);

    assertEquals(
        "0007-internal-variables.hwf",
        vars.getVariable(Const.INTERNAL_VARIABLE_WORKFLOW_FILENAME_NAME));
    assertEquals(
        "0007-internal-variables", vars.getVariable(Const.INTERNAL_VARIABLE_WORKFLOW_NAME));
    assertNotNull(vars.getVariable(Const.INTERNAL_VARIABLE_WORKFLOW_FILENAME_FOLDER));
    assertFalse(vars.getVariable(Const.INTERNAL_VARIABLE_WORKFLOW_FILENAME_FOLDER).isEmpty());
    assertTrue(
        vars.getVariable(Const.INTERNAL_VARIABLE_WORKFLOW_FILENAME_FOLDER)
            .contains("parameters_and_variables"));
    assertEquals(
        vars.getVariable(Const.INTERNAL_VARIABLE_WORKFLOW_FILENAME_FOLDER),
        vars.getVariable(Const.INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER));
  }

  @Test
  void testSetInternalHopVariablesWithoutFilename() {
    WorkflowMeta meta = new WorkflowMeta();
    meta.setNameSynchronizedWithFilename(false);
    meta.setName("unsaved-workflow");
    meta.setFilename(null);

    IVariables vars = new Variables();
    vars.setVariable(Const.INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER, "parent-folder");
    meta.setInternalHopVariables(vars);

    assertEquals("", vars.getVariable(Const.INTERNAL_VARIABLE_WORKFLOW_FILENAME_NAME));
    assertEquals("", vars.getVariable(Const.INTERNAL_VARIABLE_WORKFLOW_FILENAME_FOLDER));
    assertEquals("unsaved-workflow", vars.getVariable(Const.INTERNAL_VARIABLE_WORKFLOW_NAME));
    assertEquals("parent-folder", vars.getVariable(Const.INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER));
  }

  @Test
  void testSetInternalEntryCurrentDirectoryWithFilename() {
    WorkflowMeta workflowMetaTest = new WorkflowMeta();
    workflowMetaTest.setFilename("hasFilename");
    variables.setVariable(
        Const.INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER, "Original value defined at run execution");
    variables.setVariable(
        Const.INTERNAL_VARIABLE_WORKFLOW_FILENAME_FOLDER, "file:///C:/SomeFilenameDirectory");
    workflowMetaTest.setInternalEntryCurrentDirectory(variables);

    assertEquals(
        "file:///C:/SomeFilenameDirectory",
        variables.getVariable(Const.INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER));
  }

  @Test
  void testSetInternalEntryCurrentDirectoryWithoutFilename() {
    WorkflowMeta workflowMetaTest = new WorkflowMeta();
    variables.setVariable(
        Const.INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER, "Original value defined at run execution");
    variables.setVariable(
        Const.INTERNAL_VARIABLE_WORKFLOW_FILENAME_FOLDER, "file:///C:/SomeFilenameDirectory");
    workflowMetaTest.setInternalEntryCurrentDirectory(variables);

    assertEquals(
        "Original value defined at run execution",
        variables.getVariable(Const.INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER));
  }

  @Test
  void testUpdateCurrentDirWithFilename() {
    WorkflowMeta workflowMetaTest = new WorkflowMeta();
    workflowMetaTest.setFilename("hasFilename");
    variables.setVariable(
        Const.INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER, "Original value defined at run execution");
    variables.setVariable(
        Const.INTERNAL_VARIABLE_WORKFLOW_FILENAME_FOLDER, "file:///C:/SomeFilenameDirectory");
    workflowMetaTest.updateCurrentDir(variables);

    assertEquals(
        "file:///C:/SomeFilenameDirectory",
        variables.getVariable(Const.INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER));
  }

  @Test
  void testUpdateCurrentDirWithoutFilename() {
    WorkflowMeta workflowMetaTest = new WorkflowMeta();
    variables.setVariable(
        Const.INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER, "Original value defined at run execution");
    variables.setVariable(
        Const.INTERNAL_VARIABLE_WORKFLOW_FILENAME_FOLDER, "file:///C:/SomeFilenameDirectory");
    workflowMetaTest.updateCurrentDir(variables);

    assertEquals(
        "Original value defined at run execution",
        variables.getVariable(Const.INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER));
  }

  @Test
  void testXmlSerialization() throws Exception {
    ActionFake fake1 = new ActionFake();
    fake1.setSomeProperty("prop1");
    ActionMeta a1 = new ActionMeta(fake1);
    a1.setName("a1");
    workflowMeta.addAction(a1);

    ActionFake fake2 = new ActionFake();
    fake2.setSomeProperty("prop2");
    ActionMeta a2 = new ActionMeta(fake2);
    a2.setName("a2");
    workflowMeta.addAction(a2);

    ActionFake fake3 = new ActionFake();
    fake3.setSomeProperty("prop3");
    ActionMeta a3 = new ActionMeta(fake3);
    a3.setName("a3");
    workflowMeta.addAction(a3);

    WorkflowHopMeta a12 = new WorkflowHopMeta(a1, a2);
    a12.setEvaluation(true);
    a12.setUnconditional(false);
    workflowMeta.addWorkflowHop(a12);
    WorkflowHopMeta a23 = new WorkflowHopMeta(a2, a3);
    a23.setEvaluation(false);
    a23.setUnconditional(true);
    workflowMeta.addWorkflowHop(a23);

    // Serialize to XML
    String xml = workflowMeta.getXml(new Variables());

    // Re-inflate it
    //
    Node node = XmlHandler.loadXmlString(xml, WorkflowMeta.XML_TAG);
    WorkflowMeta copy = new WorkflowMeta(node, new MemoryMetadataProvider(), new Variables());

    assertEquals(3, copy.nrActions());

    ActionMeta copy1 = copy.getAction(0);
    assertEquals("a1", copy1.getName());
    assertNotNull(copy1.getAction());
    if (copy1.getAction() instanceof ActionFake action) {
      assertEquals("prop1", action.getSomeProperty());
    }

    ActionMeta copy2 = copy.getAction(1);
    assertEquals("a2", copy2.getName());
    assertNotNull(copy2.getAction());
    if (copy2.getAction() instanceof ActionFake action) {
      assertEquals("prop2", action.getSomeProperty());
    }

    ActionMeta copy3 = copy.getAction(2);
    assertEquals("a3", copy3.getName());
    assertNotNull(copy3.getAction());
    if (copy3.getAction() instanceof ActionFake action) {
      assertEquals("prop3", action.getSomeProperty());
    }

    assertEquals(2, copy.nrWorkflowHops());
    WorkflowHopMeta copy12 = copy.getWorkflowHop(0);
    assertTrue(copy12.isEvaluation());
    assertFalse(copy12.isUnconditional());
    assertEquals(copy1, copy12.getFrom());
    assertEquals(copy2, copy12.getTo());

    WorkflowHopMeta copy23 = copy.getWorkflowHop(1);
    assertFalse(copy23.isEvaluation());
    assertTrue(copy23.isUnconditional());
    assertEquals(copy2, copy23.getFrom());
    assertEquals(copy3, copy23.getTo());
  }

  /**
   * A hop naming an action that the file does not contain - the state {@code
   * main-0012-fuzzymatch.hwf} was in before <a href="https://github.com/apache/hop/pull/7989">
   * #7989</a> - resolves to null, so it is dropped rather than kept as half a hop that is neither
   * drawn, executed, nor saved in one piece.
   */
  @Test
  void loadingDropsAHopThatNamesAnActionNotInTheFile() throws Exception {
    String xml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<workflow>\n"
            + "  <name>fuzzymatch</name>\n"
            + "  <actions>\n"
            + "    <action><name>Start</name><type>ActionFake</type></action>\n"
            + "    <action><name>Run Fuzzy match tests</name><type>ActionFake</type></action>\n"
            + "  </actions>\n"
            + "  <hops>\n"
            + "    <hop><from>Start</from><to>Run Group By tests</to><enabled>Y</enabled></hop>\n"
            + "  </hops>\n"
            + "</workflow>";

    // Dropping the hop is logged, and logging needs a log store to write to.
    HopLogStore.init();

    WorkflowMeta loaded = new WorkflowMeta();
    loaded.loadXml(
        XmlHandler.loadXmlString(xml, WorkflowMeta.XML_TAG),
        null,
        new MemoryMetadataProvider(),
        variables);

    assertEquals(2, loaded.nrActions());
    assertEquals(0, loaded.nrWorkflowHops(), "the hop points at an action that is not in the file");
    assertFalse(
        loaded.getXml(variables).contains("Run Group By tests"),
        "saving must not write the unresolvable reference back");
  }

  /** Deleting an action takes the hops attached to it along, whoever does the deleting. */
  @Test
  void removingAnActionDropsTheHopsAttachedToIt() {
    ActionMeta first = new ActionMeta(new ActionDummy());
    first.setName("first");
    ActionMeta second = new ActionMeta(new ActionDummy());
    second.setName("second");
    ActionMeta third = new ActionMeta(new ActionDummy());
    third.setName("third");
    workflowMeta.addAction(first);
    workflowMeta.addAction(second);
    workflowMeta.addAction(third);
    workflowMeta.addWorkflowHop(new WorkflowHopMeta(first, second));
    workflowMeta.addWorkflowHop(new WorkflowHopMeta(second, third));

    workflowMeta.removeAction(workflowMeta.indexOfAction(second));

    assertEquals(0, workflowMeta.nrWorkflowHops(), "both hops were attached to the deleted action");
  }

  /**
   * A disabled hop is a hop the user switched off, not a broken one: dropping unresolvable
   * references may not touch it.
   */
  @Test
  void loadingKeepsDisabledHops() throws Exception {
    String xml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<workflow>\n"
            + "  <name>disabled</name>\n"
            + "  <actions>\n"
            + "    <action><name>Start</name><type>ActionFake</type></action>\n"
            + "    <action><name>Second</name><type>ActionFake</type></action>\n"
            + "  </actions>\n"
            + "  <hops>\n"
            + "    <hop><from>Start</from><to>Second</to><enabled>N</enabled></hop>\n"
            + "  </hops>\n"
            + "</workflow>";

    WorkflowMeta loaded = new WorkflowMeta();
    loaded.loadXml(
        XmlHandler.loadXmlString(xml, WorkflowMeta.XML_TAG),
        null,
        new MemoryMetadataProvider(),
        variables);

    assertEquals(1, loaded.nrWorkflowHops(), "the disabled hop must survive the load");
    assertFalse(loaded.getWorkflowHop(0).isEnabled());
    assertTrue(loaded.getXml(variables).contains("<enabled>N</enabled>"));
  }
}
