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

package org.apache.hop.workflow.actions.setvariables;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.core.xml.XmlParserFactoryProducer;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.action.ActionSerializationTestUtil;
import org.apache.hop.workflow.actions.setvariables.ActionSetVariables.VariableDefinition;
import org.apache.hop.workflow.actions.setvariables.ActionSetVariables.VariableType;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

class ActionSetVariablesTest {
  private IWorkflowEngine<WorkflowMeta> workflow;
  private ActionSetVariables action;

  @BeforeAll
  static void setUpBeforeClass() {
    HopLogStore.init();
  }

  @BeforeEach
  void setUp() {
    workflow = new LocalWorkflowEngine(new WorkflowMeta());
    action = new ActionSetVariables();
    workflow.getWorkflowMeta().addAction(new ActionMeta(action));
    action.setParentWorkflow(workflow);
    workflow.setStopped(false);
  }

  @Test
  void testSerialization() throws Exception {
    HopClientEnvironment.init();
    MemoryMetadataProvider provider = new MemoryMetadataProvider();

    ActionSetVariables action =
        ActionSerializationTestUtil.testSerialization(
            "/set-variables-action.xml", ActionSetVariables.class, provider);

    // Properties file variables
    assertEquals(VariableType.CURRENT_WORKFLOW, action.getFileVariableType());
    assertEquals("filename.txt", action.getFilename());

    // Settings
    assertTrue(action.isReplaceVars());

    // Static variables
    VariableDefinition definition = action.getVariableDefinitions().get(0);
    assertEquals("VAR_CURRENT", definition.getName());
    assertEquals("current", definition.getValue());
    assertEquals(VariableType.CURRENT_WORKFLOW, definition.getType());
    definition = action.getVariableDefinitions().get(1);
    assertEquals("VAR_PARENT", definition.getName());
    assertEquals("parent", definition.getValue());
    assertEquals(VariableType.PARENT_WORKFLOW, definition.getType());
    definition = action.getVariableDefinitions().get(2);
    assertEquals("VAR_ROOT", definition.getName());
    assertEquals("root", definition.getValue());
    assertEquals(VariableType.ROOT_WORKFLOW, definition.getType());
    definition = action.getVariableDefinitions().get(3);
    assertEquals("VAR_JVM", definition.getName());
    assertEquals("jvm", definition.getValue());
    assertEquals(VariableType.JVM, definition.getType());
  }

  @Test
  void testASCIIText() throws Exception {
    // properties file with native2ascii
    action.setFilename(
        "src/test/resources/org/apache/hop/workflow/actions/setvariables/ASCIIText.properties");
    action.setReplaceVars(true);
    Result result = action.execute(new Result(), 0);
    assertTrue(result.isResult(), "Result should be true");
    assertEquals("日本語", action.getVariable("Japanese"));
    assertEquals("English", action.getVariable("English"));
    assertEquals("中文", action.getVariable("Chinese"));
  }

  @Test
  void testUTF8Text() throws Exception {
    // properties files without native2ascii
    action.setFilename(
        "src/test/resources/org/apache/hop/workflow/actions/setvariables/UTF8Text.properties");
    action.setReplaceVars(true);
    Result result = action.execute(new Result(), 0);
    assertTrue(result.isResult(), "Result should be true");
    assertEquals("日本語", action.getVariable("Japanese"));
    assertEquals("English", action.getVariable("English"));
    assertEquals("中文", action.getVariable("Chinese"));
  }

  @Test
  void testInputStreamClosed() throws Exception {
    // properties files without native2ascii
    String propertiesFilename =
        "src/test/resources/org/apache/hop/workflow/actions/setvariables/UTF8Text.properties";
    action.setFilename(propertiesFilename);
    action.setReplaceVars(true);
    Result result = action.execute(new Result(), 0);
    assertTrue(result.isResult(), "Result should be true");
    RandomAccessFile fos = null;
    try {
      File file = new File(propertiesFilename);
      if (file.exists()) {
        fos = new RandomAccessFile(file, "rw");
      }
    } catch (FileNotFoundException | SecurityException e) {
      fail("the file with properties should be unallocated");
    } finally {
      if (fos != null) {
        fos.close();
      }
    }
  }

  @Test
  void testParentJobVariablesExecutingFilePropertiesThatChangesVariablesAndParameters()
      throws Exception {
    action.setReplaceVars(true);
    action.setFileVariableType(VariableType.CURRENT_WORKFLOW);

    IWorkflowEngine<WorkflowMeta> parentWorkflow = action.getParentWorkflow();

    parentWorkflow.addParameterDefinition("parentParam", "", "");
    parentWorkflow.setParameterValue("parentParam", "parentValue");
    parentWorkflow.setVariable("parentParam", "parentValue");

    action.setFilename(
        "src/test/resources/org/apache/hop/workflow/actions/setvariables/configurationA.properties");
    Result result = action.execute(new Result(), 0);
    assertTrue(result.isResult(), "Result should be true");
    assertEquals("a", parentWorkflow.getVariable("propertyFile"));
    assertEquals("a", parentWorkflow.getVariable("dynamicProperty"));
    assertEquals("parentValue", parentWorkflow.getVariable("parentParam"));

    action.setFilename(
        "src/test/resources/org/apache/hop/workflow/actions/setvariables/configurationB.properties");
    result = action.execute(new Result(), 0);
    assertTrue(result.isResult(), "Result should be true");
    assertEquals("b", parentWorkflow.getVariable("propertyFile"));
    assertEquals("new", parentWorkflow.getVariable("newProperty"));
    assertEquals("haha", parentWorkflow.getVariable("parentParam"));
    assertEquals("static", parentWorkflow.getVariable("staticProperty"));
    assertEquals("", parentWorkflow.getVariable("dynamicProperty"));

    action.setFilename(
        "src/test/resources/org/apache/hop/workflow/actions/setvariables/configurationA.properties");
    result = action.execute(new Result(), 0);
    assertTrue(result.isResult(), "Result should be true");
    assertEquals("a", parentWorkflow.getVariable("propertyFile"));
    assertEquals("", parentWorkflow.getVariable("newProperty"));
    assertEquals("parentValue", parentWorkflow.getVariable("parentParam"));
    assertEquals("", parentWorkflow.getVariable("staticProperty"));
    assertEquals("a", parentWorkflow.getVariable("dynamicProperty"));

    action.setFilename(
        "src/test/resources/org/apache/hop/workflow/actions/setvariables/configurationB.properties");
    result = action.execute(new Result(), 0);
    assertTrue(result.isResult(), "Result should be true");
    assertEquals("b", parentWorkflow.getVariable("propertyFile"));
    assertEquals("new", parentWorkflow.getVariable("newProperty"));
    assertEquals("haha", parentWorkflow.getVariable("parentParam"));
    assertEquals("static", parentWorkflow.getVariable("staticProperty"));
    assertEquals("", parentWorkflow.getVariable("dynamicProperty"));
  }

  @Test
  void testJobEntrySetVariablesExecute_VARIABLE_TYPE_JVM_NullVariable() throws Exception {
    IHopMetadataProvider metadataProvider = mock(IHopMetadataProvider.class);
    action.loadXml(getEntryNode("nullVariable", null, "JVM"), metadataProvider, new Variables());
    Result result = action.execute(new Result(), 0);
    assertTrue(result.isResult(), "Result should be true");
    assertNull(System.getProperty("nullVariable"));
  }

  @Test
  void testSetVariablesExecute_VARIABLE_TYPE_CURRENT_WORKFLOW_NullVariable() throws Exception {
    IHopMetadataProvider metadataProvider = mock(IHopMetadataProvider.class);
    action.loadXml(
        getEntryNode("nullVariable", null, "CURRENT_WORKFLOW"), metadataProvider, new Variables());
    Result result = action.execute(new Result(), 0);
    assertTrue(result.isResult(), "Result should be true");
    assertNull(action.getVariable("nullVariable"));
  }

  @Test
  void testSetVariablesExecute_VARIABLE_TYPE_JVM_VariableNotNull() throws Exception {
    IHopMetadataProvider metadataProvider = mock(IHopMetadataProvider.class);
    action.loadXml(
        getEntryNode("variableNotNull", "someValue", "JVM"), metadataProvider, new Variables());
    assertNull(System.getProperty("variableNotNull"));
    Result result = action.execute(new Result(), 0);
    assertTrue(result.isResult(), "Result should be true");
    assertEquals("someValue", System.getProperty("variableNotNull"));
  }

  @Test
  void testSetVariablesExecute_VARIABLE_TYPE_CURRENT_WORKFLOW_VariableNotNull() throws Exception {
    IHopMetadataProvider metadataProvider = mock(IHopMetadataProvider.class);
    action.loadXml(
        getEntryNode("variableNotNull", "someValue", "CURRENT_WORKFLOW"),
        metadataProvider,
        new Variables());
    assertNull(System.getProperty("variableNotNull"));
    Result result = action.execute(new Result(), 0);
    assertTrue(result.isResult(), "Result should be true");
    assertEquals("someValue", action.getVariable("variableNotNull"));
  }

  // prepare xml for use
  public Node getEntryNode(String variableName, String variableValue, String variableType)
      throws ParserConfigurationException, SAXException, IOException {
    StringBuilder sb = new StringBuilder();
    sb.append(XmlHandler.openTag("workflow"));
    sb.append("      ").append(XmlHandler.openTag("fields"));
    sb.append("      ").append(XmlHandler.openTag("field"));
    sb.append("      ").append(XmlHandler.addTagValue("variable_name", variableName));
    if (variableValue != null) {
      sb.append("      ").append(XmlHandler.addTagValue("variable_value", variableValue));
    }
    if (variableType != null) {
      sb.append("          ").append(XmlHandler.addTagValue("variable_type", variableType));
    }
    sb.append("      ").append(XmlHandler.closeTag("field"));
    sb.append("      ").append(XmlHandler.closeTag("fields"));
    sb.append(XmlHandler.closeTag("workflow"));

    InputStream stream = new ByteArrayInputStream(sb.toString().getBytes(StandardCharsets.UTF_8));
    DocumentBuilder db;
    Document doc;
    db = XmlParserFactoryProducer.createSecureDocBuilderFactory().newDocumentBuilder();
    doc = db.parse(stream);
    return doc.getFirstChild();
  }
}
