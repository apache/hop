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

import org.apache.hop.core.Result;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class WorkflowEntrySetVariablesTest {
  private IWorkflowEngine<WorkflowMeta> workflow;
  private ActionSetVariables action;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    HopLogStore.init();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  }

  @Before
  public void setUp() throws Exception {
    workflow = new LocalWorkflowEngine( new WorkflowMeta() );
    action = new ActionSetVariables();
    workflow.getWorkflowMeta().addAction( new ActionMeta( action ) );
    action.setParentWorkflow( workflow );
    workflow.setStopped( false );
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testASCIIText() throws Exception {
    // properties file with native2ascii
    action.setFilename( "src/test/resources/org/apache/hop/workflow/actions/setvariables/ASCIIText.properties" );
    action.setReplaceVars( true );
    Result result = action.execute( new Result(), 0 );
    assertTrue( "Result should be true", result.getResult() );
    assertEquals( "日本語", action.getVariable( "Japanese" ) );
    assertEquals( "English", action.getVariable( "English" ) );
    assertEquals( "中文", action.getVariable( "Chinese" ) );
  }

  @Test
  public void testUTF8Text() throws Exception {
    // properties files without native2ascii
    action.setFilename( "src/test/resources/org/apache/hop/workflow/actions/setvariables/UTF8Text.properties" );
    action.setReplaceVars( true );
    Result result = action.execute( new Result(), 0 );
    assertTrue( "Result should be true", result.getResult() );
    assertEquals( "日本語", action.getVariable( "Japanese" ) );
    assertEquals( "English", action.getVariable( "English" ) );
    assertEquals( "中文", action.getVariable( "Chinese" ) );
  }

  @Test
  public void testInputStreamClosed() throws Exception {
    // properties files without native2ascii
    String propertiesFilename = "src/test/resources/org/apache/hop/workflow/actions/setvariables/UTF8Text.properties";
    action.setFilename( propertiesFilename );
    action.setReplaceVars( true );
    Result result = action.execute( new Result(), 0 );
    assertTrue( "Result should be true", result.getResult() );
    RandomAccessFile fos = null;
    try {
      File file = new File( propertiesFilename );
      if ( file.exists() ) {
        fos = new RandomAccessFile( file, "rw" );
      }
    } catch ( FileNotFoundException | SecurityException e ) {
      fail( "the file with properties should be unallocated" );
    } finally {
      if ( fos != null ) {
        fos.close();
      }
    }
  }

  @Test
  public void testParentJobVariablesExecutingFilePropertiesThatChangesVariablesAndParameters() throws Exception {
    action.setReplaceVars( true );
    action.setFileVariableType( 1 );

    IWorkflowEngine<WorkflowMeta> parentWorkflow = action.getParentWorkflow();

    parentWorkflow.addParameterDefinition( "parentParam", "", "" );
    parentWorkflow.setParameterValue( "parentParam", "parentValue" );
    parentWorkflow.setVariable( "parentParam", "parentValue" );

    action.setFilename( "src/test/resources/org/apache/hop/workflow/actions/setvariables/configurationA.properties" );
    Result result = action.execute( new Result(), 0 );
    assertTrue( "Result should be true", result.getResult() );
    assertEquals( "a", parentWorkflow.getVariable( "propertyFile" ) );
    assertEquals( "a", parentWorkflow.getVariable( "dynamicProperty" ) );
    assertEquals( "parentValue", parentWorkflow.getVariable( "parentParam" ) );


    action.setFilename( "src/test/resources/org/apache/hop/workflow/actions/setvariables/configurationB.properties" );
    result = action.execute( new Result(), 0 );
    assertTrue( "Result should be true", result.getResult() );
    assertEquals( "b", parentWorkflow.getVariable( "propertyFile" ) );
    assertEquals( "new", parentWorkflow.getVariable( "newProperty" ) );
    assertEquals( "haha", parentWorkflow.getVariable( "parentParam" ) );
    assertEquals( "static", parentWorkflow.getVariable( "staticProperty" ) );
    assertEquals( "", parentWorkflow.getVariable( "dynamicProperty" ) );

    action.setFilename( "src/test/resources/org/apache/hop/workflow/actions/setvariables/configurationA.properties" );
    result = action.execute( new Result(), 0 );
    assertTrue( "Result should be true", result.getResult() );
    assertEquals( "a", parentWorkflow.getVariable( "propertyFile" ) );
    assertEquals( "", parentWorkflow.getVariable( "newProperty" ) );
    assertEquals( "parentValue", parentWorkflow.getVariable( "parentParam" ) );
    assertEquals( "", parentWorkflow.getVariable( "staticProperty" ) );
    assertEquals( "a", parentWorkflow.getVariable( "dynamicProperty" ) );


    action.setFilename( "src/test/resources/org/apache/hop/workflow/actions/setvariables/configurationB.properties" );
    result = action.execute( new Result(), 0 );
    assertTrue( "Result should be true", result.getResult() );
    assertEquals( "b", parentWorkflow.getVariable( "propertyFile" ) );
    assertEquals( "new", parentWorkflow.getVariable( "newProperty" ) );
    assertEquals( "haha", parentWorkflow.getVariable( "parentParam" ) );
    assertEquals( "static", parentWorkflow.getVariable( "staticProperty" ) );
    assertEquals( "", parentWorkflow.getVariable( "dynamicProperty" ) );
  }

  @Test
  public void testJobEntrySetVariablesExecute_VARIABLE_TYPE_JVM_NullVariable() throws Exception {
    IHopMetadataProvider metadataProvider = mock( IHopMetadataProvider.class );
    action.loadXml( getEntryNode( "nullVariable", null, "JVM" ), metadataProvider, new Variables() );
    Result result = action.execute( new Result(), 0 );
    assertTrue( "Result should be true", result.getResult() );
    assertNull( System.getProperty( "nullVariable" ) );
  }

  @Test
  public void testJobEntrySetVariablesExecute_VARIABLE_TYPE_CURRENT_WORKFLOW_NullVariable() throws Exception {
    IHopMetadataProvider metadataProvider = mock( IHopMetadataProvider.class );
    action.loadXml( getEntryNode( "nullVariable", null, "CURRENT_WORKFLOW" ), metadataProvider, new Variables());
    Result result = action.execute( new Result(), 0 );
    assertTrue( "Result should be true", result.getResult() );
    assertNull( action.getVariable( "nullVariable" ) );
  }

  @Test
  public void testJobEntrySetVariablesExecute_VARIABLE_TYPE_JVM_VariableNotNull() throws Exception {
    IHopMetadataProvider metadataProvider = mock( IHopMetadataProvider.class );
    action.loadXml( getEntryNode( "variableNotNull", "someValue", "JVM" ), metadataProvider, new Variables());
    assertNull( System.getProperty( "variableNotNull" ) );
    Result result = action.execute( new Result(), 0 );
    assertTrue( "Result should be true", result.getResult() );
    assertEquals( "someValue", System.getProperty( "variableNotNull" ) );
  }

  @Test
  public void testJobEntrySetVariablesExecute_VARIABLE_TYPE_CURRENT_WORKFLOW_VariableNotNull() throws Exception {
    IHopMetadataProvider metadataProvider = mock( IHopMetadataProvider.class );
    action.loadXml( getEntryNode( "variableNotNull", "someValue", "CURRENT_WORKFLOW" ), metadataProvider, new Variables());
    assertNull( System.getProperty( "variableNotNull" ) );
    Result result = action.execute( new Result(), 0 );
    assertTrue( "Result should be true", result.getResult() );
    assertEquals( "someValue", action.getVariable( "variableNotNull" ) );
  }

  //prepare xml for use
  public Node getEntryNode( String variableName, String variableValue, String variableType )
    throws ParserConfigurationException, SAXException, IOException {
    StringBuilder sb = new StringBuilder();
    sb.append( XmlHandler.openTag( "workflow" ) );
    sb.append( "      " ).append( XmlHandler.openTag( "fields" ) );
    sb.append( "      " ).append( XmlHandler.openTag( "field" ) );
    sb.append( "      " ).append( XmlHandler.addTagValue( "variable_name", variableName ) );
    if ( variableValue != null ) {
      sb.append( "      " ).append( XmlHandler.addTagValue( "variable_value", variableValue ) );
    }
    if ( variableType != null ) {
      sb.append( "          " ).append(
        XmlHandler.addTagValue( "variable_type", variableType ) );
    }
    sb.append( "      " ).append( XmlHandler.closeTag( "field" ) );
    sb.append( "      " ).append( XmlHandler.closeTag( "fields" ) );
    sb.append( XmlHandler.closeTag( "workflow" ) );

    InputStream stream = new ByteArrayInputStream( sb.toString().getBytes( StandardCharsets.UTF_8 ) );
    DocumentBuilder db;
    Document doc;
    db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
    doc = db.parse( stream );
    Node entryNode = doc.getFirstChild();
    return entryNode;
  }

}
