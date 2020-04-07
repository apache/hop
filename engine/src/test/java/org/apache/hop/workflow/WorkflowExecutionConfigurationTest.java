/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.workflow;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.ClassRule;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WorkflowExecutionConfigurationTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Test
  public void testDefaultPassedBatchId() {
    WorkflowExecutionConfiguration jec = new WorkflowExecutionConfiguration();
    assertEquals( "default passedBatchId value must be null", null, jec.getPassedBatchId() );
  }

  @Test
  public void testCopy() {
    WorkflowExecutionConfiguration jec = new WorkflowExecutionConfiguration();
    final Long passedBatchId0 = null;
    final long passedBatchId1 = 0L;
    final long passedBatchId2 = 5L;

    jec.setPassedBatchId( passedBatchId0 );
    //CHECKSTYLE IGNORE AvoidNestedBlocks FOR NEXT 3 LINES
    {
      WorkflowExecutionConfiguration jecCopy = (WorkflowExecutionConfiguration) jec.clone();
      assertEquals( "clone-copy", jec.getPassedBatchId(), jecCopy.getPassedBatchId() );
    }
    jec.setPassedBatchId( passedBatchId1 );
    //CHECKSTYLE IGNORE AvoidNestedBlocks FOR NEXT 3 LINES
    {
      WorkflowExecutionConfiguration jecCopy = (WorkflowExecutionConfiguration) jec.clone();
      assertEquals( "clone-copy", jec.getPassedBatchId(), jecCopy.getPassedBatchId() );
    }
    jec.setPassedBatchId( passedBatchId2 );
    //CHECKSTYLE IGNORE AvoidNestedBlocks FOR NEXT 3 LINES
    {
      WorkflowExecutionConfiguration jecCopy = (WorkflowExecutionConfiguration) jec.clone();
      assertEquals( "clone-copy", jec.getPassedBatchId(), jecCopy.getPassedBatchId() );
    }
  }

  @Test
  public void testCopyXml() throws Exception {
    WorkflowExecutionConfiguration jec = new WorkflowExecutionConfiguration();
    final Long passedBatchId0 = null;
    final long passedBatchId1 = 0L;
    final long passedBatchId2 = 5L;

    jec.setPassedBatchId( passedBatchId0 );
    //CHECKSTYLE IGNORE AvoidNestedBlocks FOR NEXT 3 LINES
    {
      String xml = jec.getXML();
      Document doc = XMLHandler.loadXMLString( xml );
      Node node = XMLHandler.getSubNode( doc, WorkflowExecutionConfiguration.XML_TAG );
      WorkflowExecutionConfiguration jecCopy = new WorkflowExecutionConfiguration( node );
      assertEquals( "xml-copy", jec.getPassedBatchId(), jecCopy.getPassedBatchId() );
    }
    jec.setPassedBatchId( passedBatchId1 );
    //CHECKSTYLE IGNORE AvoidNestedBlocks FOR NEXT 3 LINES
    {
      String xml = jec.getXML();
      Document doc = XMLHandler.loadXMLString( xml );
      Node node = XMLHandler.getSubNode( doc, WorkflowExecutionConfiguration.XML_TAG );
      WorkflowExecutionConfiguration jecCopy = new WorkflowExecutionConfiguration( node );
      assertEquals( "xml-copy", jec.getPassedBatchId(), jecCopy.getPassedBatchId() );
    }
    jec.setPassedBatchId( passedBatchId2 );
    //CHECKSTYLE IGNORE AvoidNestedBlocks FOR NEXT 3 LINES
    {
      String xml = jec.getXML();
      Document doc = XMLHandler.loadXMLString( xml );
      Node node = XMLHandler.getSubNode( doc, WorkflowExecutionConfiguration.XML_TAG );
      WorkflowExecutionConfiguration jecCopy = new WorkflowExecutionConfiguration( node );
      assertEquals( "xml-copy", jec.getPassedBatchId(), jecCopy.getPassedBatchId() );
    }
  }

  @Test
  public void testGetUsedVariablesWithNoPreviousExecutionConfigurationVariables() throws HopException {
    WorkflowExecutionConfiguration executionConfiguration = new WorkflowExecutionConfiguration();
    Map<String, String> variables0 = new HashMap<>();

    executionConfiguration.setVariablesMap( variables0 );

    WorkflowMeta workflowMeta0 = mock( WorkflowMeta.class );
    List<String> list0 = new ArrayList<>();
    list0.add( "var1" );
    when( workflowMeta0.getUsedVariables() ).thenReturn( list0 );
    // Const.INTERNAL_VARIABLE_PREFIX values
    when( workflowMeta0.getVariable( anyString() ) ).thenReturn( "internalDummyValue" );

    executionConfiguration.getUsedVariables( workflowMeta0 );

    // 8 = 7 internalDummyValues + var1 from WorkflowMeta with default value
    assertEquals( 7, executionConfiguration.getVariablesMap().size() );
    assertEquals( "", executionConfiguration.getVariablesMap().get( "var1" ) );

  }

  @Test
  public void testGetUsedVariablesWithSamePreviousExecutionConfigurationVariables() throws HopException {
    WorkflowExecutionConfiguration executionConfiguration = new WorkflowExecutionConfiguration();
    Map<String, String> variables0 = new HashMap<>();
    variables0.put( "var1", "valueVar1" );
    executionConfiguration.setVariablesMap( variables0 );

    WorkflowMeta workflowMeta0 = mock( WorkflowMeta.class );
    List<String> list0 = new ArrayList<>();
    list0.add( "var1" );
    when( workflowMeta0.getUsedVariables() ).thenReturn( list0 );
    when( workflowMeta0.getVariable( anyString() ) ).thenReturn( "internalDummyValue" );

    executionConfiguration.getUsedVariables( workflowMeta0 );

    // 8 = 7 internalDummyValues + var1 from WorkflowMeta ( with variables0 value )
    assertEquals( 7, executionConfiguration.getVariablesMap().size() );
    assertEquals( "valueVar1", executionConfiguration.getVariablesMap().get( "var1" ) );

  }

  @Test
  public void testGetUsedVariablesWithDifferentPreviousExecutionConfigurationVariables() throws HopException {
    WorkflowExecutionConfiguration executionConfiguration = new WorkflowExecutionConfiguration();
    Map<String, String> variables0 = new HashMap<>();
    variables0.put( "var2", "valueVar2" );
    executionConfiguration.setVariablesMap( variables0 );

    WorkflowMeta workflowMeta0 = mock( WorkflowMeta.class );
    List<String> list0 = new ArrayList<>();
    list0.add( "var1" );
    when( workflowMeta0.getUsedVariables() ).thenReturn( list0 );
    when( workflowMeta0.getVariable( anyString() ) ).thenReturn( "internalDummyValue" );

    executionConfiguration.getUsedVariables( workflowMeta0 );

    // 9 = 7 internalDummyValues + var1 from WorkflowMeta ( with the default value ) + var2 from variables0
    assertEquals( 8, executionConfiguration.getVariablesMap().size() );
    assertEquals( "", executionConfiguration.getVariablesMap().get( "var1" ) );
    assertEquals( "valueVar2", executionConfiguration.getVariablesMap().get( "var2" ) );

  }

}
