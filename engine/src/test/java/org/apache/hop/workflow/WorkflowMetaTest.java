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

import org.apache.hop.core.Const;
import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.listeners.IContentChangedListener;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.actions.dummy.ActionDummy;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.io.IOException;
import java.net.URISyntaxException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class WorkflowMetaTest {

  private static final String WORKFLOW_META_NAME = "workflowName";

  private WorkflowMeta workflowMeta;
  private IVariables variables;
  private IContentChangedListener listener;

  @Before
  public void setUp() {
    workflowMeta = new WorkflowMeta();
    workflowMeta.setNameSynchronizedWithFilename( false );
    // prepare
    listener = mock( IContentChangedListener.class );
    workflowMeta.addContentChangedListener( listener );
    workflowMeta.setName( WORKFLOW_META_NAME );

    variables = new Variables();
  }

  @Test
  public void testPathExist() throws HopXmlException, IOException, URISyntaxException {
    assertTrue( testPath( "je1-je4" ) );
  }

  @Test
  public void testPathNotExist() throws HopXmlException, IOException, URISyntaxException {
    assertFalse( testPath( "je2-je4" ) );
  }

  private boolean testPath( String branch ) {
    ActionDummy je1 = new ActionDummy();
    je1.setName( "je1" );

    ActionDummy je2 = new ActionDummy();
    je2.setName( "je2" );

    WorkflowHopMeta hop = new WorkflowHopMeta( new ActionMeta( je1 ), new ActionMeta( je2 ) );
    workflowMeta.addWorkflowHop( hop );

    ActionDummy je3 = new ActionDummy();
    je3.setName( "je3" );
    hop = new WorkflowHopMeta( new ActionMeta( je1 ), new ActionMeta( je3 ) );
    workflowMeta.addWorkflowHop( hop );

    ActionDummy je4 = new ActionDummy();
    je4.setName( "je4" );
    hop = new WorkflowHopMeta( new ActionMeta( je3 ), new ActionMeta( je4 ) );
    workflowMeta.addWorkflowHop( hop );

    if ( branch.equals( "je1-je4" ) ) {
      return workflowMeta.isPathExist( je1, je4 );
    } else if ( branch.equals( "je2-je4" ) ) {
      return workflowMeta.isPathExist( je2, je4 );
    } else {
      return false;
    }
  }

  @Test
  public void testContentChangeListener() throws Exception {
    workflowMeta.setChanged();
    workflowMeta.setChanged( true );

    verify( listener, times( 2 ) ).contentChanged( same( workflowMeta ) );

    workflowMeta.clearChanged();
    workflowMeta.setChanged( false );

    verify( listener, times( 2 ) ).contentSafe( same( workflowMeta ) );

    workflowMeta.removeContentChangedListener( listener );
    workflowMeta.setChanged();
    workflowMeta.setChanged( true );

    verifyNoMoreInteractions( listener );
  }

  @Test
  public void shouldUseCoordinatesOfItsTransformsAndNotesWhenCalculatingMinimumPoint() {
    Point actionPoint = new Point( 500, 500 );
    Point notePadMetaPoint = new Point( 400, 400 );
    ActionMeta actionMeta = mock( ActionMeta.class );
    when( actionMeta.getLocation() ).thenReturn( actionPoint );
    NotePadMeta notePadMeta = mock( NotePadMeta.class );
    when( notePadMeta.getLocation() ).thenReturn( notePadMetaPoint );

    // empty Workflow return 0 coordinate point
    Point point = workflowMeta.getMinimum();
    assertEquals( 0, point.x );
    assertEquals( 0, point.y );

    // when Workflow contains a single transform or note, then workflowMeta should return coordinates of it, subtracting borders
    workflowMeta.addAction( 0, actionMeta );
    Point actualTransformPoint = workflowMeta.getMinimum();
    assertEquals( actionPoint.x - WorkflowMeta.BORDER_INDENT, actualTransformPoint.x );
    assertEquals( actionPoint.y - WorkflowMeta.BORDER_INDENT, actualTransformPoint.y );

    // when Workflow contains transform or notes, then workflowMeta should return minimal coordinates of them, subtracting borders
    workflowMeta.addNote( notePadMeta );
    Point transformPoint = workflowMeta.getMinimum();
    assertEquals( notePadMetaPoint.x - WorkflowMeta.BORDER_INDENT, transformPoint.x );
    assertEquals( notePadMetaPoint.y - WorkflowMeta.BORDER_INDENT, transformPoint.y );
  }

  @Test
  public void testEquals_oneNameNull() {
    assertFalse( testEquals( null, null ) );
  }

  @Test
  public void testEquals_secondNameNull() {
    workflowMeta.setName( null );
    assertFalse( testEquals( WORKFLOW_META_NAME, null ) );
  }

  @Test
  public void testEquals_sameFilename() {
    String newFilename = "Filename";
    workflowMeta.setFilename( newFilename );
    assertFalse( testEquals( null, newFilename ) );
  }

  @Test
  public void testEquals_difFilenameSameName() {
    workflowMeta.setFilename( "Filename" );
    assertFalse( testEquals( WORKFLOW_META_NAME, "OtherFileName" ) );
  }

  @Test
  public void testEquals_sameFilenameSameName() {
    String newFilename = "Filename";
    workflowMeta.setFilename( newFilename );
    assertTrue( testEquals( WORKFLOW_META_NAME, newFilename ) );
  }

  @Test
  public void testEquals_sameFilenameDifName() {
    String newFilename = "Filename";
    workflowMeta.setFilename( newFilename );
    assertFalse( testEquals( "OtherName", newFilename ) );
  }

  private boolean testEquals( String name, String filename ) {
    WorkflowMeta workflowMeta2 = new WorkflowMeta();
    workflowMeta2.setNameSynchronizedWithFilename( false );
    workflowMeta2.setName( name );
    workflowMeta2.setFilename( filename );
    return workflowMeta.equals( workflowMeta2 );
  }

  @Test
  public void testLoadXml() throws HopException {
    String directory = "/home/admin";
    Node workflowNode = Mockito.mock( Node.class );
    NodeList nodeList = new NodeList() {
      Node node = Mockito.mock( Node.class );

      {
        Mockito.when( node.getNodeName() ).thenReturn( "directory" );
        Node child = Mockito.mock( Node.class );
        Mockito.when( node.getFirstChild() ).thenReturn( child );
        Mockito.when( child.getNodeValue() ).thenReturn( directory );
      }

      @Override public Node item( int index ) {
        return node;
      }

      @Override public int getLength() {
        return 1;
      }
    };

    Mockito.when( workflowNode.getChildNodes() ).thenReturn( nodeList );

    WorkflowMeta meta = new WorkflowMeta();

    meta.loadXml( workflowNode, null, Mockito.mock( IHopMetadataProvider.class ), new Variables() );
    IWorkflowEngine<WorkflowMeta> workflow = new LocalWorkflowEngine( meta );
    workflow.setInternalHopVariables();
  }

  @Test
  public void testAddRemoveJobEntryCopySetUnsetParent() throws Exception {
    ActionMeta actionCopy = mock( ActionMeta.class );
    workflowMeta.addAction( actionCopy );
    workflowMeta.removeAction( 0 );
    verify( actionCopy, times( 1 ) ).setParentWorkflowMeta( workflowMeta );
    verify( actionCopy, times( 1 ) ).setParentWorkflowMeta( null );
  }

  @Test
  public void testHasLoop_simpleLoop() throws Exception {
    //main->2->3->main
    WorkflowMeta workflowMetaSpy = spy( workflowMeta );
    ActionMeta actionCopyMain = createAction( "mainTransform" );
    ActionMeta actionCopy2 = createAction( "transform2" );
    ActionMeta actionCopy3 = createAction( "transform3" );
    when( workflowMetaSpy.findNrPrevActions( actionCopyMain ) ).thenReturn( 1 );
    when( workflowMetaSpy.findPrevAction( actionCopyMain, 0 ) ).thenReturn( actionCopy2 );
    when( workflowMetaSpy.findNrPrevActions( actionCopy2 ) ).thenReturn( 1 );
    when( workflowMetaSpy.findPrevAction( actionCopy2, 0 ) ).thenReturn( actionCopy3 );
    when( workflowMetaSpy.findNrPrevActions( actionCopy3 ) ).thenReturn( 1 );
    when( workflowMetaSpy.findPrevAction( actionCopy3, 0 ) ).thenReturn( actionCopyMain );
    assertTrue( workflowMetaSpy.hasLoop( actionCopyMain ) );
  }

  @Test
  public void testHasLoop_loopInPrevTransforms() throws Exception {
    //main->2->3->4->3
    WorkflowMeta workflowMetaSpy = spy( workflowMeta );
    ActionMeta actionCopyMain = createAction( "mainTransform" );
    ActionMeta actionCopy2 = createAction( "transform2" );
    ActionMeta actionCopy3 = createAction( "transform3" );
    ActionMeta actionCopy4 = createAction( "transform4" );
    when( workflowMetaSpy.findNrPrevActions( actionCopyMain ) ).thenReturn( 1 );
    when( workflowMetaSpy.findPrevAction( actionCopyMain, 0 ) ).thenReturn( actionCopy2 );
    when( workflowMetaSpy.findNrPrevActions( actionCopy2 ) ).thenReturn( 1 );
    when( workflowMetaSpy.findPrevAction( actionCopy2, 0 ) ).thenReturn( actionCopy3 );
    when( workflowMetaSpy.findNrPrevActions( actionCopy3 ) ).thenReturn( 1 );
    when( workflowMetaSpy.findPrevAction( actionCopy3, 0 ) ).thenReturn( actionCopy4 );
    when( workflowMetaSpy.findNrPrevActions( actionCopy4 ) ).thenReturn( 1 );
    when( workflowMetaSpy.findPrevAction( actionCopy4, 0 ) ).thenReturn( actionCopy3 );
    //check no StackOverflow error
    assertFalse( workflowMetaSpy.hasLoop( actionCopyMain ) );
  }

  private ActionMeta createAction( String name ) {
    IAction action = mock( IAction.class );
    ActionMeta actionMeta = new ActionMeta( action );
    when( actionMeta.getName() ).thenReturn( name );
    return actionMeta;
  }

  @Test
  public void testSetInternalEntryCurrentDirectoryWithFilename() {
    WorkflowMeta workflowMetaTest = new WorkflowMeta();
    workflowMetaTest.setFilename( "hasFilename" );
    variables.setVariable( Const.INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER, "Original value defined at run execution" );
    variables.setVariable( Const.INTERNAL_VARIABLE_WORKFLOW_FILENAME_FOLDER, "file:///C:/SomeFilenameDirectory" );
    workflowMetaTest.setInternalEntryCurrentDirectory(variables);

    assertEquals( "file:///C:/SomeFilenameDirectory", variables.getVariable( Const.INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER ) );

  }

  @Test
  public void testSetInternalEntryCurrentDirectoryWithoutFilename() {
    WorkflowMeta workflowMetaTest = new WorkflowMeta();
    variables.setVariable( Const.INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER, "Original value defined at run execution" );
    variables.setVariable( Const.INTERNAL_VARIABLE_WORKFLOW_FILENAME_FOLDER, "file:///C:/SomeFilenameDirectory" );
    workflowMetaTest.setInternalEntryCurrentDirectory(variables);

    assertEquals( "Original value defined at run execution", variables.getVariable( Const.INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER ) );
  }

  @Test
  public void testUpdateCurrentDirWithFilename() {
    WorkflowMeta workflowMetaTest = new WorkflowMeta();
    workflowMetaTest.setFilename( "hasFilename" );
    variables.setVariable( Const.INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER, "Original value defined at run execution" );
    variables.setVariable( Const.INTERNAL_VARIABLE_WORKFLOW_FILENAME_FOLDER, "file:///C:/SomeFilenameDirectory" );
    workflowMetaTest.updateCurrentDir(variables);

    assertEquals( "file:///C:/SomeFilenameDirectory", variables.getVariable( Const.INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER ) );

  }

  @Test
  public void testUpdateCurrentDirWithoutFilename() {
    WorkflowMeta workflowMetaTest = new WorkflowMeta();
    variables.setVariable( Const.INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER, "Original value defined at run execution" );
    variables.setVariable( Const.INTERNAL_VARIABLE_WORKFLOW_FILENAME_FOLDER, "file:///C:/SomeFilenameDirectory" );
    workflowMetaTest.updateCurrentDir(variables);

    assertEquals( "Original value defined at run execution", variables.getVariable( Const.INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER ) );
  }

}
