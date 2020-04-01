/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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
package org.apache.hop.pipeline;

import org.apache.hop.core.Const;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.ProgressMonitorListener;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopStepException;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.listeners.ContentChangedListener;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.metastore.stores.memory.MemoryMetaStore;
import org.apache.hop.pipeline.step.StepIOMeta;
import org.apache.hop.pipeline.step.StepMeta;
import org.apache.hop.pipeline.step.StepMetaChangeListenerInterface;
import org.apache.hop.pipeline.step.StepMetaInterface;
import org.apache.hop.pipeline.steps.dummy.DummyMeta;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.modules.junit4.PowerMockRunner;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

//import org.apache.hop.pipeline.steps.datagrid.DataGridMeta;
//import org.apache.hop.pipeline.steps.textfileoutput.TextFileOutputMeta;
//import org.apache.hop.pipeline.steps.userdefinedjavaclass.InfoStepDefinition;
//import org.apache.hop.pipeline.steps.userdefinedjavaclass.UserDefinedJavaClassDef;
//import org.apache.hop.pipeline.steps.userdefinedjavaclass.UserDefinedJavaClassMeta;

@RunWith( PowerMockRunner.class )
public class PipelineMetaTest {
  public static final String STEP_NAME = "Any step name";


  @BeforeClass
  public static void initHop() throws Exception {
    HopEnvironment.init();
  }

  private PipelineMeta pipelineMeta;
  private IMetaStore metaStore;

  @Before
  public void setUp() throws Exception {
    pipelineMeta = new PipelineMeta();
    metaStore = new MemoryMetaStore();
  }

  @Test
  public void testGetMinimum() {
    final Point minimalCanvasPoint = new Point( 0, 0 );

    //for test goal should content coordinate more than NotePadMetaPoint
    final Point stepPoint = new Point( 500, 500 );

    //empty Pipeline return 0 coordinate point
    Point point = pipelineMeta.getMinimum();
    assertEquals( minimalCanvasPoint.x, point.x );
    assertEquals( minimalCanvasPoint.y, point.y );

    //when Pipeline  content Step  than pipeline should return minimal coordinate of step
    StepMeta stepMeta = mock( StepMeta.class );
    when( stepMeta.getLocation() ).thenReturn( stepPoint );
    pipelineMeta.addStep( stepMeta );
    Point actualStepPoint = pipelineMeta.getMinimum();
    assertEquals( stepPoint.x - PipelineMeta.BORDER_INDENT, actualStepPoint.x );
    assertEquals( stepPoint.y - PipelineMeta.BORDER_INDENT, actualStepPoint.y );
  }


  @Test
  public void getThisStepFieldsPassesCloneRowMeta() throws Exception {
    final String overriddenValue = "overridden";

    StepMeta nextStep = mockStepMeta( "nextStep" );

    StepMetaInterface smi = mock( StepMetaInterface.class );
    StepIOMeta ioMeta = mock( StepIOMeta.class );
    when( smi.getStepIOMeta() ).thenReturn( ioMeta );
    doAnswer( new Answer<Object>() {
      @Override public Object answer( InvocationOnMock invocation ) throws Throwable {
        RowMetaInterface rmi = (RowMetaInterface) invocation.getArguments()[ 0 ];
        rmi.clear();
        rmi.addValueMeta( new ValueMetaString( overriddenValue ) );
        return null;
      }
    } ).when( smi ).getFields(
      any( RowMetaInterface.class ), anyString(), any( RowMetaInterface[].class ), eq( nextStep ),
      any( VariableSpace.class ), any( IMetaStore.class ) );

    StepMeta thisStep = mockStepMeta( "thisStep" );
    when( thisStep.getStepMetaInterface() ).thenReturn( smi );

    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaString( "value" ) );

    RowMetaInterface thisStepsFields = pipelineMeta.getThisStepFields( thisStep, nextStep, rowMeta );

    assertEquals( 1, thisStepsFields.size() );
    assertEquals( overriddenValue, thisStepsFields.getValueMeta( 0 ).getName() );
  }

  @Test
  public void getThisStepFieldsPassesClonedInfoRowMeta() throws Exception {
    // given
    StepMetaInterface smi = mock( StepMetaInterface.class );
    StepIOMeta ioMeta = mock( StepIOMeta.class );
    when( smi.getStepIOMeta() ).thenReturn( ioMeta );

    StepMeta thisStep = mockStepMeta( "thisStep" );
    StepMeta nextStep = mockStepMeta( "nextStep" );
    when( thisStep.getStepMetaInterface() ).thenReturn( smi );

    RowMeta row = new RowMeta();
    when( smi.getTableFields() ).thenReturn( row );

    // when
    pipelineMeta.getThisStepFields( thisStep, nextStep, row );

    // then
    verify( smi, never() ).getFields( any(), any(), eq( new RowMetaInterface[] { row } ), any(), any(), any() );
  }

  @Test
  public void testContentChangeListener() throws Exception {
    ContentChangedListener listener = mock( ContentChangedListener.class );
    pipelineMeta.addContentChangedListener( listener );

    pipelineMeta.setChanged();
    pipelineMeta.setChanged( true );

    verify( listener, times( 2 ) ).contentChanged( same( pipelineMeta ) );

    pipelineMeta.clearChanged();
    pipelineMeta.setChanged( false );

    verify( listener, times( 2 ) ).contentSafe( same( pipelineMeta ) );

    pipelineMeta.removeContentChangedListener( listener );
    pipelineMeta.setChanged();
    pipelineMeta.setChanged( true );

    verifyNoMoreInteractions( listener );
  }

  @Test
  public void testCompare() throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setFilename( "aFile" );
    pipelineMeta.setName( "aName" );
    PipelineMeta pipelineMeta2 = new PipelineMeta();
    pipelineMeta2.setFilename( "aFile" );
    pipelineMeta2.setName( "aName" );
    assertEquals( 0, pipelineMeta.compare( pipelineMeta, pipelineMeta2 ) );
    pipelineMeta2.setVariable( "myVariable", "myValue" );
    assertEquals( 0, pipelineMeta.compare( pipelineMeta, pipelineMeta2 ) );
    pipelineMeta2.setFilename( null );
    assertEquals( 1, pipelineMeta.compare( pipelineMeta, pipelineMeta2 ) );
    assertEquals( -1, pipelineMeta.compare( pipelineMeta2, pipelineMeta ) );
    pipelineMeta2.setFilename( "aFile" );
    pipelineMeta2.setName( null );
    assertEquals( 1, pipelineMeta.compare( pipelineMeta, pipelineMeta2 ) );
    assertEquals( -1, pipelineMeta.compare( pipelineMeta2, pipelineMeta ) );
    pipelineMeta2.setFilename( "aFile2" );
    pipelineMeta2.setName( "aName" );
    assertEquals( -1, pipelineMeta.compare( pipelineMeta, pipelineMeta2 ) );
    assertEquals( 1, pipelineMeta.compare( pipelineMeta2, pipelineMeta ) );
    pipelineMeta2.setFilename( "aFile" );
    pipelineMeta2.setName( "aName2" );
    assertEquals( -1, pipelineMeta.compare( pipelineMeta, pipelineMeta2 ) );
    assertEquals( 1, pipelineMeta.compare( pipelineMeta2, pipelineMeta ) );
    pipelineMeta.setFilename( null );
    pipelineMeta2.setFilename( null );
    pipelineMeta2.setName( "aName" );
    assertEquals( 0, pipelineMeta.compare( pipelineMeta, pipelineMeta2 ) );
  }

  @Test
  public void testEquals() throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setFilename( "1" );
    pipelineMeta.setName( "2" );
    assertFalse( pipelineMeta.equals( "somethingelse" ) );
    PipelineMeta pipelineMeta2 = new PipelineMeta();
    pipelineMeta2.setFilename( "1" );
    pipelineMeta2.setName( "2" );
    assertTrue( pipelineMeta.equals( pipelineMeta2 ) );
  }

  @Test
  public void testPipelineHops() throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setFilename( "pipelineFile" );
    pipelineMeta.setName( "myPipeline" );
    StepMeta step1 = new StepMeta( "name1", null );
    StepMeta step2 = new StepMeta( "name2", null );
    StepMeta step3 = new StepMeta( "name3", null );
    StepMeta step4 = new StepMeta( "name4", null );
    PipelineHopMeta hopMeta1 = new PipelineHopMeta( step1, step2, true );
    PipelineHopMeta hopMeta2 = new PipelineHopMeta( step2, step3, true );
    PipelineHopMeta hopMeta3 = new PipelineHopMeta( step3, step4, false );
    pipelineMeta.addPipelineHop( 0, hopMeta1 );
    pipelineMeta.addPipelineHop( 1, hopMeta2 );
    pipelineMeta.addPipelineHop( 2, hopMeta3 );
    List<StepMeta> hops = pipelineMeta.getPipelineHopSteps( true );
    assertSame( step1, hops.get( 0 ) );
    assertSame( step2, hops.get( 1 ) );
    assertSame( step3, hops.get( 2 ) );
    assertSame( step4, hops.get( 3 ) );
    assertEquals( hopMeta2, pipelineMeta.findPipelineHop( "name2 --> name3 (enabled)" ) );
    assertEquals( hopMeta3, pipelineMeta.findPipelineHopFrom( step3 ) );
    assertEquals( hopMeta2, pipelineMeta.findPipelineHop( hopMeta2 ) );
    assertEquals( hopMeta1, pipelineMeta.findPipelineHop( step1, step2 ) );
    assertEquals( null, pipelineMeta.findPipelineHop( step3, step4, false ) );
    assertEquals( hopMeta3, pipelineMeta.findPipelineHop( step3, step4, true ) );
    assertEquals( hopMeta2, pipelineMeta.findPipelineHopTo( step3 ) );
    pipelineMeta.removePipelineHop( 0 );
    hops = pipelineMeta.getPipelineHopSteps( true );
    assertSame( step2, hops.get( 0 ) );
    assertSame( step3, hops.get( 1 ) );
    assertSame( step4, hops.get( 2 ) );
    pipelineMeta.removePipelineHop( hopMeta2 );
    hops = pipelineMeta.getPipelineHopSteps( true );
    assertSame( step3, hops.get( 0 ) );
    assertSame( step4, hops.get( 1 ) );
  }

  @Test
  public void testGetAllPipelineHops() throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setFilename( "pipelineFile" );
    pipelineMeta.setName( "myPipeline" );
    StepMeta step1 = new StepMeta( "name1", null );
    StepMeta step2 = new StepMeta( "name2", null );
    StepMeta step3 = new StepMeta( "name3", null );
    StepMeta step4 = new StepMeta( "name4", null );
    PipelineHopMeta hopMeta1 = new PipelineHopMeta( step1, step2, true );
    PipelineHopMeta hopMeta2 = new PipelineHopMeta( step2, step3, true );
    PipelineHopMeta hopMeta3 = new PipelineHopMeta( step2, step4, true );
    pipelineMeta.addPipelineHop( 0, hopMeta1 );
    pipelineMeta.addPipelineHop( 1, hopMeta2 );
    pipelineMeta.addPipelineHop( 2, hopMeta3 );
    List<PipelineHopMeta> allPipelineHopFrom = pipelineMeta.findAllPipelineHopFrom( step2 );
    assertEquals( step3, allPipelineHopFrom.get( 0 ).getToStep() );
    assertEquals( step4, allPipelineHopFrom.get( 1 ).getToStep() );
  }

  @Test
  public void testAddStepWithChangeListenerInterface() {
    StepMeta stepMeta = mock( StepMeta.class );
    StepMetaChangeListenerInterfaceMock metaInterface = mock( StepMetaChangeListenerInterfaceMock.class );
    when( stepMeta.getStepMetaInterface() ).thenReturn( metaInterface );
    assertEquals( 0, pipelineMeta.steps.size() );
    assertEquals( 0, pipelineMeta.stepChangeListeners.size() );
    // should not throw exception if there are no steps in step meta
    pipelineMeta.addStep( 0, stepMeta );
    assertEquals( 1, pipelineMeta.steps.size() );
    assertEquals( 1, pipelineMeta.stepChangeListeners.size() );

    pipelineMeta.addStep( 0, stepMeta );
    assertEquals( 2, pipelineMeta.steps.size() );
    assertEquals( 2, pipelineMeta.stepChangeListeners.size() );
  }

  @Test
  public void testIsAnySelectedStepUsedInPipelineHopsNothingSelectedCase() {
    List<StepMeta> selectedSteps = asList( new StepMeta(), new StepMeta(), new StepMeta() );
    pipelineMeta.getSteps().addAll( selectedSteps );

    assertFalse( pipelineMeta.isAnySelectedStepUsedInPipelineHops() );
  }

  @Test
  public void testIsAnySelectedStepUsedInPipelineHopsAnySelectedCase() {
    StepMeta stepMeta = new StepMeta();
    stepMeta.setName( STEP_NAME );
    PipelineHopMeta pipelineHopMeta = new PipelineHopMeta();
    stepMeta.setSelected( true );
    List<StepMeta> selectedSteps = asList( new StepMeta(), stepMeta, new StepMeta() );

    pipelineHopMeta.setToStep( stepMeta );
    pipelineHopMeta.setFromStep( stepMeta );
    pipelineMeta.getSteps().addAll( selectedSteps );
    pipelineMeta.addPipelineHop( pipelineHopMeta );

    assertTrue( pipelineMeta.isAnySelectedStepUsedInPipelineHops() );
  }

  @Test
  public void testCloneWithParam() throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setFilename( "pipelineFile" );
    pipelineMeta.setName( "myPipeline" );
    pipelineMeta.addParameterDefinition( "key", "defValue", "description" );
    Object clone = pipelineMeta.realClone( true );
    assertNotNull( clone );
  }

  private static StepMeta mockStepMeta( String name ) {
    StepMeta meta = mock( StepMeta.class );
    when( meta.getName() ).thenReturn( name );
    return meta;
  }

  public abstract static class StepMetaChangeListenerInterfaceMock
    implements StepMetaInterface, StepMetaChangeListenerInterface {
    @Override
    public abstract Object clone();
  }

  @Test
  public void testLoadXml() throws HopException {
    String directory = "/home/admin";
    Node jobNode = Mockito.mock( Node.class );
    NodeList nodeList = new NodeList() {
      ArrayList<Node> nodes = new ArrayList<>();

      {

        Node nodeInfo = Mockito.mock( Node.class );
        Mockito.when( nodeInfo.getNodeName() ).thenReturn( PipelineMeta.XML_TAG_INFO );
        Mockito.when( nodeInfo.getChildNodes() ).thenReturn( this );

        Node nodeDirectory = Mockito.mock( Node.class );
        Mockito.when( nodeDirectory.getNodeName() ).thenReturn( "directory" );
        Node child = Mockito.mock( Node.class );
        Mockito.when( nodeDirectory.getFirstChild() ).thenReturn( child );
        Mockito.when( child.getNodeValue() ).thenReturn( directory );

        nodes.add( nodeDirectory );
        nodes.add( nodeInfo );

      }

      @Override public Node item( int index ) {
        return nodes.get( index );
      }

      @Override public int getLength() {
        return nodes.size();
      }
    };

    Mockito.when( jobNode.getChildNodes() ).thenReturn( nodeList );

    PipelineMeta meta = new PipelineMeta();

    VariableSpace variableSpace = Mockito.mock( VariableSpace.class );
    Mockito.when( variableSpace.listVariables() ).thenReturn( new String[ 0 ] );

    meta.loadXML( jobNode, null, metaStore, false, variableSpace );
    meta.setInternalHopVariables( null );
  }

  @Test
  public void testHasLoop_simpleLoop() throws Exception {
    //main->2->3->main
    PipelineMeta pipelineMetaSpy = spy( pipelineMeta );
    StepMeta stepMetaMain = createStepMeta( "mainStep" );
    StepMeta stepMeta2 = createStepMeta( "step2" );
    StepMeta stepMeta3 = createStepMeta( "step3" );
    List<StepMeta> mainPrevSteps = new ArrayList<>();
    mainPrevSteps.add( stepMeta2 );
    doReturn( mainPrevSteps ).when( pipelineMetaSpy ).findPreviousSteps( stepMetaMain, true );
    when( pipelineMetaSpy.findNrPrevSteps( stepMetaMain ) ).thenReturn( 1 );
    when( pipelineMetaSpy.findPrevStep( stepMetaMain, 0 ) ).thenReturn( stepMeta2 );
    List<StepMeta> stepmeta2PrevSteps = new ArrayList<>();
    stepmeta2PrevSteps.add( stepMeta3 );
    doReturn( stepmeta2PrevSteps ).when( pipelineMetaSpy ).findPreviousSteps( stepMeta2, true );
    when( pipelineMetaSpy.findNrPrevSteps( stepMeta2 ) ).thenReturn( 1 );
    when( pipelineMetaSpy.findPrevStep( stepMeta2, 0 ) ).thenReturn( stepMeta3 );
    List<StepMeta> stepmeta3PrevSteps = new ArrayList<>();
    stepmeta3PrevSteps.add( stepMetaMain );
    doReturn( stepmeta3PrevSteps ).when( pipelineMetaSpy ).findPreviousSteps( stepMeta3, true );
    when( pipelineMetaSpy.findNrPrevSteps( stepMeta3 ) ).thenReturn( 1 );
    when( pipelineMetaSpy.findPrevStep( stepMeta3, 0 ) ).thenReturn( stepMetaMain );
    assertTrue( pipelineMetaSpy.hasLoop( stepMetaMain ) );
  }

  @Test
  public void testHasLoop_loopInPrevSteps() throws Exception {
    //main->2->3->4->3
    PipelineMeta pipelineMetaSpy = spy( pipelineMeta );
    StepMeta stepMetaMain = createStepMeta( "mainStep" );
    StepMeta stepMeta2 = createStepMeta( "step2" );
    StepMeta stepMeta3 = createStepMeta( "step3" );
    StepMeta stepMeta4 = createStepMeta( "step4" );
    when( pipelineMetaSpy.findNrPrevSteps( stepMetaMain ) ).thenReturn( 1 );
    when( pipelineMetaSpy.findPrevStep( stepMetaMain, 0 ) ).thenReturn( stepMeta2 );
    when( pipelineMetaSpy.findNrPrevSteps( stepMeta2 ) ).thenReturn( 1 );
    when( pipelineMetaSpy.findPrevStep( stepMeta2, 0 ) ).thenReturn( stepMeta3 );
    when( pipelineMetaSpy.findNrPrevSteps( stepMeta3 ) ).thenReturn( 1 );
    when( pipelineMetaSpy.findPrevStep( stepMeta3, 0 ) ).thenReturn( stepMeta4 );
    when( pipelineMetaSpy.findNrPrevSteps( stepMeta4 ) ).thenReturn( 1 );
    when( pipelineMetaSpy.findPrevStep( stepMeta4, 0 ) ).thenReturn( stepMeta3 );
    //check no StackOverflow error
    assertFalse( pipelineMetaSpy.hasLoop( stepMetaMain ) );
  }


  @Test
  public void infoStepFieldsAreNotIncludedInGetStepFields() throws HopStepException {
    // validates that the fields from info steps are not included in the resulting step fields for a stepMeta.
    //  This is important with steps like StreamLookup and Append, where the previous steps may or may not
    //  have their fields included in the current step.

    PipelineMeta pipelineMeta = new PipelineMeta( new Variables() );
    StepMeta toBeAppended1 = testStep( "toBeAppended1",
      emptyList(),  // no info steps
      asList( "field1", "field2" )  // names of fields from this step
    );
    StepMeta toBeAppended2 = testStep( "toBeAppended2", emptyList(), asList( "field1", "field2" ) );

    StepMeta append = testStep( "append",
      asList( "toBeAppended1", "toBeAppended2" ),  // info step names
      singletonList( "outputField" )   // output field of this step
    );
    StepMeta after = new StepMeta( "after", new DummyMeta() );

    wireUpTestPipelineMeta( pipelineMeta, toBeAppended1, toBeAppended2, append, after );

    RowMetaInterface results = pipelineMeta.getStepFields( append, after, mock( ProgressMonitorListener.class ) );

    assertThat( 1, equalTo( results.size() ) );
    assertThat( "outputField", equalTo( results.getFieldNames()[ 0 ] ) );
  }

  @Test
  public void prevStepFieldsAreIncludedInGetStepFields() throws HopStepException {

    PipelineMeta pipelineMeta = new PipelineMeta( new Variables() );
    StepMeta prevStep1 = testStep( "prevStep1", emptyList(), asList( "field1", "field2" ) );
    StepMeta prevStep2 = testStep( "prevStep2", emptyList(), asList( "field3", "field4", "field5" ) );

    StepMeta someStep = testStep( "step", asList( "prevStep1" ), asList( "outputField" ) );

    StepMeta after = new StepMeta( "after", new DummyMeta() );

    wireUpTestPipelineMeta( pipelineMeta, prevStep1, prevStep2, someStep, after );

    RowMetaInterface results = pipelineMeta.getStepFields( someStep, after, mock( ProgressMonitorListener.class ) );

    assertThat( 4, equalTo( results.size() ) );
    assertThat( new String[] { "field3", "field4", "field5", "outputField" }, equalTo( results.getFieldNames() ) );
  }

  @Test
  public void findPreviousStepsNullMeta() {
    PipelineMeta pipelineMeta = new PipelineMeta( new Variables() );
    List<StepMeta> result = pipelineMeta.findPreviousSteps( null, false );

    assertThat( 0, equalTo( result.size() ) );
    assertThat( result, equalTo( new ArrayList<>() ) );
  }

  private void wireUpTestPipelineMeta( PipelineMeta pipelineMeta, StepMeta toBeAppended1, StepMeta toBeAppended2,
                                    StepMeta append, StepMeta after ) {
    pipelineMeta.addStep( append );
    pipelineMeta.addStep( after );
    pipelineMeta.addStep( toBeAppended1 );
    pipelineMeta.addStep( toBeAppended2 );

    pipelineMeta.addPipelineHop( new PipelineHopMeta( toBeAppended1, append ) );
    pipelineMeta.addPipelineHop( new PipelineHopMeta( toBeAppended2, append ) );
    pipelineMeta.addPipelineHop( new PipelineHopMeta( append, after ) );
  }


  private StepMeta testStep( String name, List<String> infoStepnames, List<String> fieldNames )
    throws HopStepException {
    StepMetaInterface smi = stepMetaInterfaceWithFields( new DummyMeta(), infoStepnames, fieldNames );
    return new StepMeta( name, smi );
  }

  private StepMetaInterface stepMetaInterfaceWithFields(
    StepMetaInterface smi, List<String> infoStepnames, List<String> fieldNames )
    throws HopStepException {
    RowMeta rowMetaWithFields = new RowMeta();
    StepIOMeta stepIOMeta = mock( StepIOMeta.class );
    when( stepIOMeta.getInfoStepnames() ).thenReturn( infoStepnames.toArray( new String[ 0 ] ) );
    fieldNames.stream()
      .forEach( field -> rowMetaWithFields.addValueMeta( new ValueMetaString( field ) ) );
    StepMetaInterface newSmi = spy( smi );
    when( newSmi.getStepIOMeta() ).thenReturn( stepIOMeta );

    doAnswer( (Answer<Void>) invocationOnMock -> {
      RowMetaInterface passedRmi = (RowMetaInterface) invocationOnMock.getArguments()[ 0 ];
      passedRmi.addRowMeta( rowMetaWithFields );
      return null;
    } ).when( newSmi )
      .getFields( any(), any(), any(), any(), any(), any() );

    return newSmi;
  }


  private StepMeta createStepMeta( String name ) {
    StepMeta stepMeta = mock( StepMeta.class );
    when( stepMeta.getName() ).thenReturn( name );
    return stepMeta;
  }

  @Test
  public void testSetInternalEntryCurrentDirectoryWithFilename() {
    PipelineMeta pipelineMetaTest = new PipelineMeta();
    pipelineMetaTest.setFilename( "hasFilename" );
    pipelineMetaTest.setVariable( Const.INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY, "Original value defined at run execution" );
    pipelineMetaTest.setVariable( Const.INTERNAL_VARIABLE_PIPELINE_FILENAME_DIRECTORY, "file:///C:/SomeFilenameDirectory" );
    pipelineMetaTest.setInternalEntryCurrentDirectory();

    assertEquals( "file:///C:/SomeFilenameDirectory", pipelineMetaTest.getVariable( Const.INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY ) );

  }


  @Test
  public void testSetInternalEntryCurrentDirectoryWithoutFilename() {
    PipelineMeta pipelineMetaTest = new PipelineMeta();
    pipelineMetaTest.setVariable( Const.INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY, "Original value defined at run execution" );
    pipelineMetaTest.setVariable( Const.INTERNAL_VARIABLE_PIPELINE_FILENAME_DIRECTORY, "file:///C:/SomeFilenameDirectory" );
    pipelineMetaTest.setInternalEntryCurrentDirectory();

    assertEquals( "Original value defined at run execution", pipelineMetaTest.getVariable( Const.INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY ) );
  }
}
