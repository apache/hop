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

package org.apache.hop.pipeline.transforms.pipelineexecutor;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.QueueRowSet;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.RowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LogChannelInterface;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.iVariables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.TransformMockUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Andrey Khayrutdinov
 */
public class PipelineExecutorUnitTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @BeforeClass
  public static void initHop() throws Exception {
    HopEnvironment.init();
  }

  private PipelineExecutor executor;
  private PipelineExecutorMeta meta;
  private PipelineExecutorData data;
  private Pipeline internalPipeline;
  private Result internalResult;

  @Before
  public void setUp() throws Exception {
    executor = TransformMockUtil.getTransform( PipelineExecutor.class, PipelineExecutorMeta.class, "PipelineExecutorUnitTest" );
    executor = spy( executor );

    PipelineMeta internalPipelineMeta = mock( PipelineMeta.class );
    doReturn( internalPipelineMeta ).when( executor ).loadExecutorPipelineMeta();

    internalPipeline = spy( new LocalPipelineEngine() );
    internalPipeline.setLog( mock( LogChannelInterface.class ) );
    doNothing().when( internalPipeline ).prepareExecution();
    doNothing().when( internalPipeline ).startThreads();
    doNothing().when( internalPipeline ).waitUntilFinished();
    doNothing().when( executor ).discardLogLines( any( PipelineExecutorData.class ) );

    doReturn( internalPipeline ).when( executor ).createInternalPipeline();
    internalResult = new Result();
    doReturn( internalResult ).when( internalPipeline ).getResult();

    meta = new PipelineExecutorMeta();
    data = new PipelineExecutorData();
  }

  @After
  public void cleanUp() {
    executor = null;
    meta = null;
    data = null;
    internalPipeline = null;
    internalResult = null;
  }

  @Test
  public void testCreateInternalPipelineSets() throws HopException {
    Pipeline transParentMock = mock( Pipeline.class );
    PipelineExecutorData pipelineExecutorDataMock = mock( PipelineExecutorData.class );
    PipelineMeta transMetaMock = mock( PipelineMeta.class );

    executor.init();
    when( transParentMock.getLogLevel() ).thenReturn( LogLevel.DEBUG );
    doNothing().when( transParentMock ).initializeVariablesFrom( any( iVariables.class ) );
    when( executor.getLogLevel() ).thenReturn( LogLevel.DEBUG );
    when( executor.createInternalPipeline() ).thenCallRealMethod();
    when( executor.getPipeline() ).thenReturn( transParentMock );
    when( executor.getData() ).thenReturn( pipelineExecutorDataMock );
    when( transMetaMock.listVariables() ).thenReturn( new String[ 0 ] );
    when( transMetaMock.listParameters() ).thenReturn( new String[ 0 ] );
    when( pipelineExecutorDataMock.getExecutorPipelineMeta() ).thenReturn( transMetaMock );

    Pipeline internalPipeline = executor.createInternalPipeline();
    assertNotNull( internalPipeline );

    Pipeline parentPipeline = internalPipeline.getParentPipeline();
    assertEquals( parentPipeline, transParentMock );
  }


  @Test
  public void collectsResultsFromInternalPipeline() throws Exception {
    prepareOneRowForExecutor();

    RowMetaAndData expectedResult = new RowMetaAndData( new RowMeta(), "fake result" );
    internalResult.getRows().add( expectedResult );

    RowSet rowSet = new QueueRowSet();
    // any value except null
    TransformMeta transformMeta = mockTransformAndMapItToRowSet( "transformMetaMock", rowSet );
    meta.setOutputRowsSourceTransformMeta( transformMeta );

    executor.init();
    executor.setInputRowMeta( new RowMeta() );
    assertTrue( "Passing one line at first time", executor.init();
    assertFalse( "Executing the internal pipeline during the second round", executor.init();

    Object[] resultsRow = rowSet.getRowImmediate();
    assertNotNull( resultsRow );
    assertArrayEquals( expectedResult.getData(), resultsRow );
    assertNull( "Only one row is expected", rowSet.getRowImmediate() );
  }


  @Test
  public void collectsExecutionResults() throws Exception {
    prepareOneRowForExecutor();

    TransformMeta parentTransformMeta = mock( TransformMeta.class );
    when( parentTransformMeta.getName() ).thenReturn( "parentTransformMeta" );
    meta.setParentTransformMeta( parentTransformMeta );

    internalResult.setResult( true );
    meta.setExecutionResultField( "executionResultField" );

    internalResult.setNrErrors( 1 );
    meta.setExecutionNrErrorsField( "executionNrErrorsField" );

    internalResult.setNrLinesRead( 2 );
    meta.setExecutionLinesReadField( "executionLinesReadField" );

    internalResult.setNrLinesWritten( 3 );
    meta.setExecutionLinesWrittenField( "executionLinesWrittenField" );

    internalResult.setNrLinesInput( 4 );
    meta.setExecutionLinesInputField( "executionLinesInputField" );

    internalResult.setNrLinesOutput( 5 );
    meta.setExecutionLinesOutputField( "executionLinesOutputField" );

    internalResult.setNrLinesRejected( 6 );
    meta.setExecutionLinesRejectedField( "executionLinesRejectedField" );

    internalResult.setNrLinesUpdated( 7 );
    meta.setExecutionLinesUpdatedField( "executionLinesUpdatedField" );

    internalResult.setNrLinesDeleted( 8 );
    meta.setExecutionLinesDeletedField( "executionLinesDeletedField" );

    internalResult.setNrFilesRetrieved( 9 );
    meta.setExecutionFilesRetrievedField( "executionFilesRetrievedField" );

    internalResult.setExitStatus( 10 );
    meta.setExecutionExitStatusField( "executionExitStatusField" );


    RowSet rowSet = new QueueRowSet();
    // any value except null
    TransformMeta transformMeta = mockTransformAndMapItToRowSet( "transformMetaMock", rowSet );
    meta.setExecutionResultTargetTransformMeta( transformMeta );

    executor.init();
    executor.setInputRowMeta( new RowMeta() );
    assertTrue( "Passing one line at first time", executor.init();
    assertFalse( "Executing the internal pipeline during the second round", executor.init();

    Object[] resultsRow = rowSet.getRowImmediate();
    assertNotNull( resultsRow );
    assertNull( "Only one row is expected", rowSet.getRowImmediate() );

    assertEquals( internalResult.getResult(), resultsRow[ 0 ] );
    assertEquals( internalResult.getNrErrors(), resultsRow[ 1 ] );
    assertEquals( internalResult.getNrLinesRead(), resultsRow[ 2 ] );
    assertEquals( internalResult.getNrLinesWritten(), resultsRow[ 3 ] );
    assertEquals( internalResult.getNrLinesInput(), resultsRow[ 4 ] );
    assertEquals( internalResult.getNrLinesOutput(), resultsRow[ 5 ] );
    assertEquals( internalResult.getNrLinesRejected(), resultsRow[ 6 ] );
    assertEquals( internalResult.getNrLinesUpdated(), resultsRow[ 7 ] );
    assertEquals( internalResult.getNrLinesDeleted(), resultsRow[ 8 ] );
    assertEquals( internalResult.getNrFilesRetrieved(), resultsRow[ 9 ] );
    assertEquals( internalResult.getExitStatus(), ( (Number) resultsRow[ 10 ] ).intValue() );
  }

  /**
   * Given an input data and a pipeline executor with specified field to group rows on.
   * <br/>
   * When pipeline executor is processing rows of an input data,
   * then rows should be accumulated in a group as long as the specified field value stays the same.
   */
  @Test
  public void shouldAccumulateRowsWhenGroupFieldIsSpecified() throws HopException {
    prepareMultipleRowsForExecutor();

    meta.setGroupField( "groupField" );
    executor.init();

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaString( "groupField" ) );
    executor.setInputRowMeta( rowMeta );

    // start processing
    executor.init(); // 1st row - 'value1'
    // should be added to group buffer
    assertEquals( 1, data.groupBuffer.size() );

    executor.init();
    executor.init();
    executor.init(); // 4th row - still 'value1'
    // first 4 rows should be added to the same group
    assertEquals( 4, data.groupBuffer.size() );

    executor.init(); // 5th row - value has been changed - 'value12'
    // previous group buffer should be flushed
    // and a new group should be started
    assertEquals( 1, data.groupBuffer.size() );

    executor.init(); // 6th row - 'value12'
    executor.init(); // 7th row - 'value12'
    // the rest rows should be added to another group
    assertEquals( 3, data.groupBuffer.size() );

    executor.init(); // end of file
    // group buffer should be flushed in the end
    assertEquals( 0, data.groupBuffer.size() );
  }

  /**
   * Given an input data and a pipeline executor
   * with specified number of rows to send to the pipeline (X).
   * <br/>
   * When pipeline executor is processing rows of an input data,
   * then every X rows should be accumulated in a group.
   */
  @Test
  public void shouldAccumulateRowsByCount() throws HopException {
    prepareMultipleRowsForExecutor();

    meta.setGroupSize( "5" );
    executor.init();

    // start processing
    executor.init(); // 1st row
    // should be added to group buffer
    assertEquals( 1, data.groupBuffer.size() );

    executor.init();
    executor.init();
    executor.init(); // 4th row
    // first 4 rows should be added to the same group
    assertEquals( 4, data.groupBuffer.size() );

    executor.init(); // 5th row
    // once the 5th row is processed, the pipeline executor should be triggered
    // and thus, group buffer should be flushed
    assertEquals( 0, data.groupBuffer.size() );

    executor.init(); // 6th row
    // previous group buffer should be flushed
    // and a new group should be started
    assertEquals( 1, data.groupBuffer.size() );

    executor.init(); // 7th row
    // the rest rows should be added to another group
    assertEquals( 2, data.groupBuffer.size() );

    executor.init(); // end of file
    // group buffer should be flushed in the end
    assertEquals( 0, data.groupBuffer.size() );
  }

  @Test
  public void testCollectPipelineResultsDisabledHop() throws HopException {
    TransformMeta outputRowsSourceTransformMeta = mock( TransformMeta.class );
    meta.setOutputRowsSourceTransformMeta( outputRowsSourceTransformMeta );

    Result result = mock( Result.class );
    RowMetaAndData rowMetaAndData = mock( RowMetaAndData.class );
    when( result.getRows() ).thenReturn( Arrays.asList( rowMetaAndData ) );

    doNothing().when( executor ).putRowTo( any(), any(), any() );

    executor.init();
    executor.collectPipelineResults( result );
    verify( executor, never() ).putRowTo( any(), any(), any() );
  }

  @Test
  public void testCollectExecutionResultsDisabledHop() throws HopException {
    TransformMeta executionResultTargetTransformMeta = mock( TransformMeta.class );
    meta.setExecutionResultTargetTransformMeta( executionResultTargetTransformMeta );

    IRowMeta executionResultsOutputRowMeta = mock( IRowMeta.class );
    data.setExecutionResultsOutputRowMeta( executionResultsOutputRowMeta );

    doNothing().when( executor ).putRowTo( any(), any(), any() );

    executor.init();
    Result result = mock( Result.class );
    executor.collectExecutionResults( result );

    verify( executor, never() ).putRowTo( any(), any(), any() );
  }

  @Test
  public void testCollectExecutionResultFilesDisabledHop() throws HopException {
    Result result = mock( Result.class );
    ResultFile resultFile = mock( ResultFile.class, RETURNS_DEEP_STUBS );

    when( result.getResultFilesList() ).thenReturn( Arrays.asList( resultFile ) );

    TransformMeta resultFilesTargetTransformMeta = mock( TransformMeta.class );
    meta.setResultFilesTargetTransformMeta( resultFilesTargetTransformMeta );

    IRowMeta resultFilesOutputRowMeta = mock( IRowMeta.class );
    data.setResultFilesOutputRowMeta( resultFilesOutputRowMeta );

    doNothing().when( executor ).putRowTo( any(), any(), any() );

    executor.init();
    executor.collectExecutionResultFiles( result );

    verify( executor, never() ).putRowTo( any(), any(), any() );
  }

  // values to be grouped
  private void prepareMultipleRowsForExecutor() throws HopException {
    doReturn( new Object[] { "value1" } )
      .doReturn( new Object[] { "value1" } )
      .doReturn( new Object[] { "value1" } )
      .doReturn( new Object[] { "value1" } )
      .doReturn( new Object[] { "value12" } )
      .doReturn( new Object[] { "value12" } )
      .doReturn( new Object[] { "value12" } )
      .doReturn( null )
      .when( executor ).getRow();
  }

  private void prepareOneRowForExecutor() throws Exception {
    doReturn( new Object[] { "row" } ).doReturn( null ).when( executor ).getRow();
  }

  private TransformMeta mockTransformAndMapItToRowSet( String transformName, RowSet rowSet ) throws HopTransformException {
    TransformMeta transformMeta = mock( TransformMeta.class );
    when( transformMeta.getName() ).thenReturn( transformName );
    doReturn( rowSet ).when( executor ).findOutputRowSet( transformName );
    return transformMeta;
  }

  @Test
  //PDI-16066
  public void testExecutePipeline() throws HopException {

    String childParam = "childParam";
    String childValue = "childValue";
    String paramOverwrite = "paramOverwrite";
    String parentValue = "parentValue";

    meta.getParameters().setVariable( new String[] { childParam, paramOverwrite } );
    meta.getParameters().setInput( new String[] { null, null } );
    meta.getParameters().setField( new String[] { null, null } );
    Pipeline parent = new LocalPipelineEngine();
    Mockito.when( executor.getPipeline() ).thenReturn( parent );

    executor.init();

    executor.setVariable( paramOverwrite, parentValue );
    executor.setVariable( childParam, childValue );

    Mockito.when( executor.getLogLevel() ).thenReturn( LogLevel.NOTHING );
    parent.setLog( new LogChannel( this ) );
    Mockito.doCallRealMethod().when( executor ).createInternalPipeline();
    Mockito.when( executor.getData().getExecutorPipelineMeta().listVariables() ).thenReturn( new String[ 0 ] );
    Mockito.when( executor.getData().getExecutorPipelineMeta().listParameters() ).thenReturn( new String[ 0 ] /*{parentParam}*/ );

    Pipeline internalPipeline = executor.createInternalPipeline();
    executor.getData().setExecutorPipeline( internalPipeline );
    executor.passParametersToPipeline( Arrays.asList( meta.getParameters().getInput() ) );

    //When the child parameter does exist in the parent parameters, overwrite the child parameter by the parent parameter.
    Assert.assertEquals( parentValue, internalPipeline.getVariable( paramOverwrite ) );

    //All other parent parameters need to get copied into the child parameters  (when the 'Inherit all variables from the pipeline?' option is checked)
    Assert.assertEquals( childValue, internalPipeline.getVariable( childParam ) );
  }

  @Test
  //PDI-16066
  public void testExecutePipelineWithFieldsAndNoInput() throws HopException {
    String childParam = "childParam";
    String childValue = "childValue";
    String fieldValue1 = "fieldValue1";
    String fieldValue2 = "fieldValue2";
    String paramOverwrite = "paramOverwrite";
    String parentValue = "parentValue";

    meta.getParameters().setVariable( new String[] { childParam, paramOverwrite } );
    meta.getParameters().setInput( new String[] { null, null } );
    meta.getParameters().setField( new String[] { childParam, paramOverwrite } );
    Pipeline parent = new LocalPipelineEngine();
    Mockito.when( executor.getPipeline() ).thenReturn( parent );

    executor.init();

    executor.setVariable( paramOverwrite, parentValue );
    executor.setVariable( childParam, childValue );

    IRowMeta inputRowMeta = mock( IRowMeta.class );

    Mockito.when( executor.getLogLevel() ).thenReturn( LogLevel.NOTHING );
    parent.setLog( new LogChannel( this ) );
    Mockito.doCallRealMethod().when( executor ).createInternalPipeline();
    Mockito.when( executor.getData().getExecutorPipelineMeta().listVariables() ).thenReturn( new String[ 0 ] );
    Mockito.when( executor.getData().getExecutorPipelineMeta().listParameters() ).thenReturn( new String[ 0 ] /*{parentParam}*/ );

    executor.getData().setInputRowMeta( inputRowMeta );
    Mockito.when( executor.getData().getInputRowMeta().getFieldNames() ).thenReturn( new String[] { "childParam", "paramOverwrite" } );

    Pipeline internalPipeline = executor.createInternalPipeline();
    executor.getData().setExecutorPipeline( internalPipeline );
    executor.passParametersToPipeline( Arrays.asList( new String[] { fieldValue1, fieldValue2 } ) );

    //When the child parameter does exist in the parent parameters, overwrite the child parameter by the parent parameter.
    Assert.assertEquals( fieldValue2, internalPipeline.getVariable( paramOverwrite ) );

    //All other parent parameters need to get copied into the child parameters  (when the 'Inherit all variables from the pipeline?' option is checked)
    Assert.assertEquals( fieldValue1, internalPipeline.getVariable( childParam ) );
  }


  @Test
  //PDI-16066
  public void testExecutePipelineWithInputsAndNoFields() throws HopException {

    String childParam = "childParam";
    String childValue = "childValue";
    String inputValue1 = "inputValue1";
    String inputValue2 = "inputValue2";
    String paramOverwrite = "paramOverwrite";
    String parentValue = "parentValue";

    meta.getParameters().setVariable( new String[] { childParam, paramOverwrite } );
    meta.getParameters().setInput( new String[] { inputValue1, inputValue2 } );
    meta.getParameters().setField( new String[] { null, null } );
    Pipeline parent = new LocalPipelineEngine();
    Mockito.when( executor.getPipeline() ).thenReturn( parent );

    executor.init();

    executor.setVariable( paramOverwrite, parentValue );
    executor.setVariable( childParam, childValue );

    Mockito.when( executor.getLogLevel() ).thenReturn( LogLevel.NOTHING );
    parent.setLog( new LogChannel( this ) );
    Mockito.doCallRealMethod().when( executor ).createInternalPipeline();
    Mockito.when( executor.getData().getExecutorPipelineMeta().listVariables() ).thenReturn( new String[ 0 ] );
    Mockito.when( executor.getData().getExecutorPipelineMeta().listParameters() ).thenReturn( new String[ 0 ] /*{parentParam}*/ );

    Pipeline internalPipeline = executor.createInternalPipeline();
    executor.getData().setExecutorPipeline( internalPipeline );
    executor.passParametersToPipeline( Arrays.asList( meta.getParameters().getField() ) );

    //When the child parameter does exist in the parent parameters, overwrite the child parameter by the parent parameter.
    Assert.assertEquals( inputValue2, internalPipeline.getVariable( paramOverwrite ) );

    //All other parent parameters need to get copied into the child parameters  (when the 'Inherit all variables from the pipeline?' option is checked)
    Assert.assertEquals( inputValue1, internalPipeline.getVariable( childParam ) );
  }


  @Test
  public void testSafeStop() throws Exception {
    prepareOneRowForExecutor();
    meta.setGroupSize( "1" );
    data.groupSize = 1;

    internalResult.setSafeStop( true );

    executor.init();
    executor.setInputRowMeta( new RowMeta() );
    assertTrue( executor.init();
    verify( executor.getPipeline() ).safeStop();
    verify( executor.getPipeline(), never() ).stopAll();
  }

  @Test
  public void testAbortWithError() throws Exception {
    prepareOneRowForExecutor();
    meta.setGroupSize( "1" );
    data.groupSize = 1;

    internalResult.setSafeStop( false );
    internalResult.setNrErrors( 1 );

    executor.init();
    executor.setInputRowMeta( new RowMeta() );
    assertTrue( executor.init();
    verify( executor.getPipeline(), never() ).safeStop();
    verify( executor.getPipeline(), never() ).stopAll();
  }

  private void prepareNoRowForExecutor() throws Exception {
    doReturn( null ).when( executor ).getRow();
  }

  @Test
  public void testGetLastIncomingFieldValuesWithEmptyData() throws Exception {
    prepareNoRowForExecutor();

    executor.init();
    executor.init();
    verify( executor, times( 0 ) ).getLastIncomingFieldValues();
  }

  @Test
  public void testGetLastIncomingFieldValuesWithData() throws HopException {
    prepareMultipleRowsForExecutor();

    meta.setGroupField( "groupField" );
    executor.init();

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaString( "groupField" ) );
    executor.setInputRowMeta( rowMeta );

    // start processing
    executor.init(); // 1st row - 'value1'
    executor.init();
    executor.init();
    executor.init(); // 4th row - still 'value1'

    // same group, zero calls
    verify( executor, times( 0 ) ).getLastIncomingFieldValues();

    executor.init(); // 5th row - value has been changed - 'value12'

    // group changed - 1 calls = 1 for pipeline execution
    verify( executor, times( 1 ) ).getLastIncomingFieldValues();

    executor.init(); // 6th row - 'value12'
    executor.init(); // 7th row - 'value12'
    executor.init(); // end of file

    //  No more rows = + 1 call to get the previous value
    verify( executor, times( 2 ) ).getLastIncomingFieldValues();

  }
}
