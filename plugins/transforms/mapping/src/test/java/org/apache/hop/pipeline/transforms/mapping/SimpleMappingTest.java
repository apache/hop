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
package org.apache.hop.pipeline.transforms.mapping;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.RowProducer;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transforms.input.MappingInput;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.apache.hop.pipeline.transforms.output.MappingOutput;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Tatsiana_Kasiankova
 */
public class SimpleMappingTest {

  private static final String MAPPING_INPUT_TRANSFORM_NAME = "MAPPING_INPUT_TRANSFORM_NAME";

  private static final String MAPPING_OUTPUT_TRANSFORM_NAME = "MAPPING_OUTPUT_TRANSFORM_NAME";

  private TransformMockHelper<SimpleMappingMeta, SimpleMappingData> transformMockHelper;

  // Using real SimpleMappingData object
  private SimpleMappingData simpleMpData = new SimpleMappingData();

  private SimpleMapping smp;

  @Before
  public void setup() throws Exception {
    transformMockHelper =
      new TransformMockHelper<>( "SIMPLE_MAPPING_TEST", SimpleMappingMeta.class,
        SimpleMappingData.class );
    when( transformMockHelper.logChannelFactory.create( any(), any( ILoggingObject.class ) ) ).thenReturn(
      transformMockHelper.iLogChannel );
    when( transformMockHelper.pipeline.isRunning() ).thenReturn( true );

    // Mock for MappingInput
    MappingInput mpInputMock = mock( MappingInput.class );
    when( mpInputMock.getTransformName() ).thenReturn( MAPPING_INPUT_TRANSFORM_NAME );

    // Mock for MappingOutput
    MappingOutput mpOutputMock = mock( MappingOutput.class );
    when( mpOutputMock.getTransformName() ).thenReturn( MAPPING_OUTPUT_TRANSFORM_NAME );

    // Mock for RowDataInputMapper
    RowDataInputMapper rdInputMpMock = mock( RowDataInputMapper.class );
    IRowMeta rwMetaInMock = mock( RowMeta.class );
    doReturn( Boolean.TRUE ).when( rdInputMpMock ).putRow( rwMetaInMock, new Object[] {} );

    // Mock for RowProducer
    RowProducer rProducerMock = mock( RowProducer.class );
    when( rProducerMock.putRow( any( IRowMeta.class ), any( Object[].class ), anyBoolean() ) )
      .thenReturn( true );

    // Mock for MappingIODefinition
    MappingIODefinition mpIODefMock = mock( MappingIODefinition.class );

    // Set up real SimpleMappingData with some mocked elements
    simpleMpData.mappingInput = mpInputMock;
    simpleMpData.mappingOutput = mpOutputMock;
    simpleMpData.rowDataInputMapper = rdInputMpMock;
    simpleMpData.mappingPipeline = transformMockHelper.pipeline;

    Mockito.doReturn( mpOutputMock ).when( transformMockHelper.pipeline ).findTransformInterface( MAPPING_OUTPUT_TRANSFORM_NAME, 0 );
    Mockito.doReturn(rProducerMock).when( transformMockHelper.pipeline).addRowProducer( MAPPING_INPUT_TRANSFORM_NAME, 0 );

    when( transformMockHelper.iTransformMeta.getInputMapping() ).thenReturn( mpIODefMock );
  }

  @After
  public void cleanUp() {
    transformMockHelper.cleanUp();
  }

  @Ignore
  public void testTransformSetUpAsWasStarted_AtProcessingFirstRow() throws HopException {

    smp = new SimpleMapping( transformMockHelper.transformMeta, transformMockHelper.iTransformMeta, transformMockHelper.iTransformData, 0, transformMockHelper.pipelineMeta, transformMockHelper.pipeline );
    smp.processRow();
    smp.addRowSetToInputRowSets( transformMockHelper.getMockInputRowSet( new Object[] {} ) );
    assertTrue( "The transform is processing in first", smp.first );
    assertTrue( smp.processRow() );
    assertFalse( "The transform is processing not in first", smp.first );
    assertTrue( "The transform was started", smp.getData().wasStarted );
  }

  @Ignore
  public void testTransformShouldProcessError_WhenMappingPipelineHasError() throws HopException {

    // Set Up TransMock to return the error
    int errorCount = 1;
    when( transformMockHelper.pipeline.getErrors() ).thenReturn( errorCount );

    // The transform has been already finished
    when( transformMockHelper.pipeline.isFinished() ).thenReturn( Boolean.TRUE );
    // The transform was started
    simpleMpData.wasStarted = true;

    smp =
      new SimpleMapping( transformMockHelper.transformMeta, transformMockHelper.iTransformMeta, transformMockHelper.iTransformData, 0, transformMockHelper.pipelineMeta,
        transformMockHelper.pipeline );
    smp.init();

    smp.dispose();
    verify( transformMockHelper.pipeline, times( 1 ) ).isFinished();
    verify( transformMockHelper.pipeline, never() ).waitUntilFinished();
    verify( transformMockHelper.pipeline, never() ).addActiveSubPipeline( anyString(), any( Pipeline.class ) );
    // verify( transformMockHelper.pipeline, times( 1 ) ).removeActiveSubPipeline( anyString() );
    verify( transformMockHelper.pipeline, never() ).getActiveSubPipeline( anyString() );
    verify( transformMockHelper.pipeline, times( 1 ) ).getErrors();
    assertTrue( "The transform contains the errors", smp.getErrors() == errorCount );

  }

  @Test
  public void testTransformShouldStopProcessingInput_IfUnderlyingTransitionIsStopped() throws Exception {

    MappingInput mappingInput = mock( MappingInput.class );
    when( mappingInput.getTransformName() ).thenReturn( MAPPING_INPUT_TRANSFORM_NAME );
    transformMockHelper.iTransformData.mappingInput = mappingInput;

    RowProducer rowProducer = mock( RowProducer.class );
    when( rowProducer.putRow( any( IRowMeta.class ), any( Object[].class ), anyBoolean() ) )
      .thenReturn( true );

    ITransform iTransform = mock( ITransform.class );

    Pipeline mappingPipeline = mock( Pipeline.class );
    when( mappingPipeline.addRowProducer( anyString(), anyInt() ) ).thenReturn( rowProducer );
    when( mappingPipeline.findTransformInterface( anyString(), anyInt() ) ).thenReturn( iTransform );
    when( mappingPipeline.isFinishedOrStopped() ).thenReturn( Boolean.FALSE ).thenReturn( Boolean.TRUE );
    transformMockHelper.iTransformData.mappingPipeline = mappingPipeline;

    MappingOutput mappingOutput = mock( MappingOutput.class );
    when( mappingOutput.getTransformName() ).thenReturn( MAPPING_OUTPUT_TRANSFORM_NAME );
    transformMockHelper.iTransformData.mappingOutput = mappingOutput;

    smp = new SimpleMapping( transformMockHelper.transformMeta, transformMockHelper.iTransformMeta, transformMockHelper.iTransformData, 0, transformMockHelper.pipelineMeta,
      transformMockHelper.pipeline );
    smp.init();
    smp.addRowSetToInputRowSets( transformMockHelper.getMockInputRowSet( new Object[] {} ) );
    smp.addRowSetToInputRowSets( transformMockHelper.getMockInputRowSet( new Object[] {} ) );

    assertTrue( smp.processRow() );
    assertFalse( smp.processRow() );

  }

  @After
  public void tearDown() {
    transformMockHelper.cleanUp();
  }

  @Ignore
  public void testDispose() throws HopException {

    // Set Up TransMock to return the error
    when( transformMockHelper.pipeline.getErrors() ).thenReturn( 0 );

    // The transform has been already finished
    when( transformMockHelper.pipeline.isFinished() ).thenReturn( Boolean.FALSE );
    // The transform was started
    simpleMpData.wasStarted = true;

    smp =
      new SimpleMapping( transformMockHelper.transformMeta, transformMockHelper.iTransformMeta, transformMockHelper.iTransformData, 0, transformMockHelper.pipelineMeta,
        transformMockHelper.pipeline );
    smp.init();

    smp.dispose();
    verify( transformMockHelper.pipeline, times( 1 ) ).isFinished();
    verify( transformMockHelper.pipeline, times( 1 ) ).waitUntilFinished();
    // verify( transformMockHelper.pipeline, times( 1 ) ).removeActiveSubPipelineformation( anyString() );

  }

}
