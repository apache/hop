/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.pipeline.transforms.mappinginput;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LoggingObjectInterface;
import org.apache.hop.pipeline.transform.TransformErrorMeta;
import org.apache.hop.pipeline.transform.TransformInterface;
import org.apache.hop.pipeline.transforms.mapping.MappingValueRename;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.apache.hop.pipeline.transforms.validator.Validator;
import org.apache.hop.pipeline.transforms.validator.ValidatorData;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * User: Dzmitry Stsiapanau Date: 12/24/13 Time: 12:45 PM
 */
public class MappingInputTest {
  private String transformName = "MAPPING INPUT";
  private TransformMockHelper<MappingInputMeta, MappingInputData> transformMockHelper;
  private volatile boolean processRowEnded;

  @Before
  public void setUp() throws Exception {
    transformMockHelper =
      new TransformMockHelper<MappingInputMeta, MappingInputData>( transformName, MappingInputMeta.class,
        MappingInputData.class );
    when( transformMockHelper.logChannelInterfaceFactory.create( any(), any( LoggingObjectInterface.class ) ) ).thenReturn(
      transformMockHelper.logChannelInterface );
    // when( transformMockHelper.pipeline.isRunning() ).thenReturn( true );
    setProcessRowEnded( false );
  }

  @After
  public void tearDown() throws Exception {
    transformMockHelper.cleanUp();
  }

  @Test
  public void testSetConnectorTransforms() throws Exception {

    when( transformMockHelper.pipelineMeta.getSizeRowset() ).thenReturn( 1 );
    MappingInputData mappingInputData = new MappingInputData();
    MappingInput mappingInput =
      new MappingInput( transformMockHelper.transformMeta, mappingInputData,
        0, transformMockHelper.pipelineMeta, transformMockHelper.pipeline );
    mappingInput.init( transformMockHelper.initTransformMetaInterface, mappingInputData );
    ValidatorData validatorData = new ValidatorData();
    Validator previousTransform =
      new Validator( transformMockHelper.transformMeta, validatorData, 0, transformMockHelper.pipelineMeta, transformMockHelper.pipeline );
    when( transformMockHelper.transformMeta.isDoingErrorHandling() ).thenReturn( true );
    TransformErrorMeta transformErrorMeta = mock( TransformErrorMeta.class );
    when( transformErrorMeta.getTargetTransform() ).thenReturn( transformMockHelper.transformMeta );
    when( transformMockHelper.transformMeta.getName() ).thenReturn( transformName );
    when( transformMockHelper.transformMeta.getTransformErrorMeta() ).thenReturn( transformErrorMeta );

    TransformInterface[] si = new TransformInterface[] { previousTransform };
    mappingInput.setConnectorTransforms( si, Collections.<MappingValueRename>emptyList(), transformName );
    assertEquals( previousTransform.getOutputRowSets().size(), 0 );

  }

  @Test
  public void testSetConnectorTransformsWithNullArguments() throws Exception {
    try {
      final MappingInputData mappingInputData = new MappingInputData();
      final MappingInput mappingInput =
        new MappingInput( transformMockHelper.transformMeta, mappingInputData, 0, transformMockHelper.pipelineMeta,
          transformMockHelper.pipeline );
      mappingInput.init( transformMockHelper.initTransformMetaInterface, mappingInputData );
      int timeOut = 1000;
      final int junitMaxTimeOut = 40000;
      mappingInput.setTimeOut( timeOut );
      final MappingInputTest mit = this;
      final Thread processRow = new Thread( new Runnable() {
        @Override
        public void run() {
          try {
            mappingInput.processRow( transformMockHelper.initTransformMetaInterface, mappingInputData );
            mit.setProcessRowEnded( true );
          } catch ( HopException e ) {
            mit.setProcessRowEnded( true );
          }
        }
      } );
      processRow.start();
      boolean exception = false;
      try {
        mappingInput.setConnectorTransforms( null, Collections.<MappingValueRename>emptyList(), "" );
      } catch ( IllegalArgumentException ex1 ) {
        try {
          mappingInput.setConnectorTransforms( new TransformInterface[ 0 ], null, "" );
        } catch ( IllegalArgumentException ex3 ) {
          try {
            mappingInput.setConnectorTransforms( new TransformInterface[] { mock( TransformInterface.class ) },
              Collections.<MappingValueRename>emptyList(), null );
          } catch ( IllegalArgumentException ignored ) {
            exception = true;
          }
        }

      }
      processRow.join( junitMaxTimeOut );
      assertTrue( "not enough IllegalArgumentExceptions", exception );
      assertTrue( "Process wasn`t stopped at null", isProcessRowEnded() );
    } catch ( NullPointerException npe ) {
      fail( "Null values are not suitable" );
    }
  }

  public void setProcessRowEnded( boolean processRowEnded ) {
    this.processRowEnded = processRowEnded;
  }

  public boolean isProcessRowEnded() {
    return processRowEnded;
  }
}
