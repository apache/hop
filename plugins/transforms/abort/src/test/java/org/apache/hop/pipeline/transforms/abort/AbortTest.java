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

package org.apache.hop.pipeline.transforms.abort;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LoggingObjectInterface;
import org.apache.hop.pipeline.transform.TransformDataInterface;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AbortTest {
  private TransformMockHelper<AbortMeta, TransformDataInterface> transformMockHelper;

  @Before
  public void setup() {
    transformMockHelper = new TransformMockHelper<>( "ABORT TEST", AbortMeta.class, TransformDataInterface.class );
    when( transformMockHelper.logChannelInterfaceFactory.create( any(), any( LoggingObjectInterface.class ) ) )
      .thenReturn( transformMockHelper.logChannelInterface );
    when( transformMockHelper.pipeline.isRunning() ).thenReturn( true );
  }

  @After
  public void tearDown() {
    transformMockHelper.cleanUp();
  }

  @Test
  public void testAbortDoesntAbortWithoutInputRow() throws HopException {
    Abort abort =
      new Abort(
        transformMockHelper.transformMeta, transformMockHelper.transformDataInterface, 0, transformMockHelper.pipelineMeta,
        transformMockHelper.pipeline );
    abort.init( transformMockHelper.initTransformMetaInterface, transformMockHelper.initTransformDataInterface );
    abort.addRowSetToInputRowSets( transformMockHelper.getMockInputRowSet() );
    assertFalse( abort.isStopped() );
    abort.processRow( transformMockHelper.processRowsTransformMetaInterface, transformMockHelper.processRowsTransformDataInterface );
    verify( transformMockHelper.pipeline, never() ).stopAll();
    assertFalse( abort.isStopped() );
  }

  @Test
  public void testAbortAbortsWithInputRow() throws HopException {
    Abort abort =
      new Abort(
        transformMockHelper.transformMeta, transformMockHelper.transformDataInterface, 0, transformMockHelper.pipelineMeta,
        transformMockHelper.pipeline );
    abort.init( transformMockHelper.initTransformMetaInterface, transformMockHelper.initTransformDataInterface );
    abort.addRowSetToInputRowSets( transformMockHelper.getMockInputRowSet( new Object[] {} ) );
    assertFalse( abort.isStopped() );
    abort.processRow( transformMockHelper.processRowsTransformMetaInterface, transformMockHelper.processRowsTransformDataInterface );
    verify( transformMockHelper.pipeline, times( 1 ) ).stopAll();
    assertTrue( abort.isStopped() );
  }

  @Test
  public void testSafeStop() throws HopException {
    Abort abort =
      new Abort(
        transformMockHelper.transformMeta, transformMockHelper.transformDataInterface, 0, transformMockHelper.pipelineMeta,
        transformMockHelper.pipeline );
    when( transformMockHelper.processRowsTransformMetaInterface.isSafeStop() ).thenReturn( true );
    abort.init( transformMockHelper.initTransformMetaInterface, transformMockHelper.initTransformDataInterface );
    abort.addRowSetToInputRowSets( transformMockHelper.getMockInputRowSet( new Object[] {} ) );
    abort.processRow( transformMockHelper.processRowsTransformMetaInterface, transformMockHelper.processRowsTransformDataInterface );
    verify( transformMockHelper.pipeline ).safeStop();
  }

  @Test
  public void testAbortWithError() throws HopException {
    Abort abort =
      new Abort(
        transformMockHelper.transformMeta, transformMockHelper.transformDataInterface, 0, transformMockHelper.pipelineMeta,
        transformMockHelper.pipeline );
    when( transformMockHelper.processRowsTransformMetaInterface.isSafeStop() ).thenReturn( false );
    when( transformMockHelper.processRowsTransformMetaInterface.isAbortWithError() ).thenReturn( true );
    abort.init( transformMockHelper.initTransformMetaInterface, transformMockHelper.initTransformDataInterface );
    abort.addRowSetToInputRowSets( transformMockHelper.getMockInputRowSet( new Object[] {} ) );
    abort.processRow( transformMockHelper.processRowsTransformMetaInterface, transformMockHelper.processRowsTransformDataInterface );
    assertEquals( 1L, abort.getErrors() );
    verify( transformMockHelper.pipeline ).stopAll();
  }
}
