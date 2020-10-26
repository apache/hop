/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.pipeline.transforms.tableinput;

import org.apache.hop.core.database.Database;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformPartitioningMeta;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import static org.powermock.api.mockito.PowerMockito.spy;

public class TableInputTest {

  TableInputMeta mockITransform;
  TableInputData mockTransformDataInterface;
  TableInput mockTableInput;

  @Before
  public void setUp() {

    TransformMeta mockTransformMeta = mock( TransformMeta.class );
    PipelineMeta mockPipelineMeta = mock( PipelineMeta.class );
    Pipeline mockPipeline = mock( Pipeline.class );
    TransformPartitioningMeta mockTransformPartitioningMeta = mock( TransformPartitioningMeta.class );

    when( mockTransformMeta.getName() ).thenReturn( "MockTransform" );
    when( mockPipelineMeta.findTransform( anyString() ) ).thenReturn( mockTransformMeta );
    when( mockTransformMeta.getTargetTransformPartitioningMeta() ).thenReturn( mockTransformPartitioningMeta );

    mockITransform = mock( TableInputMeta.class, withSettings().extraInterfaces( ITransform.class ) );
    mockTransformDataInterface = mock( TableInputData.class, withSettings().extraInterfaces( ITransform.class ) );
    mockTransformDataInterface.db = mock( Database.class );
    mockTableInput = spy( new TableInput( mockTransformMeta, mockTransformDataInterface, 1, mockPipelineMeta, mockPipeline ) );
  }

  @Test
  public void testStopRunningWhenTransformIsStopped() throws HopException {
    doReturn( true ).when( mockTableInput ).isStopped();

    mockTableInput.stopRunning( mockITransform, mockTransformDataInterface );

    verify( mockTableInput, times( 1 ) ).isStopped();
    verify( mockTransformDataInterface, times( 0 ) ).isDisposed();
  }

  @Test
  public void testStopRunningWhenTransformDataInterfaceIsDisposed() throws HopException {
    doReturn( false ).when( mockTableInput ).isStopped();
    doReturn( true ).when( mockTransformDataInterface ).isDisposed();

    mockTableInput.stopRunning( mockITransform, mockTransformDataInterface );

    verify( mockTableInput, times( 1 ) ).isStopped();
    verify( mockTransformDataInterface, times( 1 ) ).isDisposed();
  }

  @Test
  public void testStopRunningWhenTransformIsNotStoppedNorTransformDataInterfaceIsDisposedAndDatabaseConnectionIsValid() throws HopException {
    doReturn( false ).when( mockTableInput ).isStopped();
    doReturn( false ).when( mockTransformDataInterface ).isDisposed();
    when( mockTransformDataInterface.db.getConnection() ).thenReturn( mock( Connection.class ) );

    mockTableInput.stopRunning( mockITransform, mockTransformDataInterface );

    verify( mockTableInput, times( 1 ) ).isStopped();
    verify( mockTransformDataInterface, times( 1 ) ).isDisposed();
    verify( mockTransformDataInterface.db, times( 1 ) ).getConnection();
    verify( mockTransformDataInterface.db, times( 1 ) ).cancelQuery();
    assertTrue( mockTransformDataInterface.isCanceled );

  }

  @Test
  public void testStopRunningWhenTransformIsNotStoppedNorTransformDataInterfaceIsDisposedAndDatabaseConnectionIsNotValid() throws HopException {
    doReturn( false ).when( mockTableInput ).isStopped();
    doReturn( false ).when( mockTransformDataInterface ).isDisposed();
    when( mockTransformDataInterface.db.getConnection() ).thenReturn( null );

    mockTableInput.stopRunning( mockITransform, mockTransformDataInterface );

    verify( mockTableInput, times( 1 ) ).isStopped();
    verify( mockTransformDataInterface, times( 1 ) ).isDisposed();
    verify( mockTransformDataInterface.db, times( 1 ) ).getConnection();
    verify( mockTransformDataInterface.db, times( 0 ) ).cancelStatement( any( PreparedStatement.class ) );
    assertFalse( mockTransformDataInterface.isCanceled );
  }
}
