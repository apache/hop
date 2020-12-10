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

package org.apache.hop.pipeline.transforms.databasejoin;

import org.apache.hop.core.database.Database;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
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

public class DatabaseJoinTest {

  DatabaseJoinMeta mockTransformMetaInterface;
  DatabaseJoinData mockTransformDataInterface;
  DatabaseJoin mockDatabaseJoin;

  @Before
  public void setUp() {

    TransformMeta mockTransformMeta = mock( TransformMeta.class );
    PipelineMeta mockPipelineMeta = mock( PipelineMeta.class );
    Pipeline mockPipeline = spy( new LocalPipelineEngine() );
    TransformPartitioningMeta mockTransformPartitioningMeta = mock( TransformPartitioningMeta.class );

    when( mockTransformMeta.getName() ).thenReturn( "MockTransform" );
    when( mockPipelineMeta.findTransform( anyString() ) ).thenReturn( mockTransformMeta );
    when( mockTransformMeta.getTargetTransformPartitioningMeta() ).thenReturn( mockTransformPartitioningMeta );

    mockTransformMetaInterface = mock( DatabaseJoinMeta.class, withSettings().extraInterfaces( ITransformMeta.class ) );
    mockTransformDataInterface = mock( DatabaseJoinData.class, withSettings().extraInterfaces( ITransformMeta.class ) );
    mockTransformDataInterface.db = mock( Database.class );
    mockTransformDataInterface.pstmt = mock( PreparedStatement.class );
    mockDatabaseJoin = spy( new DatabaseJoin( mockTransformMeta, mockTransformMetaInterface, mockTransformDataInterface, 1, mockPipelineMeta, mockPipeline ) );
  }

  @Test
  public void testStopRunningWhenTransformIsStopped() throws HopException {
    doReturn( true ).when( mockDatabaseJoin ).isStopped();

    mockDatabaseJoin.stopRunning();

    verify( mockDatabaseJoin, times( 1 ) ).isStopped();
    verify( mockTransformDataInterface, times( 0 ) ).isDisposed();
  }

  @Test
  public void testStopRunningWhenTransformDataInterfaceIsDisposed() throws HopException {
    doReturn( false ).when( mockDatabaseJoin ).isStopped();
    doReturn( true ).when( mockTransformDataInterface ).isDisposed();

    mockDatabaseJoin.stopRunning();

    verify( mockDatabaseJoin, times( 1 ) ).isStopped();
    verify( mockTransformDataInterface, times( 1 ) ).isDisposed();
  }

  @Test
  public void testStopRunningWhenTransformIsNotStoppedNorTransformDataInterfaceIsDisposedAndDatabaseConnectionIsValid() throws HopException {
    doReturn( false ).when( mockDatabaseJoin ).isStopped();
    doReturn( false ).when( mockTransformDataInterface ).isDisposed();
    when( mockTransformDataInterface.db.getConnection() ).thenReturn( mock( Connection.class ) );

    mockDatabaseJoin.stopRunning();

    verify( mockDatabaseJoin, times( 1 ) ).isStopped();
    verify( mockTransformDataInterface, times( 1 ) ).isDisposed();
    verify( mockTransformDataInterface.db, times( 1 ) ).getConnection();
    verify( mockTransformDataInterface.db, times( 1 ) ).cancelStatement( any( PreparedStatement.class ) );
    assertTrue( mockTransformDataInterface.isCanceled );

  }

  @Test
  public void testStopRunningWhenTransformIsNotStoppedNorTransformDataInterfaceIsDisposedAndDatabaseConnectionIsNotValid() throws HopException {
    doReturn( false ).when( mockDatabaseJoin ).isStopped();
    doReturn( false ).when( mockTransformDataInterface ).isDisposed();
    when( mockTransformDataInterface.db.getConnection() ).thenReturn( null );

    mockDatabaseJoin.stopRunning();

    verify( mockDatabaseJoin, times( 1 ) ).isStopped();
    verify( mockTransformDataInterface, times( 1 ) ).isDisposed();
    verify( mockTransformDataInterface.db, times( 1 ) ).getConnection();
    verify( mockTransformDataInterface.db, times( 0 ) ).cancelStatement( any( PreparedStatement.class ) );
    assertFalse( mockTransformDataInterface.isCanceled );
  }
}
