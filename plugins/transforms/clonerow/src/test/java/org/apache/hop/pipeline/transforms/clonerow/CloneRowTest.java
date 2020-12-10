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

package org.apache.hop.pipeline.transforms.clonerow;

import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Andrey Khayrutdinov
 */
public class CloneRowTest {

  private TransformMockHelper<CloneRowMeta, CloneRowData> transformMockHelper;

  @Before
  public void setup() {
    transformMockHelper =
      new TransformMockHelper<>( "Test CloneRow", CloneRowMeta.class, CloneRowData.class );
    when( transformMockHelper.logChannelFactory.create( any(), any( ILoggingObject.class ) ) )
      .thenReturn( transformMockHelper.iLogChannel );
    when( transformMockHelper.pipeline.isRunning() ).thenReturn( true );
  }

  @After
  public void tearDown() {
    transformMockHelper.cleanUp();
  }

  @Test( expected = HopException.class )
  public void nullNrCloneField() throws Exception {
    CloneRow transform =
      new CloneRow( transformMockHelper.transformMeta, transformMockHelper.iTransformMeta, transformMockHelper.iTransformData, 0, transformMockHelper.pipelineMeta,
        transformMockHelper.pipeline );
    transform.init();

    IRowMeta inputRowMeta = mock( IRowMeta.class );
    when( inputRowMeta.getInteger( any( Object[].class ), anyInt() ) ).thenReturn( null );

    IRowSet inputRowSet = transformMockHelper.getMockInputRowSet( new Integer[] { null } );
    when( inputRowSet.getRowMeta() ).thenReturn( inputRowMeta );
    transform.setInputRowSets( Collections.singletonList( inputRowSet ) );

    when( transformMockHelper.iTransformMeta.isNrCloneInField() ).thenReturn( true );
    when( transformMockHelper.iTransformMeta.getNrCloneField() ).thenReturn( "field" );

    transform.processRow();
  }
}
