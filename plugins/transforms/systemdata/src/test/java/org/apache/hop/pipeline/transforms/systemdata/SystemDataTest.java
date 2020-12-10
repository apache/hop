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

package org.apache.hop.pipeline.transforms.systemdata;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * User: Dzmitry Stsiapanau Date: 1/20/14 Time: 12:12 PM
 */
public class SystemDataTest {
  private class SystemDataHandler extends SystemData {

    Object[] row = new Object[] { "anyData" };
    Object[] outputRow;

    public SystemDataHandler(TransformMeta transformMeta, SystemDataMeta meta, SystemDataData data, int copyNr, PipelineMeta pipelineMeta,
                             Pipeline pipeline ) {
      super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
    }

    @SuppressWarnings( "unused" )
    public void setRow( Object[] row ) {
      this.row = row;
    }

    /**
     * In case of getRow, we receive data from previous transforms through the input rowset. In case we split the stream, we
     * have to copy the data to the alternate splits: rowsets 1 through n.
     */
    @Override
    public Object[] getRow() throws HopException {
      return row;
    }

    /**
     * putRow is used to copy a row, to the alternate rowset(s) This should get priority over everything else!
     * (synchronized) If distribute is true, a row is copied only once to the output rowsets, otherwise copies are sent
     * to each rowset!
     *
     * @param row The row to put to the destination rowset(s).
     * @throws HopTransformException
     */
    @Override
    public void putRow( IRowMeta rowMeta, Object[] row ) throws HopTransformException {
      outputRow = row;
    }

    public Object[] getOutputRow() {
      return outputRow;
    }

  }

  private TransformMockHelper<SystemDataMeta, SystemDataData> transformMockHelper;

  @Before
  public void setUp() throws Exception {
    transformMockHelper =
      new TransformMockHelper<>( "SYSTEM_DATA TEST", SystemDataMeta.class,
        SystemDataData.class );
    when( transformMockHelper.logChannelFactory.create( any(), any( ILoggingObject.class ) ) ).thenReturn(
      transformMockHelper.iLogChannel );
    when( transformMockHelper.pipeline.isRunning() ).thenReturn( true );
    verify( transformMockHelper.pipeline, never() ).stopAll();
  }

  @After
  public void tearDown() throws Exception {
    transformMockHelper.cleanUp();
  }

  @Test
  @Ignore
  public void testProcessRow() throws Exception {
    SystemDataData systemDataData = new SystemDataData();
    SystemDataMeta systemDataMeta = new SystemDataMeta();
    systemDataMeta.allocate( 2 );
    String[] names = systemDataMeta.getFieldName();
    SystemDataTypes[] types = systemDataMeta.getFieldType();
    names[ 0 ] = "hostname";
    names[ 1 ] = "hostname_real";
    types[ 0 ] = SystemDataTypes.getTypeFromString( SystemDataTypes.TYPE_SYSTEM_INFO_HOSTNAME.getDescription() );
    types[ 1 ] = SystemDataTypes.getTypeFromString( SystemDataTypes.TYPE_SYSTEM_INFO_HOSTNAME_REAL.getDescription() );
    SystemDataHandler systemData =
      new SystemDataHandler( transformMockHelper.transformMeta, transformMockHelper.iTransformMeta, transformMockHelper.iTransformData, 0, transformMockHelper.pipelineMeta,
        transformMockHelper.pipeline );
    Object[] expectedRow = new Object[] { Const.getHostname(), Const.getHostnameReal() };
    IRowMeta inputRowMeta = mock( IRowMeta.class );
    when( inputRowMeta.clone() ).thenReturn( inputRowMeta );
    when( inputRowMeta.size() ).thenReturn( 2 );
    systemDataData.outputRowMeta = inputRowMeta;
    systemData.init();
    assertFalse( systemData.processRow());
    Object[] out = systemData.getOutputRow();
    assertArrayEquals( expectedRow, out );
  }
}
