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

package org.apache.hop.pipeline.transforms.constant;

import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConstantTest {

  private TransformMockHelper<ConstantMeta, ConstantData> mockHelper;
  private RowMetaAndData rowMetaAndData = mock( RowMetaAndData.class );
  private Constant constantSpy;

  @ClassRule
  public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @BeforeClass
  public static void setUpBeforeClass() throws HopPluginException {
    ValueMetaPluginType.getInstance().searchPlugins();
  }

  @Before
  public void setUp() throws Exception {

    mockHelper = new TransformMockHelper<>( "Add Constants", ConstantMeta.class, ConstantData.class );
    when( mockHelper.logChannelFactory.create( any(), any( ILoggingObject.class ) ) ).thenReturn(
      mockHelper.iLogChannel );
    when( mockHelper.pipeline.isRunning() ).thenReturn( true );

    doReturn( rowMetaAndData ).when( mockHelper.iTransformData ).getConstants();
    constantSpy = Mockito.spy( new Constant( mockHelper.transformMeta, mockHelper.iTransformMeta, mockHelper.iTransformData, 0,
      mockHelper.pipelineMeta, mockHelper.pipeline ) );
  }

  @After
  public void tearDown() throws Exception {
    mockHelper.cleanUp();
  }

  @Test
  public void testProcessRowSuccess() throws Exception {

    doReturn( new Object[ 1 ] ).when( constantSpy ).getRow();
    doReturn( new RowMeta() ).when( constantSpy ).getInputRowMeta();
    doReturn( new Object[ 1 ] ).when( rowMetaAndData ).getData();

    boolean success = constantSpy.processRow();
    assertTrue( success );
  }

  @Test
  public void testProcessRow_fail() throws Exception {

    doReturn( null ).when( constantSpy ).getRow();
    doReturn( null ).when( constantSpy ).getInputRowMeta();

    boolean success = constantSpy.processRow();
    assertFalse( success );
  }
}
