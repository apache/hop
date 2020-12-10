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

package org.apache.hop.pipeline.transforms.setvalueconstant;

import junit.framework.Assert;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.Collections;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

/**
 * Tests for "Set field value to a constant" transform
 *
 * @author Pavel Sakun
 * @see SetValueConstant
 */
public class SetValueConstantTest {
  private TransformMockHelper<SetValueConstantMeta, SetValueConstantData> smh;

  @Before
  public void setUp() {
    smh =
      new TransformMockHelper<>( "SetValueConstant", SetValueConstantMeta.class,
        SetValueConstantData.class );
    when( smh.logChannelFactory.create( any(), any( ILoggingObject.class ) ) ).thenReturn(
      smh.iLogChannel );
  }

  @After
  public void cleanUp() {
    smh.cleanUp();
  }

  @Test
  public void testUpdateField() throws Exception {
    SetValueConstant transform = new SetValueConstant( smh.transformMeta, smh.iTransformMeta, smh.iTransformData, 0, smh.pipelineMeta, smh.pipeline );

    IValueMeta valueMeta = new ValueMetaString( "Field1" );
    valueMeta.setStorageType( IValueMeta.STORAGE_TYPE_BINARY_STRING );

    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta( valueMeta );

    SetValueConstantMeta.Field field = new SetValueConstantMeta.Field();
    field.setFieldName( "Field Name" );
    field.setEmptyString( true );
    field.setReplaceMask( "Replace Mask" );
    field.setReplaceValue( "Replace Value" );

    doReturn( Collections.singletonList( field ) ).when( smh.iTransformMeta ).getFields();
    doReturn( field ).when( smh.iTransformMeta ).getField( 0 );
    doReturn( rowMeta ).when( smh.iTransformData ).getConvertRowMeta();
    doReturn( rowMeta ).when( smh.iTransformData ).getOutputRowMeta();
    doReturn( 1 ).when( smh.iTransformData ).getFieldnr();
    doReturn( new int[] { 0 } ).when( smh.iTransformData ).getFieldnrs();
    doReturn( new String[] { "foo" } ).when( smh.iTransformData ).getRealReplaceByValues();

    transform.init();

    Method m = SetValueConstant.class.getDeclaredMethod( "updateField", Object[].class );
    m.setAccessible( true );

    Object[] row = new Object[] { null };
    m.invoke( transform, new Object[] { row } );

    Assert.assertEquals( "foo", valueMeta.getString( row[ 0 ] ) );
  }
}
