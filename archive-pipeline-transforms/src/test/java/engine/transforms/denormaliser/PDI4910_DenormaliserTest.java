/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
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

package org.apache.hop.pipeline.transforms.denormaliser;


import org.apache.hop.core.logging.LoggingObjectInterface;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

public class PDI4910_DenormaliserTest {

  private TransformMockHelper<DenormaliserMeta, DenormaliserData> mockHelper;
  private Denormaliser denormaliser;

  @Before
  public void init() {
    mockHelper = new TransformMockHelper<DenormaliserMeta, DenormaliserData>( "Denormalizer", DenormaliserMeta.class, DenormaliserData.class );
    when( mockHelper.logChannelFactory.create( any(), any( LoggingObjectInterface.class ) ) )
      .thenReturn( mockHelper.logChannelInterface );
  }

  @After
  public void cleanUp() {
    mockHelper.cleanUp();
  }

  @Test
  public void testDeNormalise() throws Exception {

    // init transform data
    DenormaliserData transformData = new DenormaliserData();
    transformData.keyFieldNr = 0;
    transformData.keyValue = new HashMap<String, List<Integer>>();
    transformData.keyValue.put( "1", Arrays.asList( new Integer[] { 0, 1 } ) );
    transformData.fieldNameIndex = new int[] { 1, 2 };
    transformData.inputRowMeta = new RowMeta();
    ValueMetaDate outDateField1 = new ValueMetaDate( "date_field[yyyy-MM-dd]" );
    ValueMetaDate outDateField2 = new ValueMetaDate( "date_field[yyyy/MM/dd]" );
    transformData.outputRowMeta = new RowMeta();
    transformData.outputRowMeta.addValueMeta( 0, outDateField1 );
    transformData.outputRowMeta.addValueMeta( 1, outDateField2 );
    transformData.removeNrs = new int[] {};
    transformData.targetResult = new Object[] { null, null };

    // init transform meta
    DenormaliserMeta transformMeta = new DenormaliserMeta();
    DenormaliserTargetField[] denormaliserTargetFields = new DenormaliserTargetField[ 2 ];
    DenormaliserTargetField targetField1 = new DenormaliserTargetField();
    DenormaliserTargetField targetField2 = new DenormaliserTargetField();
    targetField1.setTargetFormat( "yyyy-MM-dd" );
    targetField2.setTargetFormat( "yyyy/MM/dd" );
    denormaliserTargetFields[ 0 ] = targetField1;
    denormaliserTargetFields[ 1 ] = targetField2;
    transformMeta.setDenormaliserTargetField( denormaliserTargetFields );

    // init row meta
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta( 0, new ValueMetaInteger( "key" ) );
    rowMeta.addValueMeta( 1, new ValueMetaString( "stringDate1" ) );
    rowMeta.addValueMeta( 2, new ValueMetaString( "stringDate2" ) );

    // init row data
    Object[] rowData = new Object[] { 1L, "2000-10-20", "2000/10/20" };

    // init transform
    denormaliser = new Denormaliser( mockHelper.transformMeta, transformData,
      0, mockHelper.pipelineMeta, mockHelper.pipeline );

    // inject transform meta
    Field metaField = denormaliser.getClass().getDeclaredField( "meta" );
    Assert.assertNotNull( "Can't find a field 'meta' in class Denormalizer", metaField );
    metaField.setAccessible( true );
    metaField.set( denormaliser, transformMeta );

    // call tested method
    Method deNormalise =
      denormaliser.getClass().getDeclaredMethod( "deNormalise", IRowMeta.class, Object[].class );
    Assert.assertNotNull( "Can't find a method 'deNormalise' in class Denormalizer", deNormalise );
    deNormalise.setAccessible( true );
    deNormalise.invoke( denormaliser, rowMeta, rowData );

    // vefiry
    for ( Object res : transformData.targetResult ) {
      Assert.assertNotNull( "Date is null", res );
    }
  }

}
