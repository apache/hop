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
package org.apache.hop.pipeline.transforms.normaliser;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.BeforeClass;
import org.junit.ClassRule;


import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class NormaliserTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @BeforeClass
  public static void before() throws HopException {
    HopEnvironment.init();
  }

  private NormaliserMeta.NormaliserField[] getTestNormaliserFieldsWiki() {
    NormaliserMeta.NormaliserField[] rtn = new NormaliserMeta.NormaliserField[ 6 ];
    rtn[ 0 ] = new NormaliserMeta.NormaliserField();
    rtn[ 0 ].setName( "pr_sl" );
    rtn[ 0 ].setNorm( "Product Sales" );
    rtn[ 0 ].setValue( "Product1" ); // Type

    rtn[ 1 ] = new NormaliserMeta.NormaliserField();
    rtn[ 1 ].setName( "pr1_nr" );
    rtn[ 1 ].setNorm( "Product Number" );
    rtn[ 1 ].setValue( "Product1" );

    rtn[ 2 ] = new NormaliserMeta.NormaliserField();
    rtn[ 2 ].setName( "pr2_sl" );
    rtn[ 2 ].setNorm( "Product Sales" );
    rtn[ 2 ].setValue( "Product2" );

    rtn[ 3 ] = new NormaliserMeta.NormaliserField();
    rtn[ 3 ].setName( "pr2_nr" );
    rtn[ 3 ].setNorm( "Product Number" );
    rtn[ 3 ].setValue( "Product2" );

    rtn[ 4 ] = new NormaliserMeta.NormaliserField();
    rtn[ 4 ].setName( "pr3_sl" );
    rtn[ 4 ].setNorm( "Product Sales" );
    rtn[ 4 ].setValue( "Product3" );

    rtn[ 5 ] = new NormaliserMeta.NormaliserField();
    rtn[ 5 ].setName( "pr3_nr" );
    rtn[ 5 ].setNorm( "Product Number" );
    rtn[ 5 ].setValue( "Product3" );

    return rtn;
  }

  private List<RowMetaAndData> getExpectedWikiOutputRowMetaAndData() {
    final Date theDate = new  Date( 103, 01, 01 );
    List<RowMetaAndData> list = new ArrayList<>();
    IRowMeta rm = new RowMeta();
    rm.addValueMeta( new ValueMetaDate( "DATE" ) );
    rm.addValueMeta( new ValueMetaString( "Type" ) );
    rm.addValueMeta( new ValueMetaInteger( "Product Sales" ) );
    rm.addValueMeta( new ValueMetaInteger( "Product Number" ) );
    Object[] row = new Object[ 4 ];
    row[ 0 ] = theDate;
    row[ 1 ] = "Product1";
    row[ 2 ] = 100;
    row[ 3 ] = 5;
    list.add( new RowMetaAndData( rm, row ) );

    row = new Object[ 4 ];
    row[ 0 ] = theDate;
    row[ 1 ] = "Product2";
    row[ 2 ] = 250;
    row[ 3 ] = 10;
    list.add( new RowMetaAndData( rm, row ) );

    row = new Object[ 4 ];
    row[ 0 ] = theDate;
    row[ 1 ] = "Product3";
    row[ 2 ] = 150;
    row[ 3 ] = 4;
    list.add( new RowMetaAndData( rm, row ) );
    return list;
  }


  private List<RowMetaAndData> getWikiInputRowMetaAndData() {
    List<RowMetaAndData> list = new ArrayList<>();
    Object[] row = new Object[ 7 ];
    IRowMeta rm = new RowMeta();
    rm.addValueMeta( new ValueMetaDate( "DATE" ) );
    row[ 0 ] = new Date( 103, 01, 01 );
    rm.addValueMeta( new ValueMetaInteger( "PR1_NR" ) );
    row[ 1 ] = 5;
    rm.addValueMeta( new ValueMetaInteger( "PR_SL" ) );
    row[ 2 ] = 100;
    rm.addValueMeta( new ValueMetaInteger( "PR2_NR" ) );
    row[ 3 ] = 10;
    rm.addValueMeta( new ValueMetaInteger( "PR2_SL" ) );
    row[ 4 ] = 250;
    rm.addValueMeta( new ValueMetaInteger( "PR3_NR" ) );
    row[ 5 ] = 4;
    rm.addValueMeta( new ValueMetaInteger( "PR3_SL" ) );
    row[ 6 ] = 150;
    list.add( new RowMetaAndData( rm, row ) );
    return list;
  }

  private void checkResults( List<RowMetaAndData> expectedOutput, List<RowMetaAndData> outputList ) {
    assertEquals( expectedOutput.size(), outputList.size() );
    for ( int i = 0; i < outputList.size(); i++ ) {
      RowMetaAndData aRowMetaAndData = outputList.get( i );
      RowMetaAndData expectedRowMetaAndData = expectedOutput.get( i );
      IRowMeta rowMeta = aRowMetaAndData.getRowMeta();
      IRowMeta expectedRowMeta = expectedRowMetaAndData.getRowMeta();
      String[] fields = rowMeta.getFieldNames();
      String[] expectedFields = expectedRowMeta.getFieldNames();
      assertEquals( expectedFields.length, fields.length );
      assertArrayEquals( expectedFields, fields );
      Object[] aRow = aRowMetaAndData.getData();
      Object[] expectedRow = expectedRowMetaAndData.getData();
      assertEquals( expectedRow.length, aRow.length );
      assertArrayEquals( expectedRow, aRow );
    }
  }

 // @Test
  public void testNormaliserProcessRowsWikiData() throws Exception {
    // We should have 1 row as input to the normaliser and 3 rows as output to the normaliser with the data
    //
    // Data input looks like this:
    //
    // DATE     PR1_NR  PR_SL PR2_NR  PR2_SL  PR3_NR  PR3_SL
    // 2003010  5       100   10      250     4       150
    //
    // Data output looks like this:
    //
    // DATE     Type      Product Sales Product Number
    // 2003010  Product1  100           5
    // 2003010  Product2  250           10
    // 2003010  Product3  150           4
    //


//    final String transformName = "Row Normaliser";
//    NormaliserMeta transformMeta = new NormaliserMeta();
//    transformMeta.setDefault();
//    transformMeta.setNormaliserFields( getTestNormaliserFieldsWiki() );
//    transformMeta.setTypeField( "Type" );

//    PipelineMeta pipelineMeta = PipelineTestFactory.generateTestTransformation( null, transformMeta, transformName );
//    List<RowMetaAndData> inputList = getWikiInputRowMetaAndData();
//    List<RowMetaAndData> outputList = PipelineTestFactory.executeTestTransformation( pipelineMeta, PipelineTestFactory.INJECTOR_TRANSFORMNAME, transformName, PipelineTestFactory.DUMMY_TRANSFORMNAME, inputList );
//    List<RowMetaAndData> expectedOutput = this.getExpectedWikiOutputRowMetaAndData();
//    checkResults( expectedOutput, outputList );
  }

}
