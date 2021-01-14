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

package org.apache.hop.core;

import org.apache.hop.core.injection.DefaultInjectionTypeConverter;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaInternetAddress;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.row.value.ValueMetaTimestamp;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RowMetaAndDataTest {
  RowMeta rowsMeta;
  RowMetaAndData row;

  DefaultInjectionTypeConverter converter = new DefaultInjectionTypeConverter();

  enum TestEnum {
    ONE, Two, three
  }

  @Before
  public void prepare() throws Exception {
    rowsMeta = new RowMeta();

    IValueMeta valueMetaString = new ValueMetaString( "str" );
    rowsMeta.addValueMeta( valueMetaString );

    IValueMeta valueMetaBoolean = new ValueMetaBoolean( "bool" );
    rowsMeta.addValueMeta( valueMetaBoolean );

    IValueMeta valueMetaInteger = new ValueMetaInteger( "int" );
    rowsMeta.addValueMeta( valueMetaInteger );
  }

  @Test
  public void testMergeRowAndMetaData() {
    row = new RowMetaAndData( rowsMeta, "text", true, 1 );
    RowMeta addRowMeta = new RowMeta();
    IValueMeta valueMetaString = new ValueMetaString( "str" );
    addRowMeta.addValueMeta( valueMetaString );

    RowMetaAndData addRow = new RowMetaAndData( addRowMeta, "text1" );
    row.mergeRowMetaAndData( addRow, "originName" );

    assertEquals( 4, row.size() );
    assertEquals( "text", row.getData()[ 0 ] );
    assertEquals( "text1", row.getData()[ 3 ] );
    assertEquals( "originName", row.getValueMeta( 3 ).getOrigin() );
  }

  @Test
  public void testStringConversion() throws Exception {

    row = new RowMetaAndData( rowsMeta, "text", null, null );
    assertEquals( "text", row.getAsJavaType( "str", String.class, converter ) );

    row = new RowMetaAndData( rowsMeta, "7", null, null );
    assertEquals( 7, row.getAsJavaType( "str", int.class, converter ) );
    assertEquals( 7, row.getAsJavaType( "str", Integer.class, converter ) );

    assertEquals( 7L, row.getAsJavaType( "str", long.class, converter ) );
    assertEquals( 7L, row.getAsJavaType( "str", Long.class, converter ) );

    row = new RowMetaAndData( rowsMeta, "y", null, null );
    assertEquals( true, row.getAsJavaType( "str", boolean.class, converter ) );
    assertEquals( true, row.getAsJavaType( "str", Boolean.class, converter ) );

    row = new RowMetaAndData( rowsMeta, "yes", null, null );
    assertEquals( true, row.getAsJavaType( "str", boolean.class, converter ) );
    assertEquals( true, row.getAsJavaType( "str", Boolean.class, converter ) );

    row = new RowMetaAndData( rowsMeta, "true", null, null );
    assertEquals( true, row.getAsJavaType( "str", boolean.class, converter ) );
    assertEquals( true, row.getAsJavaType( "str", Boolean.class, converter ) );

    row = new RowMetaAndData( rowsMeta, "no", null, null );
    assertEquals( false, row.getAsJavaType( "str", boolean.class, converter ) );
    assertEquals( false, row.getAsJavaType( "str", Boolean.class, converter ) );

    row = new RowMetaAndData( rowsMeta, "n", null, null );
    assertEquals( false, row.getAsJavaType( "str", boolean.class, converter ) );
    assertEquals( false, row.getAsJavaType( "str", Boolean.class, converter ) );

    row = new RowMetaAndData( rowsMeta, "false", null, null );
    assertEquals( false, row.getAsJavaType( "str", boolean.class, converter ) );
    assertEquals( false, row.getAsJavaType( "str", Boolean.class, converter ) );

    row = new RowMetaAndData( rowsMeta, "f", null, null );
    assertEquals( false, row.getAsJavaType( "str", boolean.class, converter ) );
    assertEquals( false, row.getAsJavaType( "str", Boolean.class, converter ) );

    row = new RowMetaAndData( rowsMeta, "other", null, null );
    assertEquals( false, row.getAsJavaType( "str", boolean.class, converter ) );
    assertEquals( false, row.getAsJavaType( "str", Boolean.class, converter ) );

    row = new RowMetaAndData( rowsMeta, TestEnum.ONE.name(), null, null );
    assertEquals( TestEnum.ONE, row.getAsJavaType( "str", TestEnum.class, converter ) );
    row = new RowMetaAndData( rowsMeta, TestEnum.Two.name(), null, null );
    assertEquals( TestEnum.Two, row.getAsJavaType( "str", TestEnum.class, converter ) );
    row = new RowMetaAndData( rowsMeta, TestEnum.three.name(), null, null );
    assertEquals( TestEnum.three, row.getAsJavaType( "str", TestEnum.class, converter ) );

    row = new RowMetaAndData( rowsMeta, null, null, null );
    assertEquals( null, row.getAsJavaType( "str", String.class, converter ) );
    assertEquals( null, row.getAsJavaType( "str", Integer.class, converter ) );
    assertEquals( null, row.getAsJavaType( "str", Long.class, converter ) );
    assertEquals( null, row.getAsJavaType( "str", Boolean.class, converter ) );
  }

  @Test
  public void testBooleanConversion() throws Exception {

    row = new RowMetaAndData( rowsMeta, null, true, null );
    assertEquals( true, row.getAsJavaType( "bool", boolean.class, converter ) );
    assertEquals( true, row.getAsJavaType( "bool", Boolean.class, converter ) );
    assertEquals( 1, row.getAsJavaType( "bool", int.class, converter ) );
    assertEquals( 1, row.getAsJavaType( "bool", Integer.class, converter ) );
    assertEquals( 1L, row.getAsJavaType( "bool", long.class, converter ) );
    assertEquals( 1L, row.getAsJavaType( "bool", Long.class, converter ) );
    assertEquals( "Y", row.getAsJavaType( "bool", String.class, converter ) );

    row = new RowMetaAndData( rowsMeta, null, false, null );
    assertEquals( false, row.getAsJavaType( "bool", boolean.class, converter ) );
    assertEquals( false, row.getAsJavaType( "bool", Boolean.class, converter ) );
    assertEquals( 0, row.getAsJavaType( "bool", int.class, converter ) );
    assertEquals( 0, row.getAsJavaType( "bool", Integer.class, converter ) );
    assertEquals( 0L, row.getAsJavaType( "bool", long.class, converter ) );
    assertEquals( 0L, row.getAsJavaType( "bool", Long.class, converter ) );
    assertEquals( "N", row.getAsJavaType( "bool", String.class, converter ) );

    row = new RowMetaAndData( rowsMeta, null, null, null );
    assertEquals( null, row.getAsJavaType( "bool", String.class, converter ) );
    assertEquals( null, row.getAsJavaType( "bool", Integer.class, converter ) );
    assertEquals( null, row.getAsJavaType( "bool", Long.class, converter ) );
    assertEquals( null, row.getAsJavaType( "bool", Boolean.class, converter ) );
  }

  @Test
  public void testIntegerConversion() throws Exception {

    row = new RowMetaAndData( rowsMeta, null, null, 7L );
    assertEquals( true, row.getAsJavaType( "int", boolean.class, converter ) );
    assertEquals( true, row.getAsJavaType( "int", Boolean.class, converter ) );
    assertEquals( 7, row.getAsJavaType( "int", int.class, converter ) );
    assertEquals( 7, row.getAsJavaType( "int", Integer.class, converter ) );
    assertEquals( 7L, row.getAsJavaType( "int", long.class, converter ) );
    assertEquals( 7L, row.getAsJavaType( "int", Long.class, converter ) );
    assertEquals( "7", row.getAsJavaType( "int", String.class, converter ) );

    row = new RowMetaAndData( rowsMeta, null, null, 0L );
    assertEquals( false, row.getAsJavaType( "int", boolean.class, converter ) );
    assertEquals( false, row.getAsJavaType( "int", Boolean.class, converter ) );

    row = new RowMetaAndData( rowsMeta, null, null, null );
    assertEquals( null, row.getAsJavaType( "int", String.class, converter ) );
    assertEquals( null, row.getAsJavaType( "int", Integer.class, converter ) );
    assertEquals( null, row.getAsJavaType( "int", Long.class, converter ) );
    assertEquals( null, row.getAsJavaType( "int", Boolean.class, converter ) );
  }

  @Test
  public void testEmptyValues() throws Exception {
    RowMeta rowsMetaEmpty = new RowMeta();

    rowsMetaEmpty.addValueMeta( new ValueMetaString( "str" ) );
    rowsMetaEmpty.addValueMeta( new ValueMetaBoolean( "bool" ) );
    rowsMetaEmpty.addValueMeta( new ValueMetaInteger( "int" ) );
    rowsMetaEmpty.addValueMeta( new ValueMetaNumber( "num" ) );
    rowsMetaEmpty.addValueMeta( new ValueMetaBigNumber( "bignum" ) );
    rowsMetaEmpty.addValueMeta( new ValueMetaBinary( "bin" ) );
    rowsMetaEmpty.addValueMeta( new ValueMetaDate( "date" ) );
    rowsMetaEmpty.addValueMeta( new ValueMetaTimestamp( "timestamp" ) );
    rowsMetaEmpty.addValueMeta( new ValueMetaInternetAddress( "inet" ) );
    row = new RowMetaAndData( rowsMetaEmpty, null, null, null, null, null, null, null, null, null );
    assertTrue( row.isEmptyValue( "str" ) );
    assertTrue( row.isEmptyValue( "bool" ) );
    assertTrue( row.isEmptyValue( "int" ) );
    assertTrue( row.isEmptyValue( "num" ) );
    assertTrue( row.isEmptyValue( "bignum" ) );
    assertTrue( row.isEmptyValue( "bin" ) );
    assertTrue( row.isEmptyValue( "date" ) );
    assertTrue( row.isEmptyValue( "timestamp" ) );
    assertTrue( row.isEmptyValue( "inet" ) );
  }
}
