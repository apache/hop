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
package org.apache.hop.pipeline.transforms.memgroupby;

import org.apache.hop.core.Const;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaInternetAddress;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.row.value.ValueMetaTimestamp;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.initializer.IInitializer;
import org.apache.hop.pipeline.transforms.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IntLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.PrimitiveIntArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.StringLoadSaveValidator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class MemoryGroupByMetaTest implements IInitializer<MemoryGroupByMeta> {
  LoadSaveTester<MemoryGroupByMeta> loadSaveTester;
  Class<MemoryGroupByMeta> testMetaClass = MemoryGroupByMeta.class;
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init( false );
    List<String> attributes =
      Arrays.asList( "alwaysGivingBackOneRow", "groupField", "aggregateField", "subjectField", "aggregateType", "valueField" );

    IFieldLoadSaveValidator<String[]> stringArrayLoadSaveValidator =
      new ArrayLoadSaveValidator<>( new StringLoadSaveValidator(), 5 );

    Map<String, IFieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<>();
    attrValidatorMap.put( "groupField", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "aggregateField", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "subjectField", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "valueField", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "aggregateType", new PrimitiveIntArrayLoadSaveValidator(
      new IntLoadSaveValidator( MemoryGroupByMeta.typeGroupCode.length ), 5 ) );

    Map<String, IFieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<>();

    loadSaveTester = new LoadSaveTester<>( testMetaClass, attributes, new ArrayList<>(),
      new HashMap<>(), new HashMap<>(), attrValidatorMap, typeValidatorMap, this );
  }

  // Call the allocate method on the LoadSaveTester meta class
  @Override
  public void modify( MemoryGroupByMeta someMeta ) {
    someMeta.allocate( 5, 5 );
  }

  @Test
  public void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }

  private IRowMeta getInputRowMeta() {
    IRowMeta rm = new RowMeta();
    rm.addValueMeta( new ValueMetaString( "myGroupField2" ) );
    rm.addValueMeta( new ValueMetaString( "myGroupField1" ) );
    rm.addValueMeta( new ValueMetaString( "myString" ) );
    rm.addValueMeta( new ValueMetaInteger( "myInteger" ) );
    rm.addValueMeta( new ValueMetaNumber( "myNumber" ) );
    rm.addValueMeta( new ValueMetaBigNumber( "myBigNumber" ) );
    rm.addValueMeta( new ValueMetaBinary( "myBinary" ) );
    rm.addValueMeta( new ValueMetaBoolean( "myBoolean" ) );
    rm.addValueMeta( new ValueMetaDate( "myDate" ) );
    rm.addValueMeta( new ValueMetaTimestamp( "myTimestamp" ) );
    rm.addValueMeta( new ValueMetaInternetAddress( "myInternetAddress" ) );
    return rm;
  }

  @Test
  public void testGetFields() {
    final String transformName = "this transform name";
    MemoryGroupByMeta meta = new MemoryGroupByMeta();
    meta.setDefault();
    meta.allocate( 1, 17 );

    // Declare input fields
    IRowMeta rm = getInputRowMeta();

    String[] groupFields = new String[ 2 ];
    groupFields[ 0 ] = "myGroupField1";
    groupFields[ 1 ] = "myGroupField2";

    String[] aggregateFields = new String[ 24 ];
    String[] subjectFields = new String[ 24 ];
    int[] aggregateTypes = new int[ 24 ];
    String[] valueFields = new String[ 24 ];

    subjectFields[ 0 ] = "myString";
    aggregateTypes[ 0 ] = MemoryGroupByMeta.TYPE_GROUP_CONCAT_COMMA;
    aggregateFields[ 0 ] = "ConcatComma";
    valueFields[ 0 ] = null;

    subjectFields[ 1 ] = "myString";
    aggregateTypes[ 1 ] = MemoryGroupByMeta.TYPE_GROUP_CONCAT_STRING;
    aggregateFields[ 1 ] = "ConcatString";
    valueFields[ 1 ] = "|";

    subjectFields[ 2 ] = "myString";
    aggregateTypes[ 2 ] = MemoryGroupByMeta.TYPE_GROUP_COUNT_ALL;
    aggregateFields[ 2 ] = "CountAll";
    valueFields[ 2 ] = null;

    subjectFields[ 3 ] = "myString";
    aggregateTypes[ 3 ] = MemoryGroupByMeta.TYPE_GROUP_COUNT_ANY;
    aggregateFields[ 3 ] = "CountAny";
    valueFields[ 3 ] = null;

    subjectFields[ 4 ] = "myString";
    aggregateTypes[ 4 ] = MemoryGroupByMeta.TYPE_GROUP_COUNT_DISTINCT;
    aggregateFields[ 4 ] = "CountDistinct";
    valueFields[ 4 ] = null;

    subjectFields[ 5 ] = "myString";
    aggregateTypes[ 5 ] = MemoryGroupByMeta.TYPE_GROUP_FIRST;
    aggregateFields[ 5 ] = "First(String)";
    valueFields[ 5 ] = null;

    subjectFields[ 6 ] = "myInteger";
    aggregateTypes[ 6 ] = MemoryGroupByMeta.TYPE_GROUP_FIRST;
    aggregateFields[ 6 ] = "First(Integer)";
    valueFields[ 6 ] = null;

    subjectFields[ 7 ] = "myNumber";
    aggregateTypes[ 7 ] = MemoryGroupByMeta.TYPE_GROUP_FIRST_INCL_NULL;
    aggregateFields[ 7 ] = "FirstInclNull(Number)";
    valueFields[ 7 ] = null;

    subjectFields[ 8 ] = "myBigNumber";
    aggregateTypes[ 8 ] = MemoryGroupByMeta.TYPE_GROUP_FIRST_INCL_NULL;
    aggregateFields[ 8 ] = "FirstInclNull(BigNumber)";
    valueFields[ 8 ] = null;

    subjectFields[ 9 ] = "myBinary";
    aggregateTypes[ 9 ] = MemoryGroupByMeta.TYPE_GROUP_LAST;
    aggregateFields[ 9 ] = "Last(Binary)";
    valueFields[ 9 ] = null;

    subjectFields[ 10 ] = "myBoolean";
    aggregateTypes[ 10 ] = MemoryGroupByMeta.TYPE_GROUP_LAST;
    aggregateFields[ 10 ] = "Last(Boolean)";
    valueFields[ 10 ] = null;

    subjectFields[ 11 ] = "myDate";
    aggregateTypes[ 11 ] = MemoryGroupByMeta.TYPE_GROUP_LAST_INCL_NULL;
    aggregateFields[ 11 ] = "LastInclNull(Date)";
    valueFields[ 11 ] = null;

    subjectFields[ 12 ] = "myTimestamp";
    aggregateTypes[ 12 ] = MemoryGroupByMeta.TYPE_GROUP_LAST_INCL_NULL;
    aggregateFields[ 12 ] = "LastInclNull(Timestamp)";
    valueFields[ 12 ] = null;

    subjectFields[ 13 ] = "myInternetAddress";
    aggregateTypes[ 13 ] = MemoryGroupByMeta.TYPE_GROUP_MAX;
    aggregateFields[ 13 ] = "Max(InternetAddress)";
    valueFields[ 13 ] = null;

    subjectFields[ 14 ] = "myString";
    aggregateTypes[ 14 ] = MemoryGroupByMeta.TYPE_GROUP_MAX;
    aggregateFields[ 14 ] = "Max(String)";
    valueFields[ 14 ] = null;

    subjectFields[ 15 ] = "myInteger";
    aggregateTypes[ 15 ] = MemoryGroupByMeta.TYPE_GROUP_MEDIAN; // Always returns Number
    aggregateFields[ 15 ] = "Median(Integer)";
    valueFields[ 15 ] = null;

    subjectFields[ 16 ] = "myNumber";
    aggregateTypes[ 16 ] = MemoryGroupByMeta.TYPE_GROUP_MIN;
    aggregateFields[ 16 ] = "Min(Number)";
    valueFields[ 16 ] = null;

    subjectFields[ 17 ] = "myBigNumber";
    aggregateTypes[ 17 ] = MemoryGroupByMeta.TYPE_GROUP_MIN;
    aggregateFields[ 17 ] = "Min(BigNumber)";
    valueFields[ 17 ] = null;

    subjectFields[ 18 ] = "myBinary";
    aggregateTypes[ 18 ] = MemoryGroupByMeta.TYPE_GROUP_PERCENTILE;
    aggregateFields[ 18 ] = "Percentile(Binary)";
    valueFields[ 18 ] = "0.5";

    subjectFields[ 19 ] = "myBoolean";
    aggregateTypes[ 19 ] = MemoryGroupByMeta.TYPE_GROUP_STANDARD_DEVIATION;
    aggregateFields[ 19 ] = "StandardDeviation(Boolean)";
    valueFields[ 19 ] = null;

    subjectFields[ 20 ] = "myDate";
    aggregateTypes[ 20 ] = MemoryGroupByMeta.TYPE_GROUP_SUM;
    aggregateFields[ 20 ] = "Sum(Date)";
    valueFields[ 20 ] = null;

    subjectFields[ 21 ] = "myInteger";
    aggregateTypes[ 21 ] = MemoryGroupByMeta.TYPE_GROUP_SUM;
    aggregateFields[ 21 ] = "Sum(Integer)";
    valueFields[ 21 ] = null;

    subjectFields[ 22 ] = "myInteger";
    aggregateTypes[ 22 ] = MemoryGroupByMeta.TYPE_GROUP_AVERAGE;
    aggregateFields[ 22 ] = "Average(Integer)";
    valueFields[ 22 ] = null;

    subjectFields[ 23 ] = "myDate";
    aggregateTypes[ 23 ] = MemoryGroupByMeta.TYPE_GROUP_AVERAGE;
    aggregateFields[ 23 ] = "Average(Date)";
    valueFields[ 23 ] = null;

    meta.setGroupField( groupFields );
    meta.setSubjectField( subjectFields );
    meta.setAggregateType( aggregateTypes );
    meta.setAggregateField( aggregateFields );
    meta.setValueField( valueFields );

    Variables vars = new Variables();
    meta.getFields( rm, transformName, null, null, vars, null );
    assertNotNull( rm );
    assertEquals( 26, rm.size() );
    assertTrue( rm.indexOfValue( "myGroupField1" ) >= 0 );
    assertEquals( IValueMeta.TYPE_STRING, rm.getValueMeta( rm.indexOfValue( "myGroupField1" ) ).getType() );
    assertTrue( rm.indexOfValue( "myGroupField2" ) >= 0 );
    assertEquals( IValueMeta.TYPE_STRING, rm.getValueMeta( rm.indexOfValue( "myGroupField2" ) ).getType() );
    assertTrue( rm.indexOfValue( "myGroupField2" ) > rm.indexOfValue( "myGroupField1" ) );
    assertTrue( rm.indexOfValue( "ConcatComma" ) >= 0 );
    assertEquals( IValueMeta.TYPE_STRING, rm.getValueMeta( rm.indexOfValue( "ConcatComma" ) ).getType() );
    assertTrue( rm.indexOfValue( "ConcatString" ) >= 0 );
    assertEquals( IValueMeta.TYPE_STRING, rm.getValueMeta( rm.indexOfValue( "ConcatString" ) ).getType() );
    assertTrue( rm.indexOfValue( "CountAll" ) >= 0 );
    assertEquals( IValueMeta.TYPE_INTEGER, rm.getValueMeta( rm.indexOfValue( "CountAll" ) ).getType() );
    assertTrue( rm.indexOfValue( "CountAny" ) >= 0 );
    assertEquals( IValueMeta.TYPE_INTEGER, rm.getValueMeta( rm.indexOfValue( "CountAny" ) ).getType() );
    assertTrue( rm.indexOfValue( "CountDistinct" ) >= 0 );
    assertEquals( IValueMeta.TYPE_INTEGER, rm.getValueMeta( rm.indexOfValue( "CountDistinct" ) ).getType() );
    assertTrue( rm.indexOfValue( "First(String)" ) >= 0 );
    assertEquals( IValueMeta.TYPE_STRING, rm.getValueMeta( rm.indexOfValue( "First(String)" ) ).getType() );
    assertTrue( rm.indexOfValue( "First(Integer)" ) >= 0 );
    assertEquals( IValueMeta.TYPE_INTEGER, rm.getValueMeta( rm.indexOfValue( "First(Integer)" ) ).getType() );
    assertTrue( rm.indexOfValue( "FirstInclNull(Number)" ) >= 0 );
    assertEquals( IValueMeta.TYPE_NUMBER, rm.getValueMeta( rm.indexOfValue( "FirstInclNull(Number)" ) ).getType() );
    assertTrue( rm.indexOfValue( "FirstInclNull(BigNumber)" ) >= 0 );
    assertEquals( IValueMeta.TYPE_BIGNUMBER, rm.getValueMeta( rm.indexOfValue( "FirstInclNull(BigNumber)" ) ).getType() );
    assertTrue( rm.indexOfValue( "Last(Binary)" ) >= 0 );
    assertEquals( IValueMeta.TYPE_BINARY, rm.getValueMeta( rm.indexOfValue( "Last(Binary)" ) ).getType() );
    assertTrue( rm.indexOfValue( "Last(Boolean)" ) >= 0 );
    assertEquals( IValueMeta.TYPE_BOOLEAN, rm.getValueMeta( rm.indexOfValue( "Last(Boolean)" ) ).getType() );
    assertTrue( rm.indexOfValue( "LastInclNull(Date)" ) >= 0 );
    assertEquals( IValueMeta.TYPE_DATE, rm.getValueMeta( rm.indexOfValue( "LastInclNull(Date)" ) ).getType() );
    assertTrue( rm.indexOfValue( "LastInclNull(Timestamp)" ) >= 0 );
    assertEquals( IValueMeta.TYPE_TIMESTAMP, rm.getValueMeta( rm.indexOfValue( "LastInclNull(Timestamp)" ) ).getType() );
    assertTrue( rm.indexOfValue( "Max(InternetAddress)" ) >= 0 );
    assertEquals( IValueMeta.TYPE_INET, rm.getValueMeta( rm.indexOfValue( "Max(InternetAddress)" ) ).getType() );
    assertTrue( rm.indexOfValue( "Max(String)" ) >= 0 );
    assertEquals( IValueMeta.TYPE_STRING, rm.getValueMeta( rm.indexOfValue( "Max(String)" ) ).getType() );
    assertTrue( rm.indexOfValue( "Median(Integer)" ) >= 0 );
    assertEquals( IValueMeta.TYPE_NUMBER, rm.getValueMeta( rm.indexOfValue( "Median(Integer)" ) ).getType() );
    assertTrue( rm.indexOfValue( "Min(Number)" ) >= 0 );
    assertEquals( IValueMeta.TYPE_NUMBER, rm.getValueMeta( rm.indexOfValue( "Min(Number)" ) ).getType() );
    assertTrue( rm.indexOfValue( "Min(BigNumber)" ) >= 0 );
    assertEquals( IValueMeta.TYPE_BIGNUMBER, rm.getValueMeta( rm.indexOfValue( "Min(BigNumber)" ) ).getType() );
    assertTrue( rm.indexOfValue( "Percentile(Binary)" ) >= 0 );
    assertEquals( IValueMeta.TYPE_NUMBER, rm.getValueMeta( rm.indexOfValue( "Percentile(Binary)" ) ).getType() );
    assertTrue( rm.indexOfValue( "StandardDeviation(Boolean)" ) >= 0 );
    assertEquals( IValueMeta.TYPE_NUMBER, rm.getValueMeta( rm.indexOfValue( "StandardDeviation(Boolean)" ) ).getType() );
    assertTrue( rm.indexOfValue( "Sum(Date)" ) >= 0 );
    assertEquals( IValueMeta.TYPE_NUMBER, rm.getValueMeta( rm.indexOfValue( "Sum(Date)" ) ).getType() ); // Force changed to Numeric
    assertTrue( rm.indexOfValue( "Sum(Integer)" ) >= 0 );
    assertEquals( IValueMeta.TYPE_INTEGER, rm.getValueMeta( rm.indexOfValue( "Sum(Integer)" ) ).getType() );
    assertTrue( rm.indexOfValue( "Average(Integer)" ) >= 0 );
    assertEquals( IValueMeta.TYPE_INTEGER, rm.getValueMeta( rm.indexOfValue( "Average(Integer)" ) ).getType() );
    assertTrue( rm.indexOfValue( "Average(Date)" ) >= 0 );
    assertEquals( IValueMeta.TYPE_NUMBER, rm.getValueMeta( rm.indexOfValue( "Average(Date)" ) ).getType() );

    // Test Compatibility
    rm = getInputRowMeta();
    vars.setVariable( Const.HOP_COMPATIBILITY_MEMORY_GROUP_BY_SUM_AVERAGE_RETURN_NUMBER_TYPE, "Y" );
    meta.getFields( rm, transformName, null, null, vars, null );
    assertNotNull( rm );
    assertEquals( 26, rm.size() );
    assertTrue( rm.indexOfValue( "Average(Integer)" ) >= 0 );
    assertEquals( IValueMeta.TYPE_NUMBER, rm.getValueMeta( rm.indexOfValue( "Average(Integer)" ) ).getType() );
  }

  @Test
  public void testPDI16559() throws Exception {
    MemoryGroupByMeta memoryGroupBy = new MemoryGroupByMeta();
    memoryGroupBy.setGroupField( new String[] { "group1", "group 2" } );
    memoryGroupBy.setSubjectField( new String[] { "field1", "field2", "field3", "field4", "field5", "field6", "field7", "field8", "field9", "field10", "field11", "field12" } );
    memoryGroupBy.setAggregateField( new String[] { "fieldID1", "fieldID2", "fieldID3", "fieldID4", "fieldID5", "fieldID6", "fieldID7", "fieldID8", "fieldID9", "fieldID10", "fieldID11" } );
    memoryGroupBy.setValueField( new String[] { "asdf", "asdf", "qwer", "qwer", "QErasdf", "zxvv", "fasdf", "qwerqwr" } );
    memoryGroupBy.setAggregateType( new int[] { 12, 6, 15, 14, 23, 177, 13, 21 } );

    try {
      String badXml = memoryGroupBy.getXml();
      Assert.fail( "Before calling afterInjectionSynchronization, should have thrown an ArrayIndexOOB" );
    } catch ( Exception expected ) {
      // Do Nothing
    }
    memoryGroupBy.afterInjectionSynchronization();
    //run without a exception
    String ktrXml = memoryGroupBy.getXml();

    int targetSz = memoryGroupBy.getSubjectField().length;
    Assert.assertEquals( targetSz, memoryGroupBy.getAggregateField().length );
    Assert.assertEquals( targetSz, memoryGroupBy.getAggregateType().length );
    Assert.assertEquals( targetSz, memoryGroupBy.getValueField().length );

    // Check for null arrays being handled
    memoryGroupBy.setValueField( null ); // null string array
    memoryGroupBy.afterInjectionSynchronization();
    Assert.assertEquals( targetSz, memoryGroupBy.getValueField().length );

  }

}
