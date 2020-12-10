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

package org.apache.hop.pipeline.transforms.mergejoin;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.errorhandling.IStream;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidatorFactory;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


public class MergeJoinMetaTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  LoadSaveTester loadSaveTester;

  public MergeJoinMetaTest() {
    //SwitchCaseMeta bean-like attributes
    List<String> attributes = Arrays.asList( "joinType", "keyFields1", "keyFields2" );

    Map<String, IFieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<>();

    Map<String, IFieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<>();

    Map<String, String> getterMap = new HashMap<>();
    getterMap.put( "joinType", "getJoinType" );
    getterMap.put( "keyFields1", "getKeyFields1" );
    getterMap.put( "keyFields2", "getKeyFields2" );

    Map<String, String> setterMap = new HashMap<>();
    setterMap.put( "joinType", "setJoinType" );
    setterMap.put( "keyFields1", "setKeyFields1" );
    setterMap.put( "keyFields2", "setKeyFields2" );

    this.loadSaveTester = new LoadSaveTester( MergeJoinMeta.class,
      attributes,
      getterMap, setterMap,
      attrValidatorMap, typeValidatorMap );

    IFieldLoadSaveValidatorFactory validatorFactory = loadSaveTester.getFieldLoadSaveValidatorFactory();

    IFieldLoadSaveValidator<MergeJoinMeta> targetValidator = new IFieldLoadSaveValidator<MergeJoinMeta>() {

      @Override
      public MergeJoinMeta getTestObject() {
        return new MergeJoinMeta() {
          {
            setJoinType( joinTypes[ 0 ] );
            setKeyFields1( new String[] { "field1", "field2" } );
            setKeyFields2( new String[] { "field1", "field3" } );
          }
        };
      }

      @Override
      public boolean validateTestObject( MergeJoinMeta testObject, Object actual ) {
        return testObject.getJoinType().equals( ( (MergeJoinMeta) actual ).getJoinType() )
          && Arrays.equals( testObject.getKeyFields1(), ( (MergeJoinMeta) actual ).getKeyFields1() )
          && Arrays.equals( testObject.getKeyFields2(), ( (MergeJoinMeta) actual ).getKeyFields2() );
      }
    };

    validatorFactory.registerValidator( validatorFactory.getName( MergeJoinMeta.class ), targetValidator );
  }

  @Test
  public void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }

  @Test
  public void testGetFieldsEmptyInput() throws Exception {
    RowMeta outputRowMeta = new RowMeta();
    MergeJoinMeta meta = new MergeJoinMeta();

    RowMeta inputRow1 = new RowMeta();
    ValueMetaInteger field1Row1 = new ValueMetaInteger( "field1" );
    field1Row1.setOrigin( "inputTransform1" );
    inputRow1.addValueMeta( field1Row1 );
    ValueMetaString field2Row1 = new ValueMetaString( "field2" );
    field2Row1.setOrigin( "inputTransform1" );
    inputRow1.addValueMeta( field2Row1 );

    RowMeta inputRow2 = new RowMeta();
    ValueMetaString field1Row2 = new ValueMetaString( "field1" );
    field1Row2.setOrigin( "inputTransform2" );
    inputRow2.addValueMeta( field1Row2 );
    ValueMetaString field3Row2 = new ValueMetaString( "field3" );
    field3Row2.setOrigin( "inputTransform2" );
    inputRow2.addValueMeta( field3Row2 );

    TransformMeta transformMeta = new TransformMeta( "Merge", meta );

    meta.getFields( outputRowMeta, "Merge Join",
      new IRowMeta[] { inputRow1, inputRow2 }, transformMeta, new Variables(), null );

    assertNotNull( outputRowMeta );
    assertFalse( outputRowMeta.isEmpty() );
    assertEquals( 4, outputRowMeta.size() );
    List<IValueMeta> vmi = outputRowMeta.getValueMetaList();
    assertNotNull( vmi );
    // Proceed in order
    IValueMeta field1 = outputRowMeta.getValueMeta( 0 );
    assertNotNull( field1 );
    assertEquals( "field1", field1.getName() );
    assertTrue( field1 instanceof ValueMetaInteger );
    assertEquals( "inputTransform1", field1.getOrigin() );

    IValueMeta field2 = outputRowMeta.getValueMeta( 1 );
    assertNotNull( field2 );
    assertEquals( "field2", field2.getName() );
    assertTrue( field2 instanceof ValueMetaString );
    assertEquals( "inputTransform1", field2.getOrigin() );

    IValueMeta field1_1 = outputRowMeta.getValueMeta( 2 );
    assertNotNull( field1_1 );
    assertEquals( "field1_1", field1_1.getName() );
    assertTrue( field1_1 instanceof ValueMetaString );
    assertEquals( "Merge Join", field1_1.getOrigin() );

    IValueMeta field3 = outputRowMeta.getValueMeta( 3 );
    assertNotNull( field3 );
    assertEquals( "field3", field3.getName() );
    assertTrue( field3 instanceof ValueMetaString );
    assertEquals( "inputTransform2", field3.getOrigin() );

  }

  @Test
  public void cloneTest() throws Exception {
    MergeJoinMeta meta = new MergeJoinMeta();
    meta.allocate( 2, 3 );
    meta.setKeyFields1( new String[] { "kf1-1", "kf1-2" } );
    meta.setKeyFields2( new String[] { "kf2-1", "kf2-2", "kf2-3" } );
    // scalars should be cloned using super.clone() - makes sure they're calling super.clone()
    meta.setJoinType( "INNER" );
    MergeJoinMeta aClone = (MergeJoinMeta) meta.clone();
    assertFalse( aClone == meta ); // Not same object returned by clone
    assertTrue( Arrays.equals( meta.getKeyFields1(), aClone.getKeyFields1() ) );
    assertTrue( Arrays.equals( meta.getKeyFields2(), aClone.getKeyFields2() ) );
    assertEquals( meta.getJoinType(), aClone.getJoinType() );

    assertNotNull( aClone.getTransformIOMeta() );
    assertFalse( meta.getTransformIOMeta() == aClone.getTransformIOMeta() );
    List<IStream> infoStreams = meta.getTransformIOMeta().getInfoStreams();
    List<IStream> cloneInfoStreams = aClone.getTransformIOMeta().getInfoStreams();
    assertFalse( infoStreams == cloneInfoStreams );
    int streamSize = infoStreams.size();
    assertTrue( streamSize == cloneInfoStreams.size() );
    for ( int i = 0; i < streamSize; i++ ) {
      assertFalse( infoStreams.get( i ) == cloneInfoStreams.get( i ) );
    }
  }
}
