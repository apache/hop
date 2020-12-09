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
package org.apache.hop.pipeline.transforms.datagrid;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.initializer.IInitializer;
import org.apache.hop.pipeline.transforms.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.BooleanLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IntLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.PrimitiveBooleanArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.PrimitiveIntArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.StringLoadSaveValidator;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertTrue;

public class DataGridMetaTest implements IInitializer<ITransformMeta> {
  LoadSaveTester loadSaveTester;
  Class<DataGridMeta> testMetaClass = DataGridMeta.class;
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init( false );
    List<String> attributes =
      Arrays.asList( "currency", "decimal", "group", "fieldName", "fieldType", "fieldFormat", "fieldLength",
        "fieldPrecision", "setEmptyString", "dataLines" );

    Map<String, String> getterMap = new HashMap<>();
    Map<String, String> setterMap = new HashMap<String, String>() {
      {
        put( "setEmptyString", "setEmptyString" );
      }
    };
    IFieldLoadSaveValidator<String[]> stringArrayLoadSaveValidator =
      new ArrayLoadSaveValidator<>( new StringLoadSaveValidator(), 3 );

    Map<String, IFieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<>();
    attrValidatorMap.put( "currency", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "decimal", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "group", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "fieldName", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "fieldType", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "fieldFormat", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "fieldLength",
      new PrimitiveIntArrayLoadSaveValidator( new IntLoadSaveValidator( 75 ), 3 ) );
    attrValidatorMap.put( "fieldPrecision",
      new PrimitiveIntArrayLoadSaveValidator( new IntLoadSaveValidator( 9 ), 3 ) );
    attrValidatorMap.put( "setEmptyString",
      new PrimitiveBooleanArrayLoadSaveValidator( new BooleanLoadSaveValidator(), 3 ) );
    attrValidatorMap.put( "dataLines", new DataGridLinesLoadSaveValidator() );

    Map<String, IFieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<>();

    loadSaveTester =
      new LoadSaveTester( testMetaClass, attributes, new ArrayList<>(),
        getterMap, setterMap, attrValidatorMap, typeValidatorMap, this );
  }

  // Call the allocate method on the LoadSaveTester meta class
  @Override
  public void modify( ITransformMeta someMeta ) {
    if ( someMeta instanceof DataGridMeta ) {
      ( (DataGridMeta) someMeta ).allocate( 3 );
    }
  }

  @Test
  public void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }

  public class DataGridLinesLoadSaveValidator implements IFieldLoadSaveValidator<List<List<String>>> {
    final Random rand = new Random();

    @Override
    public List<List<String>> getTestObject() {
      List<List<String>> dataLinesList = new ArrayList<>();
      for ( int i = 0; i < 3; i++ ) {
        List<String> dl = new ArrayList<>();
        dl.add( "line" + ( ( i * 2 ) + 1 ) );
        dl.add( "line" + ( ( i * 2 ) + 2 ) );
        dl.add( "line" + ( ( i * 2 ) + 3 ) );
        dataLinesList.add( dl );
      }
      return dataLinesList;
    }

    @Override
    public boolean validateTestObject( List<List<String>> testObject, Object actual ) {
      if ( !( actual instanceof List<?> ) ) {
        return false;
      }
      boolean rtn = true;
      List<?> act0 = (List<?>) actual;
      assertTrue( act0.size() == 3 );
      Object obj0 = act0.get( 0 );
      assertTrue( obj0 instanceof List<?> ); // establishes list of lists
      List<?> act1 = act0;
      rtn = rtn && ( act1.size() == 3 );
      Object obj2 = act1.get( 0 );
      rtn = rtn && ( obj2 instanceof List<?> );
      List<?> obj3 = (List<?>) obj2;
      rtn = rtn && ( obj3.size() == 3 );
      List<List<String>> realActual = (List<List<String>>) actual;
      for ( int i = 0; i < realActual.size(); i++ ) {
        List<String> metaList = realActual.get( i );
        List<String> testList = testObject.get( i );
        rtn = rtn && ( metaList.size() == testList.size() );
        for ( int j = 0; j < metaList.size(); j++ ) {
          rtn = rtn && ( metaList.get( j ).equals( testList.get( j ) ) );
        }
      }
      return rtn;
    }
  }

}
