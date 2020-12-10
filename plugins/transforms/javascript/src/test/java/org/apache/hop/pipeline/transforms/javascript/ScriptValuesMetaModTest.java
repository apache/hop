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
package org.apache.hop.pipeline.transforms.javascript;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IValueMeta;
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
import org.junit.*;
import org.powermock.reflect.Whitebox;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

public class ScriptValuesMetaModTest implements IInitializer<ITransformMeta> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  LoadSaveTester loadSaveTester;
  Class<ScriptValuesMetaMod> testMetaClass = ScriptValuesMetaMod.class;

  @Before
  public void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init( false );
    List<String> attributes =
      Arrays.asList( "fieldname", "rename", "type", "length", "precision", "replace", "jsScripts", "optimizationLevel" );

    Map<String, String> getterMap = new HashMap<String, String>() {
      {
        put( "fieldname", "getFieldname" );
        put( "rename", "getRename" );
        put( "type", "getType" );
        put( "length", "getLength" );
        put( "precision", "getPrecision" );
        put( "replace", "getReplace" );
//        put( "compatible", "isCompatible" );
        put( "optimizationLevel", "getOptimizationLevel" );
        put( "jsScripts", "getJSScripts" );
      }
    };
    Map<String, String> setterMap = new HashMap<String, String>() {
      {
        put( "fieldname", "setFieldname" );
        put( "rename", "setRename" );
        put( "type", "setType" );
        put( "length", "setLength" );
        put( "precision", "setPrecision" );
        put( "replace", "setReplace" );
//        put( "compatible", "setCompatible" );
        put( "optimizationLevel", "setOptimizationLevel" );
        put( "jsScripts", "setJSScripts" );
      }
    };
    IFieldLoadSaveValidator<String[]> stringArrayLoadSaveValidator =
      new ArrayLoadSaveValidator<>( new StringLoadSaveValidator(), 5 );

    IFieldLoadSaveValidator<ScriptValuesScript[]> svsArrayLoadSaveValidator =
      new ArrayLoadSaveValidator<>( new ScriptValuesScriptLoadSaveValidator(), 5 );

    Map<String, IFieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<>();
    attrValidatorMap.put( "fieldname", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "rename", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "type", new PrimitiveIntArrayLoadSaveValidator( new IntLoadSaveValidator( 9 ), 5 ) );
    attrValidatorMap.put( "length", new PrimitiveIntArrayLoadSaveValidator( new IntLoadSaveValidator( 100 ), 5 ) );
    attrValidatorMap.put( "precision", new PrimitiveIntArrayLoadSaveValidator( new IntLoadSaveValidator( 6 ), 5 ) );
    attrValidatorMap.put( "replace",
      new PrimitiveBooleanArrayLoadSaveValidator( new BooleanLoadSaveValidator(), 5 ) );
    attrValidatorMap.put( "jsScripts", svsArrayLoadSaveValidator );

    Map<String, IFieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<>();

    loadSaveTester =
      new LoadSaveTester( testMetaClass, attributes, new ArrayList<>(),
        getterMap, setterMap, attrValidatorMap, typeValidatorMap, this );
  }

  // Call the allocate method on the LoadSaveTester meta class
  public void modify( ITransformMeta someMeta ) {
    if ( someMeta instanceof ScriptValuesMetaMod ) {
      ( (ScriptValuesMetaMod) someMeta ).allocate( 5 );
    }
  }

  @Test
  @Ignore
  public void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }

  public class ScriptValuesScriptLoadSaveValidator implements IFieldLoadSaveValidator<ScriptValuesScript> {
    final Random rand = new Random();

    @Override
    public ScriptValuesScript getTestObject() {
      int scriptType = rand.nextInt( 4 );
      if ( scriptType == 3 ) {
        scriptType = -1;
      }
      ScriptValuesScript rtn = new ScriptValuesScript( scriptType, UUID.randomUUID().toString(), UUID.randomUUID().toString() );
      return rtn;
    }

    @Override
    public boolean validateTestObject( ScriptValuesScript testObject, Object actual ) {
      if ( !( actual instanceof ScriptValuesScript ) ) {
        return false;
      }
      return ( actual.toString().equals( testObject.toString() ) );
    }
  }

  @Test
  public void testExtend() {
    ScriptValuesMetaMod meta = new ScriptValuesMetaMod();
    int size = 1;
    meta.extend( size );

    Assert.assertEquals( size, meta.getFieldname().length );
    Assert.assertNull( meta.getFieldname()[ 0 ] );
    Assert.assertEquals( size, meta.getRename().length );
    Assert.assertNull( meta.getRename()[ 0 ] );
    Assert.assertEquals( size, meta.getType().length );
    Assert.assertEquals( -1, meta.getType()[ 0 ] );
    Assert.assertEquals( size, meta.getLength().length );
    Assert.assertEquals( -1, meta.getLength()[ 0 ] );
    Assert.assertEquals( size, meta.getPrecision().length );
    Assert.assertEquals( -1, meta.getPrecision()[ 0 ] );
    Assert.assertEquals( size, meta.getReplace().length );
    Assert.assertFalse( meta.getReplace()[ 0 ] );

    meta = new ScriptValuesMetaMod();
    // set some values, uneven lengths
    Whitebox.setInternalState( meta, "fieldname", new String[] { "Field 1", "Field 2", "Field 3" } );
    Whitebox.setInternalState( meta, "rename", new String[] { "Field 1 - new" } );
    Whitebox.setInternalState( meta, "type", new int[] { IValueMeta.TYPE_STRING, IValueMeta
      .TYPE_INTEGER, IValueMeta.TYPE_NUMBER } );

    meta.extend( 3 );
    validateExtended( meta );
  }

  private void validateExtended( final ScriptValuesMetaMod meta ) {

    Assert.assertEquals( 3, meta.getFieldname().length );
    Assert.assertEquals( "Field 1", meta.getFieldname()[ 0 ] );
    Assert.assertEquals( "Field 2", meta.getFieldname()[ 1 ] );
    Assert.assertEquals( "Field 3", meta.getFieldname()[ 2 ] );
    Assert.assertEquals( 3, meta.getRename().length );
    Assert.assertEquals( "Field 1 - new", meta.getRename()[ 0 ] );
    Assert.assertNull( meta.getRename()[ 1 ] );
    Assert.assertNull( meta.getRename()[ 2 ] );
    Assert.assertEquals( 3, meta.getType().length );
    Assert.assertEquals( IValueMeta.TYPE_STRING, meta.getType()[ 0 ] );
    Assert.assertEquals( IValueMeta.TYPE_INTEGER, meta.getType()[ 1 ] );
    Assert.assertEquals( IValueMeta.TYPE_NUMBER, meta.getType()[ 2 ] );
    Assert.assertEquals( 3, meta.getLength().length );
    Assert.assertEquals( -1, meta.getLength()[ 0 ] );
    Assert.assertEquals( -1, meta.getLength()[ 1 ] );
    Assert.assertEquals( -1, meta.getLength()[ 2 ] );
    Assert.assertEquals( 3, meta.getPrecision().length );
    Assert.assertEquals( -1, meta.getPrecision()[ 0 ] );
    Assert.assertEquals( -1, meta.getPrecision()[ 1 ] );
    Assert.assertEquals( -1, meta.getPrecision()[ 2 ] );
    Assert.assertEquals( 3, meta.getReplace().length );
    Assert.assertFalse( meta.getReplace()[ 0 ] );
    Assert.assertFalse( meta.getReplace()[ 1 ] );
    Assert.assertFalse( meta.getReplace()[ 2 ] );
  }
}
