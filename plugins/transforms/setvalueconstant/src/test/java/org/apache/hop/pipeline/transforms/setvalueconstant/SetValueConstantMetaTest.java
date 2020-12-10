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

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.initializer.IInitializer;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.ListLoadSaveValidator;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

public class SetValueConstantMetaTest implements IInitializer<ITransformMeta> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();
  LoadSaveTester loadSaveTester;
  Class<SetValueConstantMeta> testMetaClass = SetValueConstantMeta.class;

  @Before
  public void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init( false );
    List<String> attributes =
      Arrays.asList( "fields", "usevar" );

    Map<String, String> getterMap = new HashMap<String, String>() {
      {
        put( "fields", "getFields" );
        put( "usevar", "isUseVars" );
      }
    };
    Map<String, String> setterMap = new HashMap<String, String>() {
      {
        put( "fields", "setFields" );
        put( "usevar", "setUseVars" );
      }
    };

    Map<String, IFieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<>();
    attrValidatorMap.put( "fields", new ListLoadSaveValidator<>( new SetValueConstantMetaFieldLoadSaveValidator(), 5 ) );
    Map<String, IFieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<>();

    loadSaveTester =
      new LoadSaveTester( testMetaClass, attributes, new ArrayList<>(),
        getterMap, setterMap, attrValidatorMap, typeValidatorMap, this );
  }

  @Override
  public void modify( ITransformMeta someMeta ) {
  }

  @Test
  public void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }

  public class SetValueConstantMetaFieldLoadSaveValidator implements IFieldLoadSaveValidator<SetValueConstantMeta.Field> {
    final Random rand = new Random();

    @Override
    public SetValueConstantMeta.Field getTestObject() {
      SetValueConstantMeta.Field field = new SetValueConstantMeta.Field();
      field.setReplaceMask( UUID.randomUUID().toString() );
      field.setReplaceValue( UUID.randomUUID().toString() );
      field.setEmptyString( rand.nextBoolean() );
      field.setFieldName( UUID.randomUUID().toString() );
      return field;
    }

    @Override
    public boolean validateTestObject( SetValueConstantMeta.Field testObject, Object actual ) {
      if ( !( actual instanceof SetValueConstantMeta.Field ) ) {
        return false;
      }
      SetValueConstantMeta.Field actualInput = (SetValueConstantMeta.Field) actual;
      return ( actualInput.equals( testObject ) );
    }
  }

}
