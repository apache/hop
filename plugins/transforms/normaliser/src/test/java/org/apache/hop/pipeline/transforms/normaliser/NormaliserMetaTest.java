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

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.initializer.IInitializer;
import org.apache.hop.pipeline.transforms.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
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

public class NormaliserMetaTest implements IInitializer<NormaliserMeta> {
  LoadSaveTester<NormaliserMeta> loadSaveTester;
  Class<NormaliserMeta> testMetaClass = NormaliserMeta.class;
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init( false );
    List<String> attributes =
      Arrays.asList( "typeField", "normaliserFields" );

    Map<String, String> getterMap = new HashMap<>();
    Map<String, String> setterMap = new HashMap<>();

    Map<String, IFieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<>();
    attrValidatorMap.put( "normaliserFields", new ArrayLoadSaveValidator<>( new NormaliserFieldLoadSaveValidator(), 5 ) );

    Map<String, IFieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<>();

    loadSaveTester =  new LoadSaveTester<>( testMetaClass, attributes, new ArrayList<>(), getterMap, setterMap, attrValidatorMap, typeValidatorMap, this );
  }

  // Call the allocate method on the LoadSaveTester meta class
  @Override
  public void modify( NormaliserMeta someMeta ) {
    if ( someMeta instanceof NormaliserMeta ) {
      ( (NormaliserMeta) someMeta ).allocate( 5 );
    }
  }

  @Test
  public void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }

  // NormaliserFieldLoadSaveValidator
  public class NormaliserFieldLoadSaveValidator implements IFieldLoadSaveValidator<NormaliserMeta.NormaliserField> {
    final Random rand = new Random();

    @Override
    public NormaliserMeta.NormaliserField getTestObject() {
      NormaliserMeta.NormaliserField rtn = new NormaliserMeta.NormaliserField();
      rtn.setName( UUID.randomUUID().toString() );
      rtn.setNorm( UUID.randomUUID().toString() );
      rtn.setValue( UUID.randomUUID().toString() );
      return rtn;
    }

    @Override
    public boolean validateTestObject( NormaliserMeta.NormaliserField testObject, Object actual ) {
      if ( !( actual instanceof NormaliserMeta.NormaliserField ) ) {
        return false;
      }
      NormaliserMeta.NormaliserField another = (NormaliserMeta.NormaliserField) actual;
      return new EqualsBuilder()
        .append( testObject.getName(), another.getName() )
        .append( testObject.getNorm(), another.getNorm() )
        .append( testObject.getValue(), another.getValue() )
        .isEquals();
    }
  }

}
