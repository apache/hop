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
package org.apache.hop.pipeline.transforms.denormaliser;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.initializer.IInitializer;
import org.apache.hop.pipeline.transforms.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
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
import java.util.UUID;

public class DenormalizerMetaTest implements IInitializer<ITransformMeta> {
  LoadSaveTester loadSaveTester;
  Class<DenormaliserMeta> testMetaClass = DenormaliserMeta.class;
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init( false );
    List<String> attributes =
      Arrays.asList( "groupField", "keyField", "denormaliserTargetField" );

    Map<String, String> getterMap = new HashMap<String, String>() {
      //CHECKSTYLE IGNORE EmptyBlock FOR NEXT 3 LINES
      {
        // put( "fieldName", "getFieldName" );
      }
    };
    Map<String, String> setterMap = new HashMap<String, String>() {
      //CHECKSTYLE IGNORE EmptyBlock FOR NEXT 3 LINES
      {
        // put( "fieldName", "setFieldName" );
      }
    };
    IFieldLoadSaveValidator<String[]> stringArrayLoadSaveValidator =
      new ArrayLoadSaveValidator<>( new StringLoadSaveValidator(), 5 );

    Map<String, IFieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<>();
    attrValidatorMap.put( "groupField", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "denormaliserTargetField",
      new ArrayLoadSaveValidator<>( new DenormaliserTargetFieldLoadSaveValidator(), 5 ) );

    Map<String, IFieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<>();

    loadSaveTester =
      new LoadSaveTester( testMetaClass, attributes, new ArrayList<>(),
        getterMap, setterMap, attrValidatorMap, typeValidatorMap, this );
  }

  // Call the allocate method on the LoadSaveTester meta class
  @Override
  public void modify( ITransformMeta someMeta ) {
    if ( someMeta instanceof DenormaliserMeta ) {
      ( (DenormaliserMeta) someMeta ).allocate( 5, 5 );
    }
  }

  @Test
  public void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }

  public class DenormaliserTargetFieldLoadSaveValidator implements IFieldLoadSaveValidator<DenormaliserTargetField> {
    final Random rand = new Random();

    @Override
    public DenormaliserTargetField getTestObject() {
      DenormaliserTargetField rtn = new DenormaliserTargetField();
      rtn.setFieldName( UUID.randomUUID().toString() );
      rtn.setKeyValue( UUID.randomUUID().toString() );
      rtn.setTargetCurrencySymbol( UUID.randomUUID().toString() );
      rtn.setTargetGroupingSymbol( UUID.randomUUID().toString() );
      rtn.setTargetName( UUID.randomUUID().toString() );
      rtn.setTargetType( rand.nextInt( 7 ) );
      rtn.setTargetPrecision( rand.nextInt( 9 ) );
      rtn.setTargetNullString( UUID.randomUUID().toString() );
      rtn.setTargetLength( rand.nextInt( 50 ) );
      rtn.setTargetDecimalSymbol( UUID.randomUUID().toString() );
      rtn.setTargetAggregationType( rand.nextInt( DenormaliserTargetField.typeAggrDesc.length ) );
      return rtn;
    }

    @Override
    public boolean validateTestObject( DenormaliserTargetField testObject, Object actual ) {
      if ( !( actual instanceof DenormaliserTargetField ) ) {
        return false;
      }
      DenormaliserTargetField another = (DenormaliserTargetField) actual;
      return new EqualsBuilder()
        .append( testObject.getFieldName(), another.getFieldName() )
        .append( testObject.getKeyValue(), another.getKeyValue() )
        .append( testObject.getTargetName(), another.getTargetName() )
        .append( testObject.getTargetType(), another.getTargetType() )
        .append( testObject.getTargetLength(), another.getTargetLength() )
        .append( testObject.getTargetPrecision(), another.getTargetPrecision() )
        .append( testObject.getTargetCurrencySymbol(), another.getTargetCurrencySymbol() )
        .append( testObject.getTargetDecimalSymbol(), another.getTargetDecimalSymbol() )
        .append( testObject.getTargetGroupingSymbol(), another.getTargetGroupingSymbol() )
        .append( testObject.getTargetNullString(), another.getTargetNullString() )
        .append( testObject.getTargetFormat(), another.getTargetFormat() )
        .append( testObject.getTargetAggregationType(), another.getTargetAggregationType() )
        .isEquals();
    }
  }

}
