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

package org.apache.hop.pipeline.transforms.nullif;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.nullif.NullIfMeta.Field;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class NullIfMetaTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  LoadSaveTester loadSaveTester;

  @Before
  public void setUp() throws Exception {

    List<String> attributes = Arrays.asList( "fields" );

    Map<String, String> getterMap = new HashMap<String, String>() {
      {
        put( "fields", "getFields" );
      }
    };
    Map<String, String> setterMap = new HashMap<String, String>() {
      {
        put( "fields", "setFields" );
      }
    };
    Field field = new Field();
    field.setFieldName( "fieldName" );
    field.setFieldValue( "fieldValue" );
    IFieldLoadSaveValidator<Field[]> fieldArrayLoadSaveValidator =
      new ArrayLoadSaveValidator<>( new NullIfFieldLoadSaveValidator( field ), 5 );
    Map<String, IFieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<>();

    typeValidatorMap.put( Field[].class.getCanonicalName(), fieldArrayLoadSaveValidator );
    Map<String, IFieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<>();
    attrValidatorMap.put( "fields", fieldArrayLoadSaveValidator );

    loadSaveTester =
      new LoadSaveTester( NullIfMeta.class, attributes, getterMap, setterMap, attrValidatorMap, typeValidatorMap );
  }

  @Test
  public void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }

  @Test
  public void setFieldValueTest() {
    Field field = new Field();
    System.setProperty( Const.HOP_EMPTY_STRING_DIFFERS_FROM_NULL, "N" );
    field.setFieldValue( "theValue" );
    assertEquals( "theValue", field.getFieldValue() );
  }

  @Test
  public void setFieldValueNullTest() {
    Field field = new Field();
    System.setProperty( Const.HOP_EMPTY_STRING_DIFFERS_FROM_NULL, "N" );
    field.setFieldValue( null );
    assertEquals( null, field.getFieldValue() );
  }

  @Test
  public void setFieldValueNullWithEmptyStringsDiffersFromNullTest() {
    Field field = new Field();
    System.setProperty( Const.HOP_EMPTY_STRING_DIFFERS_FROM_NULL, "Y" );
    field.setFieldValue( null );
    assertEquals( "", field.getFieldValue() );
  }

  public static class NullIfFieldLoadSaveValidator implements IFieldLoadSaveValidator<Field> {

    private final Field defaultValue;

    public NullIfFieldLoadSaveValidator( Field defaultValue ) {
      this.defaultValue = defaultValue;
    }

    @Override
    public Field getTestObject() {
      return defaultValue;
    }

    @Override
    public boolean validateTestObject( Field testObject, Object actual ) {
      return EqualsBuilder.reflectionEquals( testObject, actual );
    }
  }

}
