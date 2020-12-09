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
package org.apache.hop.pipeline.transforms.getvariable;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaTimestamp;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transforms.getvariable.GetVariableMeta.FieldDefinition;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.initializer.IInitializer;
import org.apache.hop.pipeline.transforms.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class GetVariableMetaTest implements IInitializer<GetVariableMeta> {
  LoadSaveTester<GetVariableMeta> loadSaveTester;
  Class<GetVariableMeta> testMetaClass = GetVariableMeta.class;
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @BeforeClass
  public static void setUpBeforeClass() throws HopException {
    HopEnvironment.init();
    PluginRegistry.init( false );
  }

  @Before
  public void setUpLoadSave() throws Exception {
    List<String> attributes = Arrays.asList( "fieldDefinitions" );

    Map<String, String> getterMap = new HashMap<>();
    getterMap.put( "fieldDefinitions", "getFieldDefinitions" );

    Map<String, String> setterMap = new HashMap<>();
    setterMap.put( "fieldDefinitions", "setFieldDefinitions" );

    FieldDefinition fieldDefinition = new FieldDefinition();
    fieldDefinition.setFieldName( "fieldName" );
    fieldDefinition.setFieldLength( 4 );
    fieldDefinition.setCurrency( null );
    fieldDefinition.setFieldPrecision( 5 );
    fieldDefinition.setFieldType( IValueMeta.TYPE_NUMBER );
    fieldDefinition.setGroup( "group" );
    fieldDefinition.setVariableString( "variableString" );

    IFieldLoadSaveValidator<FieldDefinition[]> fieldDefinitionLoadSaveValidator =
      new ArrayLoadSaveValidator<>( new FieldDefinitionLoadSaveValidator( fieldDefinition ), 5 );

    Map<String, IFieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<>();
    attrValidatorMap.put( "fieldName", fieldDefinitionLoadSaveValidator );

    Map<String, IFieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<>();
    typeValidatorMap.put( FieldDefinition[].class.getCanonicalName(), fieldDefinitionLoadSaveValidator );

    loadSaveTester =
      new LoadSaveTester<>( testMetaClass, attributes, Collections.emptyList(), getterMap,
        setterMap, attrValidatorMap, typeValidatorMap, this );
  }

  // Call the allocate method on the LoadSaveTester meta class
  @Override
  public void modify( GetVariableMeta someMeta ) {
    someMeta.allocate( 5 );
  }

  @Test
  public void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }

  @Test
  public void testGetValueMetaPlugin() throws HopTransformException {
    GetVariableMeta meta = new GetVariableMeta();
    meta.setDefault();

    FieldDefinition field = new FieldDefinition();
    field.setFieldName( "outputField" );
    field.setVariableString( String.valueOf( 2000000L ) );
    field.setFieldType( IValueMeta.TYPE_TIMESTAMP );
    meta.setFieldDefinitions( new FieldDefinition[] { field } );

    IRowMeta rowMeta = new RowMeta();
    meta.getFields( rowMeta, "transformName", null, null, new Variables(), null );

    assertNotNull( rowMeta );
    assertEquals( 1, rowMeta.size() );
    assertEquals( "outputField", rowMeta.getFieldNames()[ 0 ] );
    assertEquals( IValueMeta.TYPE_TIMESTAMP, rowMeta.getValueMeta( 0 ).getType() );
    assertTrue( rowMeta.getValueMeta( 0 ) instanceof ValueMetaTimestamp );
  }

  public static class FieldDefinitionLoadSaveValidator implements IFieldLoadSaveValidator<FieldDefinition> {

    private final FieldDefinition defaultValue;

    public FieldDefinitionLoadSaveValidator( FieldDefinition defaultValue ) {
      this.defaultValue = defaultValue;
    }

    @Override
    public FieldDefinition getTestObject() {
      return defaultValue;
    }

    @Override
    public boolean validateTestObject( FieldDefinition testObject, Object actual ) {
      return EqualsBuilder.reflectionEquals( testObject, actual );
    }
  }
}
