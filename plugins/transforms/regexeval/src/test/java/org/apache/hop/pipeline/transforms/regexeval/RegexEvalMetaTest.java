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

package org.apache.hop.pipeline.transforms.regexeval;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.initializer.IInitializer;
import org.apache.hop.pipeline.transforms.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IntLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.PrimitiveIntArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.StringLoadSaveValidator;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RegexEvalMetaTest implements IInitializer<ITransform> {
  IRowMeta mockInputRowMeta;
  IVariables mockVariableSpace;
  LoadSaveTester loadSaveTester;
  Class<RegexEvalMeta> testMetaClass = RegexEvalMeta.class;
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @BeforeClass
  public static void setupClass() throws HopException {
    ValueMetaPluginType.getInstance().searchPlugins();
  }

  @Before
  public void setup() throws Exception {
    mockInputRowMeta = mock( IRowMeta.class );
    mockVariableSpace = mock( IVariables.class );
  }

  @Test
  public void testGetFieldsReplacesResultFieldIfItExists() throws HopTransformException {
    RegexEvalMeta regexEvalMeta = new RegexEvalMeta();
    String name = "TEST_NAME";
    String resultField = "result";
    regexEvalMeta.setResultFieldName( resultField );
    when( mockInputRowMeta.indexOfValue( resultField ) ).thenReturn( 0 );
    IValueMeta mockValueMeta = mock( IValueMeta.class );
    String mockName = "MOCK_NAME";
    when( mockValueMeta.getName() ).thenReturn( mockName );
    when( mockInputRowMeta.getValueMeta( 0 ) ).thenReturn( mockValueMeta );
    regexEvalMeta.setReplacefields( true );
    regexEvalMeta.getFields( mockInputRowMeta, name, null, null, mockVariableSpace, null );
    ArgumentCaptor<IValueMeta> captor = ArgumentCaptor.forClass( IValueMeta.class );
    verify( mockInputRowMeta ).setValueMeta( eq( 0 ), captor.capture() );
    assertEquals( mockName, captor.getValue().getName() );
  }

  @Test
  public void testGetFieldsAddsResultFieldIfDoesntExist() throws HopTransformException {
    RegexEvalMeta regexEvalMeta = new RegexEvalMeta();
    String name = "TEST_NAME";
    String resultField = "result";
    regexEvalMeta.setResultFieldName( resultField );
    when( mockInputRowMeta.indexOfValue( resultField ) ).thenReturn( -1 );
    IValueMeta mockValueMeta = mock( IValueMeta.class );
    String mockName = "MOCK_NAME";
    when( mockVariableSpace.resolve( resultField ) ).thenReturn( mockName );
    when( mockInputRowMeta.getValueMeta( 0 ) ).thenReturn( mockValueMeta );
    regexEvalMeta.setReplacefields( true );
    regexEvalMeta.getFields( mockInputRowMeta, name, null, null, mockVariableSpace, null );
    ArgumentCaptor<IValueMeta> captor = ArgumentCaptor.forClass( IValueMeta.class );
    verify( mockInputRowMeta ).addValueMeta( captor.capture() );
    assertEquals( mockName, captor.getValue().getName() );
  }

  @Test
  public void testGetFieldsReplacesFieldIfItExists() throws HopTransformException {
    RegexEvalMeta regexEvalMeta = new RegexEvalMeta();
    String name = "TEST_NAME";
    regexEvalMeta.allocate( 1 );
    String fieldName = "fieldname";
    //CHECKSTYLE:Indentation:OFF
    regexEvalMeta.getFieldName()[ 0 ] = fieldName;
    when( mockInputRowMeta.indexOfValue( fieldName ) ).thenReturn( 0 );
    IValueMeta mockValueMeta = mock( IValueMeta.class );
    String mockName = "MOCK_NAME";
    when( mockValueMeta.getName() ).thenReturn( mockName );
    when( mockInputRowMeta.getValueMeta( 0 ) ).thenReturn( mockValueMeta );
    regexEvalMeta.setReplacefields( true );
    regexEvalMeta.setAllowCaptureGroupsFlag( true );
    regexEvalMeta.getFields( mockInputRowMeta, name, null, null, mockVariableSpace, null );
    ArgumentCaptor<IValueMeta> captor = ArgumentCaptor.forClass( IValueMeta.class );
    verify( mockInputRowMeta ).setValueMeta( eq( 0 ), captor.capture() );
    assertEquals( mockName, captor.getValue().getName() );
  }

  @Test
  public void testGetFieldsAddsFieldIfDoesntExist() throws HopTransformException {
    RegexEvalMeta regexEvalMeta = new RegexEvalMeta();
    String name = "TEST_NAME";
    regexEvalMeta.allocate( 1 );
    String fieldName = "fieldname";
    regexEvalMeta.getFieldName()[ 0 ] = fieldName;
    when( mockInputRowMeta.indexOfValue( fieldName ) ).thenReturn( -1 );
    IValueMeta mockValueMeta = mock( IValueMeta.class );
    String mockName = "MOCK_NAME";
    when( mockVariableSpace.resolve( fieldName ) ).thenReturn( mockName );
    when( mockInputRowMeta.getValueMeta( 0 ) ).thenReturn( mockValueMeta );
    regexEvalMeta.setReplacefields( true );
    regexEvalMeta.setAllowCaptureGroupsFlag( true );
    regexEvalMeta.getFields( mockInputRowMeta, name, null, null, mockVariableSpace, null );
    ArgumentCaptor<IValueMeta> captor = ArgumentCaptor.forClass( IValueMeta.class );
    verify( mockInputRowMeta ).addValueMeta( captor.capture() );
    assertEquals( fieldName, captor.getValue().getName() );
  }

  @Before
  public void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init( false );
    List<String> attributes =
      Arrays.asList( "script", "matcher", "resultfieldname", "usevar", "allowcapturegroups", "replacefields", "canoneq",
        "caseinsensitive", "comment", "dotall", "multiline", "unicode", "unix", "fieldName", "fieldFormat", "fieldGroup",
        "fieldDecimal", "fieldCurrency", "fieldNullIf", "fieldIfNull", "fieldTrimType", "fieldLength", "fieldPrecision",
        "fieldType" );

    Map<String, String> getterMap = new HashMap<String, String>() {
      {
        put( "script", "getScript" );
        put( "matcher", "getMatcher" );
        put( "resultfieldname", "getResultFieldName" );
        put( "usevar", "isUseVariableInterpolationFlagSet" );
        put( "allowcapturegroups", "isAllowCaptureGroupsFlagSet" );
        put( "replacefields", "isReplacefields" );
        put( "canoneq", "isCanonicalEqualityFlagSet" );
        put( "caseinsensitive", "isCaseInsensitiveFlagSet" );
        put( "comment", "isCommentFlagSet" );
        put( "dotall", "isDotAllFlagSet" );
        put( "multiline", "isMultilineFlagSet" );
        put( "unicode", "isUnicodeFlagSet" );
        put( "unix", "isUnixLineEndingsFlagSet" );
        put( "fieldName", "getFieldName" );
        put( "fieldFormat", "getFieldFormat" );
        put( "fieldGroup", "getFieldGroup" );
        put( "fieldDecimal", "getFieldDecimal" );
        put( "fieldCurrency", "getFieldCurrency" );
        put( "fieldNullIf", "getFieldNullIf" );
        put( "fieldIfNull", "getFieldIfNull" );
        put( "fieldTrimType", "getFieldTrimType" );
        put( "fieldLength", "getFieldLength" );
        put( "fieldPrecision", "getFieldPrecision" );
        put( "fieldType", "getFieldType" );
      }
    };
    Map<String, String> setterMap = new HashMap<String, String>() {
      {
        put( "script", "setScript" );
        put( "matcher", "setMatcher" );
        put( "resultfieldname", "setResultFieldName" );
        put( "usevar", "setUseVariableInterpolationFlag" );
        put( "allowcapturegroups", "setAllowCaptureGroupsFlag" );
        put( "replacefields", "setReplacefields" );
        put( "canoneq", "setCanonicalEqualityFlag" );
        put( "caseinsensitive", "setCaseInsensitiveFlag" );
        put( "comment", "setCommentFlag" );
        put( "dotall", "setDotAllFlag" );
        put( "multiline", "setMultilineFlag" );
        put( "unicode", "setUnicodeFlag" );
        put( "unix", "setUnixLineEndingsFlag" );
        put( "fieldName", "setFieldName" );
        put( "fieldFormat", "setFieldFormat" );
        put( "fieldGroup", "setFieldGroup" );
        put( "fieldDecimal", "setFieldDecimal" );
        put( "fieldCurrency", "setFieldCurrency" );
        put( "fieldNullIf", "setFieldNullIf" );
        put( "fieldIfNull", "setFieldIfNull" );
        put( "fieldTrimType", "setFieldTrimType" );
        put( "fieldLength", "setFieldLength" );
        put( "fieldPrecision", "setFieldPrecision" );
        put( "fieldType", "setFieldType" );
      }
    };
    IFieldLoadSaveValidator<String[]> stringArrayLoadSaveValidator =
      new ArrayLoadSaveValidator<>( new StringLoadSaveValidator(), 5 );


    Map<String, IFieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<>();
    attrValidatorMap.put( "fieldName", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "fieldFormat", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "fieldGroup", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "fieldDecimal", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "fieldCurrency", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "fieldNullIf", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "fieldIfNull", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "fieldTrimType", new PrimitiveIntArrayLoadSaveValidator( new IntLoadSaveValidator( ValueMetaBase.getTrimTypeCodes().length ), 5 ) );
    attrValidatorMap.put( "fieldLength", new PrimitiveIntArrayLoadSaveValidator( new IntLoadSaveValidator( 100 ), 5 ) );
    attrValidatorMap.put( "fieldPrecision", new PrimitiveIntArrayLoadSaveValidator( new IntLoadSaveValidator( 9 ), 5 ) );
    attrValidatorMap.put( "fieldType", new PrimitiveIntArrayLoadSaveValidator( new IntLoadSaveValidator( 9 ), 5 ) );


    Map<String, IFieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<>();

    loadSaveTester =
      new LoadSaveTester( testMetaClass, attributes, new ArrayList<>(),
        getterMap, setterMap, attrValidatorMap, typeValidatorMap, this );
  }

  // Call the allocate method on the LoadSaveTester meta class
  public void modify( ITransform someMeta ) {
    if ( someMeta instanceof RegexEvalMeta ) {
      ( (RegexEvalMeta) someMeta ).allocate( 5 );
    }
  }


}
