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

package org.apache.hop.pipeline.transforms.stringoperations;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.initializer.IInitializer;
import org.apache.hop.pipeline.transforms.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IntLoadSaveValidator;
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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;


/**
 * User: Dzmitry Stsiapanau Date: 2/3/14 Time: 5:41 PM
 */
public class StringOperationsMetaTest implements IInitializer<ITransformMeta> {
  LoadSaveTester loadSaveTester;
  Class<StringOperationsMeta> testMetaClass = StringOperationsMeta.class;
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init( false );
    List<String> attributes =
      Arrays.asList( "padLen", "padChar", "fieldInStream", "fieldOutStream", "trimType", "lowerUpper", "initCap", "maskXML", "digits", "removeSpecialCharacters", "paddingType" );

    IFieldLoadSaveValidator<String[]> stringArrayLoadSaveValidator =
      new ArrayLoadSaveValidator<>( new StringLoadSaveValidator(), 5 );

    Map<String, IFieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<>();
    attrValidatorMap.put( "padLen", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "padChar", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "fieldInStream", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "fieldOutStream", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "trimType",
      new PrimitiveIntArrayLoadSaveValidator( new IntLoadSaveValidator( 4 ), 5 ) );
    attrValidatorMap.put( "lowerUpper",
      new PrimitiveIntArrayLoadSaveValidator( new IntLoadSaveValidator( StringOperationsMeta.lowerUpperCode.length ), 5 ) );
    attrValidatorMap.put( "initCap",
      new PrimitiveIntArrayLoadSaveValidator( new IntLoadSaveValidator( StringOperationsMeta.initCapCode.length ), 5 ) );
    attrValidatorMap.put( "maskXML",
      new PrimitiveIntArrayLoadSaveValidator( new IntLoadSaveValidator( StringOperationsMeta.maskXMLCode.length ), 5 ) );
    attrValidatorMap.put( "digits",
      new PrimitiveIntArrayLoadSaveValidator( new IntLoadSaveValidator( StringOperationsMeta.digitsCode.length ), 5 ) );
    attrValidatorMap.put( "removeSpecialCharacters",
      new PrimitiveIntArrayLoadSaveValidator( new IntLoadSaveValidator( StringOperationsMeta.removeSpecialCharactersCode.length ), 5 ) );
    attrValidatorMap.put( "paddingType",
      new PrimitiveIntArrayLoadSaveValidator( new IntLoadSaveValidator( StringOperationsMeta.paddingCode.length ), 5 ) );

    Map<String, IFieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<>();

    loadSaveTester =
      new LoadSaveTester( testMetaClass, attributes, new ArrayList<>(),
        new HashMap<>(), new HashMap<>(), attrValidatorMap, typeValidatorMap, this );
  }

  // Call the allocate method on the LoadSaveTester meta class
  @Override
  public void modify( ITransformMeta someMeta ) {
    if ( someMeta instanceof StringOperationsMeta ) {
      ( (StringOperationsMeta) someMeta ).allocate( 5 );
    }
  }

  @Test
  public void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }

  @Test
  public void testGetFields() throws Exception {
    StringOperationsMeta meta = new StringOperationsMeta();
    meta.allocate( 1 );
    meta.setFieldInStream( new String[] { "field1" } );

    IRowMeta iRowMeta = new RowMeta();
    IValueMeta valueMeta = new ValueMetaString( "field1" );
    valueMeta.setStorageMetadata( new ValueMetaString( "field1" ) );
    valueMeta.setStorageType( IValueMeta.STORAGE_TYPE_BINARY_STRING );
    iRowMeta.addValueMeta( valueMeta );

    IVariables variables = mock( IVariables.class );
    meta.getFields( iRowMeta, "STRING_OPERATIONS", null, null, variables, null );
    IRowMeta expectedRowMeta = new RowMeta();
    expectedRowMeta.addValueMeta( new ValueMetaString( "field1" ) );
    assertEquals( expectedRowMeta.toString(), iRowMeta.toString() );
  }
}
