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

package org.apache.hop.pipeline.transforms.csvinput;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.file.TextFileInputField;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.TransformLoadSaveTester;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transforms.loadsave.initializer.IInitializer;
import org.apache.hop.pipeline.transforms.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

public class CsvInputMetaTest implements IInitializer<ITransformMeta> {
  TransformLoadSaveTester<CsvInputMeta> transformLoadSaveTester;
  Class<CsvInputMeta> testMetaClass = CsvInputMeta.class;
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  private static class TextFileInputFieldValidator implements IFieldLoadSaveValidator<TextFileInputField> {
    @Override public TextFileInputField getTestObject() {
      return new TextFileInputField( UUID.randomUUID().toString(), new Random().nextInt(), new Random().nextInt() );
    }

    @Override
    public boolean validateTestObject( TextFileInputField testObject, Object actual ) {
      if ( !( actual instanceof TextFileInputField ) ) {
        return false;
      }

      TextFileInputField another = (TextFileInputField) actual;
      return new EqualsBuilder()
        .append( testObject.getName(), another.getName() )
        .append( testObject.getLength(), another.getLength() )
        .append( testObject.getType(), another.getType() )
        .append( testObject.getTrimType(), another.getTrimType() )
        .isEquals();
    }
  }

  @Before
  public void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init( false );
    List<String> attributes =
      Arrays.asList( "bufferSize", "delimiter", "enclosure", "encoding", "filename", "filenameField", "inputFields", "rowNumField",
        "addResultFile", "headerPresent", "includingFilename", "lazyConversionActive", "newlinePossibleInFields", "runningInParallel" );

    Map<String, String> getterMap = new HashMap<String, String>() {
      {
        put( "inputFields", "getInputFields" );
        put( "hasHeader", "hasHeader" );
        put( "includeFilename", "includeFilename" );
        put( "includeRowNumber", "includeRowNumber" );
      }
    };
    Map<String, String> setterMap = new HashMap<String, String>() {
      {
        put( "inputFields", "setInputFields" );
        put( "includeFilename", "includeFilename" );
        put( "includeRowNumber", "includeRowNumber" );
      }
    };

    Map<String, IFieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<>();
    attrValidatorMap.put( "inputFields", new ArrayLoadSaveValidator<>( new TextFileInputFieldValidator(), 5 ) );

    Map<String, IFieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<>();

    transformLoadSaveTester = new TransformLoadSaveTester( testMetaClass,
      attributes, attributes, getterMap, setterMap, attrValidatorMap, typeValidatorMap );
  }

  // Call the allocate method on the LoadSaveTester meta class
  @Override
  public void modify( ITransformMeta someMeta ) {
    if ( someMeta instanceof CsvInputMeta ) {
      ( (CsvInputMeta) someMeta ).allocate( 5 );
    }
  }

  @Test
  public void testSerialization() throws HopException {
    transformLoadSaveTester.testSerialization();
  }

  @Test
  public void testClone() {
    final CsvInputMeta original = new CsvInputMeta();
    original.setDelimiter( ";" );
    original.setEnclosure( "'" );
    final TextFileInputField[] originalFields = new TextFileInputField[ 1 ];
    final TextFileInputField originalField = new TextFileInputField();
    originalField.setName( "field" );
    originalFields[ 0 ] = originalField;
    original.setInputFields( originalFields );

    final CsvInputMeta clone = (CsvInputMeta) original.clone();
    // verify that the clone and its input fields are "equal" to the originals, but not the same objects
    Assert.assertNotSame( original, clone );
    Assert.assertEquals( original.getDelimiter(), clone.getDelimiter() );
    Assert.assertEquals( original.getEnclosure(), clone.getEnclosure() );

    Assert.assertNotSame( original.getInputFields(), clone.getInputFields() );
    Assert.assertNotSame( original.getInputFields()[ 0 ], clone.getInputFields()[ 0 ] );
    Assert.assertEquals( original.getInputFields()[ 0 ].getName(), clone.getInputFields()[ 0 ].getName() );
  }
}
