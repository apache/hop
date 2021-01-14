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

package org.apache.hop.pipeline.transforms.fileinput.text;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transforms.file.BaseFileField;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static org.junit.Assert.assertTrue;

/**
 * @author Andrey Khayrutdinov
 */
public class TextFileInputMetaLoadSaveTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  private LoadSaveTester tester;

  @Before
  public void setUp() throws Exception {
    List<String> commonAttributes = Arrays.asList(
      "errorCountField",
      "errorFieldsField",
      "errorTextField",
      "length" );
    List<String> xmlAttributes = Collections.emptyList();

    Map<String, String> getters = new HashMap<>();
    getters.put( "header", "hasHeader" );
    getters.put( "footer", "hasFooter" );
    getters.put( "noEmptyLines", "noEmptyLines" );
    getters.put( "includeFilename", "includeFilename" );
    getters.put( "includeRowNumber", "includeRowNumber" );
    getters.put( "errorFilesExtension", "getErrorLineFilesExtension" );
    getters.put( "isaddresult", "isAddResultFile" );
    getters.put( "shortFileFieldName", "getShortFileNameField" );
    getters.put( "pathFieldName", "getPathField" );
    getters.put( "hiddenFieldName", "isHiddenField" );
    getters.put( "lastModificationTimeFieldName", "getLastModificationDateField" );
    getters.put( "uriNameFieldName", "getUriField" );
    getters.put( "rootUriNameFieldName", "getRootUriField" );
    getters.put( "extensionFieldName", "getExtensionField" );
    getters.put( "sizeFieldName", "getSizeField" );

    Map<String, String> setters = new HashMap<>();
    setters.put( "fileName", "setFileNameForTest" );
    setters.put( "errorFilesExtension", "setErrorLineFilesExtension" );
    setters.put( "isaddresult", "setAddResultFile" );
    setters.put( "shortFileFieldName", "setShortFileNameField" );
    setters.put( "pathFieldName", "setPathField" );
    setters.put( "hiddenFieldName", "setIsHiddenField" );
    setters.put( "lastModificationTimeFieldName", "setLastModificationDateField" );
    setters.put( "uriNameFieldName", "setUriField" );
    setters.put( "rootUriNameFieldName", "setRootUriField" );
    setters.put( "extensionFieldName", "setExtensionField" );
    setters.put( "sizeFieldName", "setSizeField" );

    Map<String, IFieldLoadSaveValidator<?>> attributeValidators = Collections.emptyMap();

    Map<String, IFieldLoadSaveValidator<?>> typeValidators = new HashMap<>();
    typeValidators.put( TextFileFilter[].class.getCanonicalName(), new ArrayLoadSaveValidator<>( new TextFileFilterValidator() ) );
    typeValidators.put( BaseFileField[].class.getCanonicalName(), new ArrayLoadSaveValidator<>( new TextFileInputFieldValidator() ) );

    assertTrue( !commonAttributes.isEmpty() || !xmlAttributes.isEmpty() );

    tester =
      new LoadSaveTester( TextFileInputMeta.class, commonAttributes, xmlAttributes,
        getters, setters, attributeValidators, typeValidators );
  }

  @Test
  public void testSerialization() throws Exception {
    tester.testSerialization();
  }

  private static class TextFileInputFieldValidator implements IFieldLoadSaveValidator<BaseFileField> {
    @Override public BaseFileField getTestObject() {
      return new BaseFileField( UUID.randomUUID().toString(), new Random().nextInt(), new Random().nextInt() );
    }

    @Override
    public boolean validateTestObject( BaseFileField testObject, Object actual ) {
      if ( !( actual instanceof BaseFileField ) ) {
        return false;
      }

      BaseFileField another = (BaseFileField) actual;
      return new EqualsBuilder()
        .append( testObject.getName(), another.getName() )
        .append( testObject.getLength(), another.getLength() )
        .append( testObject.getPosition(), another.getPosition() )
        .isEquals();
    }
  }

  private static class TextFileFilterValidator implements IFieldLoadSaveValidator<TextFileFilter> {
    @Override public TextFileFilter getTestObject() {
      TextFileFilter fileFilter = new TextFileFilter();
      fileFilter.setFilterPosition( new Random().nextInt() );
      fileFilter.setFilterString( UUID.randomUUID().toString() );
      fileFilter.setFilterLastLine( new Random().nextBoolean() );
      fileFilter.setFilterPositive( new Random().nextBoolean() );
      return fileFilter;
    }

    @Override public boolean validateTestObject( TextFileFilter testObject, Object actual ) {
      if ( !( actual instanceof TextFileFilter ) ) {
        return false;
      }

      TextFileFilter another = (TextFileFilter) actual;
      return new EqualsBuilder()
        .append( testObject.getFilterPosition(), another.getFilterPosition() )
        .append( testObject.getFilterString(), another.getFilterString() )
        .append( testObject.isFilterLastLine(), another.isFilterLastLine() )
        .append( testObject.isFilterPositive(), another.isFilterPositive() )
        .isEquals();
    }
  }
}
