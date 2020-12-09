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

package org.apache.hop.pipeline.transforms.ldifinput;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.initializer.IInitializer;
import org.apache.hop.pipeline.transforms.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.StringLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.YNLoadSaveValidator;
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

public class LDIFInputMetaTest implements IInitializer<ITransformMeta> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();
  LoadSaveTester loadSaveTester;

  @Before
  public void setUp() throws Exception {
    List<String> attributes =
      Arrays.asList( "includeFilename", "filenameField", "includeRowNumber", "rowNumberField", "rowLimit",
        "addtoresultfilename", "multiValuedSeparator", "includeContentType", "contentTypeField", "DNField",
        "includeDN", "filefield", "dynamicFilenameField", "shortFileFieldName", "pathFieldName", "hiddenFieldName",
        "lastModificationTimeFieldName", "uriNameFieldName", "rootUriNameFieldName", "extensionFieldName", "sizeFieldName",
        "fileRequired", "includeSubFolders", "fileName", "fileMask", "excludeFileMask", "inputFields" );

    Map<String, String> getterMap = new HashMap<String, String>() {
      {
        put( "includeFilename", "getIncludeFilename" );
        put( "filenameField", "getFilenameField" );
        put( "includeRowNumber", "getIncludeRowNumber" );
        put( "rowNumberField", "getRowNumberField" );
        put( "rowLimit", "getRowLimit" );
        put( "addtoresultfilename", "getAddToResultFilename" );
        put( "multiValuedSeparator", "getMultiValuedSeparator" );
        put( "includeContentType", "getIncludeContentType" );
        put( "contentTypeField", "getContentTypeField" );
        put( "DNField", "getDNField" );
        put( "includeDN", "getIncludeDN" );
        put( "filefield", "isFileField" );
        put( "dynamicFilenameField", "getDynamicFilenameField" );
        put( "shortFileFieldName", "getShortFileNameField" );
        put( "pathFieldName", "getPathField" );
        put( "hiddenFieldName", "getHiddenField" );
        put( "lastModificationTimeFieldName", "getLastModificationDateField" );
        put( "uriNameFieldName", "getUriField" );
        put( "rootUriNameFieldName", "getRootUriField" );
        put( "extensionFieldName", "getExtensionField" );
        put( "sizeFieldName", "getSizeField" );
        put( "fileRequired", "getFileRequired" );
        put( "includeSubFolders", "getIncludeSubFolders" );
        put( "fileName", "getFileName" );
        put( "fileMask", "getFileMask" );
        put( "excludeFileMask", "getExcludeFileMask" );
        put( "inputFields", "getInputFields" );
      }
    };

    Map<String, String> setterMap = new HashMap<String, String>() {
      {
        put( "includeFilename", "setIncludeFilename" );
        put( "filenameField", "setFilenameField" );
        put( "includeRowNumber", "setIncludeRowNumber" );
        put( "rowNumberField", "setRowNumberField" );
        put( "rowLimit", "setRowLimit" );
        put( "addtoresultfilename", "setAddToResultFilename" );
        put( "multiValuedSeparator", "setMultiValuedSeparator" );
        put( "includeContentType", "setIncludeContentType" );
        put( "contentTypeField", "setContentTypeField" );
        put( "DNField", "setDNField" );
        put( "includeDN", "setIncludeDN" );
        put( "filefield", "setFileField" );
        put( "dynamicFilenameField", "setDynamicFilenameField" );
        put( "shortFileFieldName", "setShortFileNameField" );
        put( "pathFieldName", "setPathField" );
        put( "hiddenFieldName", "setHiddenField" );
        put( "lastModificationTimeFieldName", "setLastModificationDateField" );
        put( "uriNameFieldName", "setUriField" );
        put( "rootUriNameFieldName", "setRootUriField" );
        put( "extensionFieldName", "setExtensionField" );
        put( "sizeFieldName", "setSizeField" );
        put( "fileRequired", "setFileRequired" );
        put( "includeSubFolders", "setIncludeSubFolders" );
        put( "fileName", "setFileName" );
        put( "fileMask", "setFileMask" );
        put( "excludeFileMask", "setExcludeFileMask" );
        put( "inputFields", "setInputFields" );
      }
    };
    IFieldLoadSaveValidator<String[]> stringArrayLoadSaveValidator =
      new ArrayLoadSaveValidator<>( new StringLoadSaveValidator(), 5 );

    IFieldLoadSaveValidator<LDIFInputField[]> liflsv =
      new ArrayLoadSaveValidator<>( new LDIFInputFieldLoadSaveValidator(), 5 );

    IFieldLoadSaveValidator<String[]> YNArrayLoadSaveValidator =
      new ArrayLoadSaveValidator<>( new YNLoadSaveValidator(), 5 );

    Map<String, IFieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<>();
    attrValidatorMap.put( "fileName", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "fileMask", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "excludeFileMask", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "fileRequired", YNArrayLoadSaveValidator );
    attrValidatorMap.put( "includeSubFolders", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "inputFields", liflsv );

    Map<String, IFieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<>();

    loadSaveTester =
      new LoadSaveTester( LDIFInputMeta.class, attributes, new ArrayList<>(),
        getterMap, setterMap, attrValidatorMap, typeValidatorMap, this );
  }

  // Call the allocate method on the LoadSaveTester meta class
  @Override
  public void modify( ITransformMeta someMeta ) {
    if ( someMeta instanceof LDIFInputMeta ) {
      ( (LDIFInputMeta) someMeta ).allocate( 5, 5 );
    }
  }

  @Test
  public void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }

  public class LDIFInputFieldLoadSaveValidator implements IFieldLoadSaveValidator<LDIFInputField> {
    final Random rand = new Random();

    @Override
    public LDIFInputField getTestObject() {
      LDIFInputField rtn = new LDIFInputField();
      rtn.setAttribut( UUID.randomUUID().toString() );
      rtn.setCurrencySymbol( UUID.randomUUID().toString() );
      rtn.setDecimalSymbol( UUID.randomUUID().toString() );
      rtn.setFormat( UUID.randomUUID().toString() );
      rtn.setGroupSymbol( UUID.randomUUID().toString() );
      rtn.setLength( rand.nextInt( 50 ) );
      rtn.setName( UUID.randomUUID().toString() );
      rtn.setPrecision( rand.nextInt( 9 ) );
      rtn.setRepeated( rand.nextBoolean() );
      rtn.setSamples( new String[] { UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString() } );
      rtn.setTrimType( rand.nextInt( 4 ) );
      rtn.setType( rand.nextInt( 5 ) );
      return rtn;
    }

    @Override
    public boolean validateTestObject( LDIFInputField testObject, Object actual ) {
      if ( !( actual instanceof LDIFInputField ) ) {
        return false;
      }
      LDIFInputField actualInput = (LDIFInputField) actual;
      return ( testObject.getXml().equals( actualInput.getXml() ) );
    }
  }
}
