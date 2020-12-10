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
package org.apache.hop.pipeline.transforms.yamlinput;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transform.ITransform;
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

public class YamlInputMetaTest implements IInitializer<YamlInputMeta> {
  LoadSaveTester<YamlInputMeta> loadSaveTester;
  Class<YamlInputMeta> testMetaClass = YamlInputMeta.class;

  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init( false );
    List<String> attributes =
      Arrays.asList( "includeFilename", "filenameField", "includeRowNumber", "rowNumberField", "rowLimit",
        "encoding", "yamlField", "inFields", "IsAFile", "addResultFile", "validating", "IsIgnoreEmptyFile",
        "doNotFailIfNoFile", "fileName", "fileMask", "fileRequired", "includeSubFolders", "inputFields" );

    Map<String, String> getterMap = new HashMap<String, String>() {
      {
        put( "includeFilename", "includeFilename" );
        put( "filenameField", "getFilenameField" );
        put( "includeRowNumber", "includeRowNumber" );
        put( "rowNumberField", "getRowNumberField" );
        put( "rowLimit", "getRowLimit" );
        put( "encoding", "getEncoding" );
        put( "yamlField", "getYamlField" );
        put( "inFields", "isInFields" );
        put( "IsAFile", "getIsAFile" );
        put( "addResultFile", "addResultFile" );
        put( "validating", "isValidating" );
        put( "IsIgnoreEmptyFile", "isIgnoreEmptyFile" );
        put( "doNotFailIfNoFile", "isdoNotFailIfNoFile" );
        put( "fileName", "getFileName" );
        put( "fileMask", "getFileMask" );
        put( "fileRequired", "getFileRequired" );
        put( "includeSubFolders", "getIncludeSubFolders" );
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
        put( "encoding", "setEncoding" );
        put( "yamlField", "setYamlField" );
        put( "inFields", "setInFields" );
        put( "IsAFile", "setIsAFile" );
        put( "addResultFile", "setAddResultFile" );
        put( "validating", "setValidating" );
        put( "IsIgnoreEmptyFile", "setIgnoreEmptyFile" );
        put( "doNotFailIfNoFile", "setdoNotFailIfNoFile" );
        put( "fileName", "setFileName" );
        put( "fileMask", "setFileMask" );
        put( "fileRequired", "setFileRequired" );
        put( "includeSubFolders", "setIncludeSubFolders" );
        put( "inputFields", "setInputFields" );
      }
    };
    IFieldLoadSaveValidator<String[]> stringArrayLoadSaveValidator =
      new ArrayLoadSaveValidator<>( new StringLoadSaveValidator(), 5 );
    IFieldLoadSaveValidator<YamlInputField[]> yamlInputFieldArrayLoadSaveValidator =
      new ArrayLoadSaveValidator<>( new YamlInputFieldLoadSaveValidator(), 5 );


    Map<String, IFieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<>();
    attrValidatorMap.put( "fileName", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "fileRequired", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "fileMask", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "includeSubFolders", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "inputFields", yamlInputFieldArrayLoadSaveValidator );

    Map<String, IFieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<>();

    loadSaveTester =
      new LoadSaveTester( testMetaClass, attributes, new ArrayList<>(),
        getterMap, setterMap, attrValidatorMap, typeValidatorMap, this );
  }

  // Call the allocate method on the LoadSaveTester meta class
  @Override
  public void modify( YamlInputMeta someMeta ) {
    if ( someMeta instanceof YamlInputMeta ) {
      ( (YamlInputMeta) someMeta ).allocate( 5, 5 );
    }
  }

  @Test
  public void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }

  // YamlInputFieldLoadSaveValidator
  public class YamlInputFieldLoadSaveValidator implements IFieldLoadSaveValidator<YamlInputField> {
    final Random rand = new Random();

    @Override
    public YamlInputField getTestObject() {
      YamlInputField rtn = new YamlInputField();
      rtn.setCurrencySymbol( UUID.randomUUID().toString() );
      rtn.setDecimalSymbol( UUID.randomUUID().toString() );
      rtn.setFormat( UUID.randomUUID().toString() );
      rtn.setGroupSymbol( UUID.randomUUID().toString() );
      rtn.setName( UUID.randomUUID().toString() );
      rtn.setTrimType( rand.nextInt( 4 ) );
      rtn.setPrecision( rand.nextInt( 9 ) );
      rtn.setLength( rand.nextInt( 50 ) );
      rtn.setPath( UUID.randomUUID().toString() );
      rtn.setType( rand.nextInt( 8 ) );
      return rtn;
    }

    @Override
    public boolean validateTestObject( YamlInputField testObject, Object actual ) {
      if ( !( actual instanceof YamlInputField ) ) {
        return false;
      }
      YamlInputField actualInput = (YamlInputField) actual;
      return ( testObject.getXml().equals( actualInput.getXml() ) );
    }
  }

}
