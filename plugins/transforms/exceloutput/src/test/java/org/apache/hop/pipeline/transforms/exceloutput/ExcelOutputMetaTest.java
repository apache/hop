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
package org.apache.hop.pipeline.transforms.exceloutput;


import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.initializer.IInitializer;
import org.apache.hop.pipeline.transforms.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IntLoadSaveValidator;
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

public class ExcelOutputMetaTest implements IInitializer<ITransformMeta> {
  LoadSaveTester loadSaveTester;
  Class<ExcelOutputMeta> testMetaClass = ExcelOutputMeta.class;
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init( false );
    List<String> attributes =
      Arrays.asList( "headerFontName", "headerFontSize", "headerFontBold", "headerFontItalic", "headerFontUnderline",
        "headerFontOrientation", "headerFontColor", "headerBackGroundColor", "headerRowHeight", "headerAlignment",
        "headerImage", "rowFontName", "rowFontSize", "rowFontColor", "rowBackGroundColor", "fileName", "extension",
        "password", "headerEnabled", "footerEnabled", "splitEvery", "transformNrInFilename", "dateInFilename", "addToResultFiles",
        "sheetProtected", "timeInFilename", "templateEnabled", "templateFileName", "templateAppend", "sheetname", "useTempFiles",
        "tempDirectory", "encoding", "append", "doNotOpenNewFileInit", "createParentFolder", "specifyFormat", "dateTimeFormat",
        "autoSizeColumns", "nullBlank", "outputFields" );

    // Note - newline (get/set) doesn't appear to be used or persisted. So, it's not included in the load/save tester.

    Map<String, String> getterMap = new HashMap<>();
    Map<String, String> setterMap = new HashMap<String, String>() {
      {
        put( "sheetProtected", "setProtectSheet" );
        put( "nullBlank", "setNullIsBlank" );
      }
    };

    Map<String, IFieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<>();
    attrValidatorMap.put( "outputFields",
      new ArrayLoadSaveValidator<>( new ExcelFieldLoadSaveValidator(), 5 ) );
    attrValidatorMap.put( "headerFontName", new IntLoadSaveValidator( ExcelOutputMeta.fontNameCode.length ) );
    attrValidatorMap.put( "headerFontUnderline", new IntLoadSaveValidator( ExcelOutputMeta.font_underlineCode.length ) );
    attrValidatorMap.put( "headerFontOrientation", new IntLoadSaveValidator( ExcelOutputMeta.font_orientationCode.length ) );
    attrValidatorMap.put( "headerFontColor", new IntLoadSaveValidator( ExcelOutputMeta.fontColorCode.length ) );
    attrValidatorMap.put( "headerBackGroundColor", new IntLoadSaveValidator( ExcelOutputMeta.fontColorCode.length ) );
    attrValidatorMap.put( "headerAlignment", new IntLoadSaveValidator( ExcelOutputMeta.font_alignmentCode.length ) );
    attrValidatorMap.put( "rowBackGroundColor", new IntLoadSaveValidator( ExcelOutputMeta.fontColorCode.length ) );
    attrValidatorMap.put( "rowFontName", new IntLoadSaveValidator( ExcelOutputMeta.fontNameCode.length ) );
    attrValidatorMap.put( "rowFontColor", new IntLoadSaveValidator( ExcelOutputMeta.fontColorCode.length ) );

    Map<String, IFieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<>();

    loadSaveTester =
      new LoadSaveTester( testMetaClass, attributes, new ArrayList<>(),
        getterMap, setterMap, attrValidatorMap, typeValidatorMap, this );
  }

  // Call the allocate method on the LoadSaveTester meta class
  @Override
  public void modify( ITransformMeta someMeta ) {
    if ( someMeta instanceof ExcelOutputMeta ) {
      ( (ExcelOutputMeta) someMeta ).allocate( 5 );
    }
  }

  @Test
  public void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }

  public class ExcelFieldLoadSaveValidator implements IFieldLoadSaveValidator<ExcelField> {
    final Random rand = new Random();

    @Override
    public ExcelField getTestObject() {
      ExcelField rtn = new ExcelField();
      rtn.setFormat( UUID.randomUUID().toString() );
      rtn.setName( UUID.randomUUID().toString() );
      rtn.setType( rand.nextInt( 7 ) );
      return rtn;
    }

    @Override
    public boolean validateTestObject( ExcelField testObject, Object actual ) {
      if ( !( actual instanceof ExcelField ) ) {
        return false;
      }
      ExcelField actualInput = (ExcelField) actual;
      return ( testObject.toString().equals( actualInput.toString() ) );
    }
  }

}
