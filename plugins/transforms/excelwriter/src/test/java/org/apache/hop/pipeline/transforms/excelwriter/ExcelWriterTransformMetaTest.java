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

package org.apache.hop.pipeline.transforms.excelwriter;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.initializer.IInitializer;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidatorFactory;
import org.apache.hop.pipeline.transforms.loadsave.validator.ListLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.ObjectValidator;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

public class ExcelWriterTransformMetaTest  implements IInitializer<ITransformMeta> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @BeforeClass
  public static void setUpBeforeClass() throws HopException {
    HopEnvironment.init();
  }

  @Test
  public void testRoundTrip() throws HopException {
    List<String> attributes = Arrays.asList(
      "header", "footer", "makeSheetActive", "rowWritingMethod", "startingCell", "appendOmitHeader", "appendOffset",
      "appendEmpty", "rowWritingMethod", "forceFormulaRecalculation", "leaveExistingStylesUnchanged",
      "appendLines", "add_to_result_filenames" );

    Map<String, String> getterMap = new HashMap<>();
    getterMap.put( "header", "isHeaderEnabled" );
    getterMap.put( "footer", "isFooterEnabled" );
    getterMap.put( "makeSheetActive", "isMakeSheetActive" );
    getterMap.put( "startingCell", "getStartingCell" );
    getterMap.put( "appendOmitHeader", "isAppendOmitHeader" );
    getterMap.put( "appendOffset", "getAppendOffset" );
    getterMap.put( "appendEmpty", "getAppendEmpty" );
    getterMap.put( "rowWritingMethod", "getRowWritingMethod" );
    getterMap.put( "add_to_result_filenames", "isAddToResultFilenames" );
    getterMap.put( "forceFormulaRecalculation", "isForceFormulaRecalculation" );
    getterMap.put( "leaveExistingStylesUnchanged", "isLeaveExistingStylesUnchanged" );
    getterMap.put( "appendLines", "isAppendLines" );
    getterMap.put( "fields", "getOutputFields" );
    getterMap.put( "template", "getTemplate" );
    getterMap.put( "file", "getFile" );

    Map<String, String> setterMap = new HashMap<>();
    setterMap.put( "header", "setHeaderEnabled" );
    setterMap.put( "footer", "setFooterEnabled" );
    setterMap.put( "makeSheetActive", "setMakeSheetActive" );
    setterMap.put( "startingCell", "setStartingCell" );
    setterMap.put( "appendOmitHeader", "setAppendOmitHeader" );
    setterMap.put( "appendOffset", "setAppendOffset" );
    setterMap.put( "appendEmpty", "setAppendEmpty" );
    setterMap.put( "rowWritingMethod", "setRowWritingMethod" );
    setterMap.put( "add_to_result_filenames", "setAddToResultFilenames" );
    setterMap.put( "forceFormulaRecalculation", "setForceFormulaRecalculation" );
    setterMap.put( "leaveExistingStylesUnchanged", "setLeaveExistingStylesUnchanged" );
    setterMap.put( "appendLines", "setAppendLines" );
    setterMap.put( "fields", "setOutputFields" );
    setterMap.put( "template", "setTemplate" );
    setterMap.put( "file", "setFile" );

    Map<String, IFieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<>();
    attrValidatorMap.put(
            "fields", new ListLoadSaveValidator<>(new ExcelWriterOutputFieldLoadSaveValidator(), 5));

    LoadSaveTester loadSaveTester =
      new LoadSaveTester( ExcelWriterTransformMeta.class, attributes, getterMap, setterMap, attrValidatorMap,
              new HashMap<>(), this );
    IFieldLoadSaveValidatorFactory validatorFactory =
            loadSaveTester.getFieldLoadSaveValidatorFactory();

    validatorFactory.registerValidator(
            validatorFactory.getName(ExcelWriterTemplateField.class),
            new ObjectValidator<ExcelWriterTemplateField>(
                    validatorFactory,
                    ExcelWriterTemplateField.class,
                    Arrays.asList("enabled", "sheet_enabled", "hidden", "sheetname", "filename"),
                    new HashMap<String, String>() {
                      {
                        put("enabled", "isTemplateEnabled");
                        put("sheet_enabled", "isTemplateSheetEnabled");
                        put("hidden", "isTemplateSheetHidden");
                        put("sheetname", "getTemplateSheetName");
                        put("filename", "getTemplateFileName");
                      }
                    },
                    new HashMap<String, String>() {
                      {
                        put("enabled", "setTemplateEnabled");
                        put("sheet_enabled", "setTemplateSheetEnabled");
                        put("hidden", "setTemplateSheetHidden");
                        put("sheetname", "setTemplateSheetName");
                        put("filename", "setTemplateFileName");
                      }
                    }));

    validatorFactory.registerValidator(
            validatorFactory.getName(ExcelWriterFileField.class),
            new ObjectValidator<ExcelWriterFileField>(
                    validatorFactory,
                    ExcelWriterFileField.class,
                    Arrays.asList("name", "extension", "password", "protected_by", "protect_sheet", "add_time"
                            , "sheetname", "do_not_open_newfile_init"
                            , "SpecifyFormat", "date_time_format", "autosizecolums", "stream_data", "splitevery"
                            , "split", "if_file_exists", "if_sheet_exists", "add_date"),
                    new HashMap<String, String>() {
                      {
                        put("name", "getFileName");
                        put("extension", "getExtension");
                        put("password", "getPassword");
                        put("protected_by", "getProtectedBy");
                        put("protect_sheet", "isProtectsheet");
                        put("add_time", "isTimeInFilename");
                        put("sheetname", "getSheetname");
                        put("do_not_open_newfile_init", "isDoNotOpenNewFileInit");
                        put("SpecifyFormat", "isSpecifyFormat");
                        put("date_time_format", "getDateTimeFormat");
                        put("autosizecolums", "isAutosizecolums");
                        put("splitevery", "getSplitEvery");
                        put("stream_data", "isStreamingData");
                        put("split", "isTransformNrInFilename");
                        put("if_file_exists", "getIfFileExists");
                        put("if_sheet_exists", "getIfSheetExists");
                        put("add_date", "isDateInFilename");
                      }
                    },
                    new HashMap<String, String>() {
                      {
                        put("name", "setFileName");
                        put("extension", "setExtension");
                        put("password", "setPassword");
                        put("protected_by", "setProtectedBy");
                        put("protect_sheet", "setProtectsheet");
                        put("add_time", "setTimeInFilename");
                        put("sheetname", "setSheetname");
                        put("do_not_open_newfile_init", "setDoNotOpenNewFileInit");
                        put("SpecifyFormat", "setSpecifyFormat");
                        put("date_time_format", "setDateTimeFormat");
                        put("autosizecolums", "setAutosizecolums");
                        put("splitevery", "setSplitEvery");
                        put("stream_data", "setStreamingData");
                        put("split", "setTransformNrInFilename");
                        put("if_file_exists", "setIfFileExists");
                        put("if_sheet_exists", "setIfSheetExists");
                        put("add_date", "setDateInFilename");
                      }
                    }));

    loadSaveTester.testSerialization();
  }

  // Call the allocate method on the LoadSaveTester meta class
  public void modify( ITransformMeta someMeta ) {
    if ( someMeta instanceof ExcelWriterTransformMeta ) {
      ((ExcelWriterTransformMeta) someMeta ).getOutputFields().clear();
      ((ExcelWriterTransformMeta) someMeta)
              .getOutputFields()
              .addAll(
                      Arrays.asList(
                              new ExcelWriterOutputField("Field1","String", "" ),
                              new ExcelWriterOutputField("Field2","String", "" ),
                              new ExcelWriterOutputField("Field3","String", "" ),
                              new ExcelWriterOutputField("Field4","String", "" ),
                              new ExcelWriterOutputField("Field5","String", "" )));
    }
  }


  public class ExcelWriterTemplateFieldLoadSaveValidator
          implements IFieldLoadSaveValidator<ExcelWriterTemplateField> {
    final Random rand = new Random();

    @Override
    public ExcelWriterTemplateField getTestObject() {

      ExcelWriterTemplateField field =
              new ExcelWriterTemplateField (
                      false,
                      false,
                      false,
                      UUID.randomUUID().toString(),
                      UUID.randomUUID().toString());
      return field;
    }

    @Override
    public boolean validateTestObject(ExcelWriterTemplateField testObject, Object actual) {
      if (!(actual instanceof ExcelWriterTemplateField)) {
        return false;
      }
      ExcelWriterTemplateField another = (ExcelWriterTemplateField) actual;
      return new EqualsBuilder()
              .append(testObject.isTemplateEnabled(), another.isTemplateEnabled())
              .append(testObject.isTemplateSheetEnabled(), another.isTemplateSheetEnabled())
              .append(testObject.isTemplateSheetEnabled(), another.isTemplateSheetHidden())
              .append(testObject.getTemplateSheetName(), another.getTemplateSheetName())
              .append(testObject.getTemplateFileName(), another.getTemplateFileName())
              .isEquals();
    }
  }


  public class ExcelWriterFileFieldLoadSaveValidator
          implements IFieldLoadSaveValidator<ExcelWriterFileField> {
    final Random rand = new Random();

    @Override
    public ExcelWriterFileField getTestObject() {

      ExcelWriterFileField field =
              new ExcelWriterFileField (
                      UUID.randomUUID().toString(),
                      "xls",
                      UUID.randomUUID().toString());
      return field;
    }

    @Override
    public boolean validateTestObject(ExcelWriterFileField testObject, Object actual) {
      if (!(actual instanceof ExcelWriterFileField)) {
        return false;
      }
      ExcelWriterFileField another = (ExcelWriterFileField) actual;
      return new EqualsBuilder()
              .append(testObject.getFileName(), another.getFileName())
              .append(testObject.getExtension(), another.getExtension())
              .append(testObject.getSheetname(), another.getSheetname())
              .isEquals();
    }
  }

  public class ExcelWriterOutputFieldLoadSaveValidator implements IFieldLoadSaveValidator<ExcelWriterOutputField> {
    @Override
    public boolean validateTestObject(ExcelWriterOutputField testObject, Object actual ) {
      //Perform more-extensive test, as equals() method does check on "name" only
      ExcelWriterOutputField obj2 = (ExcelWriterOutputField) actual;
      return testObject.equals( ( obj2 ) )
        && testObject.getType() == obj2.getType()
        && testObject.getFormat().equals( obj2.getFormat() )
        && testObject.getTitle().equals( obj2.getTitle() )
        && testObject.getTitleStyleCell().equals( obj2.getTitleStyleCell() )
        && testObject.getStyleCell().equals( obj2.getStyleCell() )
        && testObject.getCommentField().equals( obj2.getCommentField() )
        && testObject.getCommentAuthorField().equals( obj2.getCommentAuthorField() )
        && testObject.isFormula() == obj2.isFormula()
        && testObject.getHyperlinkField().equals( obj2.getHyperlinkField() );
    }

    @Override
    public ExcelWriterOutputField getTestObject() {
      ExcelWriterOutputField obj = new ExcelWriterOutputField();
      obj.setName( UUID.randomUUID().toString() );
      obj.setType( UUID.randomUUID().toString() );
      obj.setFormat( UUID.randomUUID().toString() );
      obj.setTitle( UUID.randomUUID().toString() );
      obj.setTitleStyleCell( UUID.randomUUID().toString() );
      obj.setStyleCell( UUID.randomUUID().toString() );
      obj.setCommentField( UUID.randomUUID().toString() );
      obj.setCommentAuthorField( UUID.randomUUID().toString() );
      obj.setFormula( new Random().nextBoolean() );
      obj.setHyperlinkField( UUID.randomUUID().toString() );
      return obj;
    }
  }
}
