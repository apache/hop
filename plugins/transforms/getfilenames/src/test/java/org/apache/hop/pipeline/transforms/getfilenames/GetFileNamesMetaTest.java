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

package org.apache.hop.pipeline.transforms.getfilenames;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.StringLoadSaveValidator;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class GetFileNamesMetaTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Test
  public void testRoundTrip() throws HopException {
    List<String> attributes =
      Arrays.asList( "filterfiletype", "doNotFailIfNoFile", "rownum", "isaddresult", "filefield", "rownum_field",
        "filename_Field", "wildcard_Field", "exclude_wildcard_Field", "dynamic_include_subfolders", "limit", "name",
        "filemask", "exclude_filemask", "file_required", "include_subfolders" );

    Map<String, String> getterMap = new HashMap<>();
    getterMap.put( "filterfiletype", "getFileTypeFilter" );
    getterMap.put( "doNotFailIfNoFile", "isdoNotFailIfNoFile" );
    getterMap.put( "rownum", "includeRowNumber" );
    getterMap.put( "isaddresult", "isAddResultFile" );
    getterMap.put( "filefield", "isFileField" );
    getterMap.put( "rownum_field", "getRowNumberField" );
    getterMap.put( "filename_Field", "getDynamicFilenameField" );
    getterMap.put( "wildcard_Field", "getDynamicWildcardField" );
    getterMap.put( "exclude_wildcard_Field", "getDynamicExcludeWildcardField" );
    getterMap.put( "dynamic_include_subfolders", "isDynamicIncludeSubFolders" );
    getterMap.put( "limit", "getRowLimit" );
    getterMap.put( "name", "getFileName" );
    getterMap.put( "filemask", "getFileMask" );
    getterMap.put( "exclude_filemask", "getExludeFileMask" );
    getterMap.put( "file_required", "getFileRequired" );
    getterMap.put( "include_subfolders", "getIncludeSubFolders" );

    Map<String, String> setterMap = new HashMap<>();
    setterMap.put( "filterfiletype", "setFilterFileType" );
    setterMap.put( "doNotFailIfNoFile", "setdoNotFailIfNoFile" );
    setterMap.put( "rownum", "setIncludeRowNumber" );
    setterMap.put( "isaddresult", "setAddResultFile" );
    setterMap.put( "filefield", "setFileField" );
    setterMap.put( "rownum_field", "setRowNumberField" );
    setterMap.put( "filename_Field", "setDynamicFilenameField" );
    setterMap.put( "wildcard_Field", "setDynamicWildcardField" );
    setterMap.put( "exclude_wildcard_Field", "setDynamicExcludeWildcardField" );
    setterMap.put( "dynamic_include_subfolders", "setDynamicIncludeSubFolders" );
    setterMap.put( "limit", "setRowLimit" );
    setterMap.put( "name", "setFileName" );
    setterMap.put( "filemask", "setFileMask" );
    setterMap.put( "exclude_filemask", "setExcludeFileMask" );
    setterMap.put( "file_required", "setFileRequired" );
    setterMap.put( "include_subfolders", "setIncludeSubFolders" );

    Map<String, IFieldLoadSaveValidator<?>> fieldLoadSaveValidatorAttributeMap =
      new HashMap<>();

    //Arrays need to be consistent length
    IFieldLoadSaveValidator<String[]> stringArrayLoadSaveValidator =
      new ArrayLoadSaveValidator<>( new StringLoadSaveValidator(), 25 );
    IFieldLoadSaveValidator<String[]> fileRequiredArrayLoadSaveValidator =
      new ArrayLoadSaveValidator<>( new FileRequiredLoadSaveValidator(), 25 );

    fieldLoadSaveValidatorAttributeMap.put( "filterfiletype", new FileTypeFilterLoadSaveValidator() );
    fieldLoadSaveValidatorAttributeMap.put( "name", stringArrayLoadSaveValidator );
    fieldLoadSaveValidatorAttributeMap.put( "filemask", stringArrayLoadSaveValidator );
    fieldLoadSaveValidatorAttributeMap.put( "name", stringArrayLoadSaveValidator );
    fieldLoadSaveValidatorAttributeMap.put( "exclude_filemask", stringArrayLoadSaveValidator );
    fieldLoadSaveValidatorAttributeMap.put( "file_required", fileRequiredArrayLoadSaveValidator );
    fieldLoadSaveValidatorAttributeMap.put( "include_subfolders", stringArrayLoadSaveValidator );

    LoadSaveTester<GetFileNamesMeta> loadSaveTester =
      new LoadSaveTester<>( GetFileNamesMeta.class, attributes, getterMap, setterMap,
        fieldLoadSaveValidatorAttributeMap, new HashMap<>() );

    loadSaveTester.testSerialization();
  }

  public class FileTypeFilterLoadSaveValidator implements IFieldLoadSaveValidator<FileInputList.FileTypeFilter> {

    @Override
    public FileInputList.FileTypeFilter getTestObject() {
      FileInputList.FileTypeFilter[] filters = FileInputList.FileTypeFilter.values();
      return filters[ new Random().nextInt( filters.length ) ];
    }

    @Override
    public boolean validateTestObject( FileInputList.FileTypeFilter testObject, Object actual ) {
      if ( !( actual instanceof FileInputList.FileTypeFilter ) ) {
        return false;
      }
      return testObject.equals( actual );
    }
  }

  public class FileRequiredLoadSaveValidator implements IFieldLoadSaveValidator<String> {

    @Override
    public String getTestObject() {
      return GetFileNamesMeta.RequiredFilesCode[ new Random().nextInt( GetFileNamesMeta.RequiredFilesCode.length ) ];
    }

    @Override
    public boolean validateTestObject( String testObject, Object actual ) {
      return testObject.equals( actual );
    }
  }

  // cloneTest() removed as it's now covered by the load/save tester.

}
