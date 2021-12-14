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

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.initializer.IInitializer;
import org.apache.hop.pipeline.transforms.loadsave.validator.*;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.*;

public class GetFileNamesMetaTest implements IInitializer<ITransformMeta> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  LoadSaveTester loadSaveTester;

  @Before
  public void setUp() throws HopException {
    HopEnvironment.init();
    PluginRegistry.init(false);

    List<String> attributes =
        Arrays.asList(
            "file",
            "filter",
            "rownum",
            "rownum_field",
            "filename_Field",
            "wildcard_Field",
            "exclude_wildcard_Field",
            "filefield",
            "dynamic_include_subfolders",
            "doNotFailIfNoFile",
            "raiseAnExceptionIfNoFile",
            "isaddresult",
            "limit");

    Map<String, String> getterMap = new HashMap<>();
    getterMap.put("file", "getFilesList");
    getterMap.put("filter", "getFilterItemList");
    getterMap.put("rownum", "isIncludeRowNumber");
    getterMap.put("rownum_field", "getRowNumberField");
    getterMap.put("filefield", "isFileField");
    getterMap.put("filename_Field", "getDynamicFilenameField");
    getterMap.put("wildcard_Field", "getDynamicWildcardField");
    getterMap.put("exclude_wildcard_Field", "getDynamicExcludeWildcardField");
    getterMap.put("dynamic_include_subfolders", "isDynamicIncludeSubFolders");
    getterMap.put("limit", "getRowLimit");
    getterMap.put("isaddresult", "isAddResultFile");
    getterMap.put("doNotFailIfNoFile", "isDoNotFailIfNoFile");
    getterMap.put("raiseAnExceptionIfNoFile", "isRaiseAnExceptionIfNoFile");

    Map<String, String> setterMap = new HashMap<>();
    setterMap.put("file", "setFilesList");
    setterMap.put("filter", "setFilterItemList");
    setterMap.put("rownum", "setIncludeRowNumber");
    setterMap.put("rownum_field", "setRowNumberField");
    setterMap.put("filefield", "setFileField");
    setterMap.put("filename_Field", "setDynamicFilenameField");
    setterMap.put("wildcard_Field", "setDynamicWildcardField");
    setterMap.put("exclude_wildcard_Field", "setDynamicExcludeWildcardField");
    setterMap.put("dynamic_include_subfolders", "setDynamicIncludeSubFolders");
    setterMap.put("limit", "setRowLimit");
    setterMap.put("isaddresult", "setAddResultFile");
    setterMap.put("doNotFailIfNoFile", "setDoNotFailIfNoFile");
    setterMap.put("raiseAnExceptionIfNoFile", "setRaiseAnExceptionIfNoFile");

    Map<String, IFieldLoadSaveValidator<?>> fieldLoadSaveValidatorAttributeMap = new HashMap<>();

    Map<String, IFieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<>();
    Map<String, IFieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<>();

    loadSaveTester =
        new LoadSaveTester<>(
            GetFileNamesMeta.class,
            attributes,
            getterMap,
            setterMap,
            attrValidatorMap,
            typeValidatorMap);

    IFieldLoadSaveValidatorFactory validatorFactory =
        loadSaveTester.getFieldLoadSaveValidatorFactory();

    validatorFactory.registerValidator(
        validatorFactory.getName(FileItem.class),
        new ObjectValidator<FileItem>(
            validatorFactory,
            FileItem.class,
            Arrays.asList(
                "name", "filemask", "exclude_filemask", "file_required", "include_subfolders"),
            new HashMap<String, String>() {
              {
                put("name", "getFileName");
                put("filemask", "getFileMask");
                put("exclude_filemask", "getExcludeFileMask");
                put("file_required", "getFileRequired");
                put("include_subfolders", "getIncludeSubFolders");
              }
            },
            new HashMap<String, String>() {
              {
                put("name", "setFileName");
                put("filemask", "setFileMask");
                put("exclude_filemask", "setExcludeFileMask");
                put("file_required", "setFileRequired");
                put("include_subfolders", "setIncludeSubFolders");
              }
            }));

    validatorFactory.registerValidator(
        validatorFactory.getName(List.class, FileItem.class),
        new ListLoadSaveValidator<FileItem>(new FileItemLoadSaveValidator()));

    validatorFactory.registerValidator(
        validatorFactory.getName(FilterItem.class),
        new ObjectValidator<FilterItem>(
            validatorFactory,
            FilterItem.class,
            Arrays.asList("filterfiletype"),
            new HashMap<String, String>() {
              {
                put("filterfiletype", "getFileTypeFilterSelection");
              }
            },
            new HashMap<String, String>() {
              {
                put("filterfiletype", "setFileTypeFilterSelection");
              }
            }));

    validatorFactory.registerValidator(
            validatorFactory.getName(List.class, FilterItem.class),
            new ListLoadSaveValidator<FilterItem>(new FilterItemLoadSaveValidator()));
  }

  @Test
  public void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }

  @Override
  public void modify(ITransformMeta someMeta) {

    if (someMeta instanceof GetFileNamesMeta) {
      ((GetFileNamesMeta) someMeta).getFilesList().clear();
      ((GetFileNamesMeta) someMeta).getFilesList()
              .addAll(
                      Arrays.asList(
                              new FileItem("Filename1", "w1", "ew1", "Y", "N"),
                              new FileItem("Filename2", "w2", "ew2", "Y", "N"),
                              new FileItem("Filename3", "w3", "ew3", "Y", "N"),
                              new FileItem("Filename4", "w4", "ew4", "Y", "N"),
                              new FileItem("Filename5", "w5", "ew5", "Y", "N")));
      ((GetFileNamesMeta) someMeta).getFilterItemList().clear();
      ((GetFileNamesMeta) someMeta).getFilterItemList()
              .addAll(
                      Arrays.asList(
                              new FilterItem("StreamField1"),
                              new FilterItem("StreamField1"),
                              new FilterItem("StreamField1"),
                              new FilterItem("StreamField1"),
                              new FilterItem("StreamField1")));
    }

  }

  public class FilterItemLoadSaveValidator implements IFieldLoadSaveValidator<FilterItem> {
    final Random rand = new Random();

    @Override
    public FilterItem getTestObject() {

      FilterItem field =
              new FilterItem(
                      FileInputList.FileTypeFilter.getByOrdinal(new Random().nextInt(3)).toString());

      return field;
    }

    @Override
    public boolean validateTestObject(FilterItem testObject, Object actual) {
      if (!(actual instanceof FilterItem)) {
        return false;
      }
      FilterItem another = (FilterItem) actual;
      return new EqualsBuilder()
              .append(testObject.getFileTypeFilterSelection(), another.getFileTypeFilterSelection())
              .isEquals();
    }
  }


  public class FileItemLoadSaveValidator implements IFieldLoadSaveValidator<FileItem> {
    final Random rand = new Random();

    @Override
    public FileItem getTestObject() {

      FileItem field =
          new FileItem(
              UUID.randomUUID().toString(),
              UUID.randomUUID().toString(),
              UUID.randomUUID().toString(),
              GetFileNamesMeta.RequiredFilesCode[
                  new Random().nextInt(GetFileNamesMeta.RequiredFilesCode.length)],
              GetFileNamesMeta.RequiredFilesCode[
                  new Random().nextInt(GetFileNamesMeta.RequiredFilesCode.length)]);

      return field;
    }

    @Override
    public boolean validateTestObject(FileItem testObject, Object actual) {
      if (!(actual instanceof FileItem)) {
        return false;
      }
      FileItem another = (FileItem) actual;
      return new EqualsBuilder()
          .append(testObject.getFileName(), another.getFileName())
          .append(testObject.getFileMask(), another.getFileMask())
          .append(testObject.getExcludeFileMask(), another.getExcludeFileMask())
          .append(testObject.getFileRequired(), another.getFileRequired())
          .append(testObject.getIncludeSubFolders(), another.getIncludeSubFolders())
          .isEquals();
    }
  }
}
