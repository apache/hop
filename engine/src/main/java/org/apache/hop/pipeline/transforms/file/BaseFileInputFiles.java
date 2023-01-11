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

package org.apache.hop.pipeline.transforms.file;

import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;

import java.util.ArrayList;
import java.util.List;

/** Input files settings. */
public class BaseFileInputFiles implements Cloneable {
  private static final Class<?> PKG = BaseFileInputFiles.class; // For Translator

  public static final String NO = "N";

  public static final String YES = "Y";

  public static final String[] RequiredFilesCode = new String[] {"N", "Y"};
  public static final String[] RequiredFilesDesc =
      new String[] {
        BaseMessages.getString(PKG, "System.Combo.No"),
        BaseMessages.getString(PKG, "System.Combo.Yes")
      };

  /** Array of filenames */
  @HopMetadataProperty(
      key = "file",
      injectionKey = "FILENAME",
      injectionKeyDescription = "TextFileInput.Injection.FILENAME")
  public List<String> fileName = new ArrayList<>();

  /** Wildcard or filemask (regular expression) */
  @HopMetadataProperty(
      key = "filemask",
      injectionKey = "FILEMASK",
      injectionKeyDescription = "TextFileInput.Injection.FILEMASK")
  public List<String> fileMask = new ArrayList<>();

  /** Wildcard or filemask to exclude (regular expression) */
  @HopMetadataProperty(
      key = "exclude_filemask",
      injectionKey = "EXCLUDE_FILEMASK",
      injectionKeyDescription = "TextFileInput.Injection.EXCLUDE_FILEMASK")
  public List<String> excludeFileMask = new ArrayList<>();

  /** Array of boolean values as string, indicating if a file is required. */
  @HopMetadataProperty(
      key = "file_required",
      injectionKey = "FILE_REQUIRED",
      injectionKeyDescription = "TextFileInput.Injection.FILE_REQUIRED")
  public List<String> fileRequired = new ArrayList<>();

  /** Array of boolean values as string, indicating if we need to fetch sub folders. */
  @HopMetadataProperty(
      key = "include_subfolders",
      injectionKey = "INCLUDE_SUBFOLDERS",
      injectionKeyDescription = "TextFileInput.Injection.INCLUDE_SUBFOLDERS")
  public List<String> includeSubFolders = new ArrayList<>();

  /** Are we accepting filenames in input rows? */
  @HopMetadataProperty(
      key = "accept_filenames",
      injectionKey = "ACCEPT_FILE_NAMES",
      injectionKeyDescription = "TextFileInput.Injection.ACCEPT_FILE_NAMES")
  public boolean acceptingFilenames;

  /** The transformName to accept filenames from */
  @HopMetadataProperty(
      injectionKey = "ACCEPT_FILE_TRANSFORM",
      injectionKeyDescription = "TextFileInput.Injection.ACCEPT_FILE_TRANSFORM")
  public String acceptingTransformName;

  /** If receiving input rows, should we pass through existing fields? */
  @HopMetadataProperty(
      key = "passing_through_fields",
      injectionKey = "PASS_THROUGH_FIELDS",
      injectionKeyDescription = "TextFileInput.Injection.PASS_THROUGH_FIELDS")
  public boolean passingThruFields;

  /** The field in which the filename is placed */
  @HopMetadataProperty(
      key = "accept_field",
      injectionKey = "ACCEPT_FILE_FIELD",
      injectionKeyDescription = "TextFileInput.Injection.ACCEPT_FILE_FIELD")
  public String acceptingField;

  /** The add filenames to result filenames flag */
  @HopMetadataProperty(
      key = "add_to_result_filenames",
      injectionKey = "ADD_FILES_TO_RESULT",
      injectionKeyDescription = "TextFileInput.Injection.ADD_FILES_TO_RESULT")
  public boolean isaddresult;

  @Override
  public Object clone() {
    try {
      BaseFileInputFiles cloned = (BaseFileInputFiles) super.clone();
      cloned.fileName = new ArrayList<>(fileName);
      cloned.fileMask = new ArrayList<>(fileMask);
      cloned.excludeFileMask = new ArrayList<>(excludeFileMask);
      cloned.fileRequired = new ArrayList<>(fileRequired);
      cloned.includeSubFolders = new ArrayList<>(includeSubFolders);
      return cloned;
    } catch (CloneNotSupportedException ex) {
      throw new IllegalArgumentException("Clone not supported for " + this.getClass().getName());
    }
  }

  public void setFileRequired(List<String> fileRequiredin) {
    for (String fRequired : fileRequiredin) {
      this.fileRequired.add(getRequiredFilesCode(fRequired));
    }
  }

  public void setIncludeSubFolders(List<String> includeSubFoldersin) {
    for (String includeSubFolder : includeSubFoldersin) {
      this.includeSubFolders.add(getRequiredFilesCode(includeSubFolder));
    }
  }

  public static String getRequiredFilesCode(String tt) {
    if (tt == null) {
      return RequiredFilesCode[0];
    }
    if (tt.equals(RequiredFilesDesc[1])) {
      return RequiredFilesCode[1];
    } else {
      return RequiredFilesCode[0];
    }
  }

  public void normalizeAllocation() {
    fileMask = normalizeAllocation(fileMask);
    excludeFileMask = normalizeAllocation(excludeFileMask);
    fileRequired = normalizeAllocation(fileRequired);
    includeSubFolders = normalizeAllocation(includeSubFolders);
  }

  protected static List<String> normalizeAllocation(List<String> oldAllocation) {
    return new ArrayList<>(oldAllocation);
  }

  public boolean[] includeSubFolderBoolean() {
    int len = fileName.size();
    boolean[] includeSubFolderBoolean = new boolean[len];
    for (int i = 0; i < len; i++) {
      includeSubFolderBoolean[i] = YES.equalsIgnoreCase(includeSubFolders.get(i));
    }
    return includeSubFolderBoolean;
  }

  public List<ResourceReference> getResourceDependencies(
      IVariables variables, TransformMeta transformMeta) {
    List<ResourceReference> references = new ArrayList<>(5);
    ResourceReference reference = new ResourceReference(transformMeta);
    references.add(reference);

    String[] textFiles =
        FileInputList.createFilePathList(
            variables,
            fileName.toArray(new String[0]),
            fileMask.toArray(new String[0]),
            excludeFileMask.toArray(new String[0]),
            fileRequired.toArray(new String[0]),
            includeSubFolderBoolean());
    if (textFiles != null) {
      for (int i = 0; i < textFiles.length; i++) {
        reference.getEntries().add(new ResourceEntry(textFiles[i], ResourceType.FILE));
      }
    }
    return references;
  }

  public List<String> getFileName() {
    return fileName;
  }

  public void setFileName(List<String> fileName) {
    this.fileName = fileName;
  }

  public List<String> getFileMask() {
    return fileMask;
  }

  public void setFileMask(List<String> fileMask) {
    this.fileMask = fileMask;
  }

  public List<String> getExcludeFileMask() {
    return excludeFileMask;
  }

  public void setExcludeFileMask(List<String> excludeFileMask) {
    this.excludeFileMask = excludeFileMask;
  }

  public List<String> getFileRequired() {
    return fileRequired;
  }

  public List<String> getIncludeSubFolders() {
    return includeSubFolders;
  }

  public boolean isAcceptingFilenames() {
    return acceptingFilenames;
  }

  public void setAcceptingFilenames(boolean acceptingFilenames) {
    this.acceptingFilenames = acceptingFilenames;
  }

  public String getAcceptingTransformName() {
    return acceptingTransformName;
  }

  public void setAcceptingTransformName(String acceptingTransformName) {
    this.acceptingTransformName = acceptingTransformName;
  }

  public boolean isPassingThruFields() {
    return passingThruFields;
  }

  public void setPassingThruFields(boolean passingThruFields) {
    this.passingThruFields = passingThruFields;
  }

  public String getAcceptingField() {
    return acceptingField;
  }

  public void setAcceptingField(String acceptingField) {
    this.acceptingField = acceptingField;
  }

  public boolean isIsaddresult() {
    return isaddresult;
  }

  public void setIsaddresult(boolean isaddresult) {
    this.isaddresult = isaddresult;
  }
}
