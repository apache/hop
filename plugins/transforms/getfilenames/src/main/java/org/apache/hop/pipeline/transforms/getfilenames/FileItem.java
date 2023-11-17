/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.getfilenames;

import java.util.Objects;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IEnumHasCode;

public class FileItem {

  private static final Class<?> PKG = FileItem.class;

  public enum YesNoType implements IEnumHasCode {
    YES("Y", BaseMessages.getString(PKG, "System.Combo.Yes")),
    NO("N", BaseMessages.getString(PKG, "System.Combo.No"));

    private String code;
    private String description;

    YesNoType(String code, String description) {
      this.code = code;
      this.description = description;
    }


    public String getCode() {
      return code;
    }

    public static final String[] getDescriptions() {
      String[] items = new String[values().length];
      for (int i = 0; i < items.length; i++) {
        items[i] = values()[i].description;
      }
      return items;
    }

    public static YesNoType getType(String value) {
      for (YesNoType type : values()) {
        if (value.equalsIgnoreCase(type.description)) {
          return type;
        }
      }
      return null;
    }
    
    public String getDescription() {
      return description;
    }
  }

  /** Array of filenames */
  @HopMetadataProperty(
      key = "name",
      injectionKeyDescription = "GetFileNames.Injection.Filename.Label")
  private String fileName;

  /** Wildcard or filemask (regular expression) */
  @HopMetadataProperty(
      key = "filemask",
      injectionKeyDescription = "GetFileNames.Injection.Filemask.Label")
  private String fileMask;

  /** Wildcard or filemask to exclude (regular expression) */
  @HopMetadataProperty(
      key = "exclude_filemask",
      injectionKeyDescription = "GetFileNames.Injection.ExcludeFilemask.Label")
  private String excludeFileMask;

  /** Array of boolean values as string, indicating if a file is required. */
  @HopMetadataProperty(
      key = "file_required",
      storeWithCode = true,
      injectionKeyDescription = "GetFileNames.Injection.FileRequired.Label")
  private YesNoType fileRequired;

  /** Array of boolean values as string, indicating if we need to fetch sub folders. */
  @HopMetadataProperty(
      key = "include_subfolders",
      storeWithCode = true,
      injectionKeyDescription = "GetFileNames.Injection.IncludeSubDirs.Label")
  private YesNoType includeSubFolders;

  public FileItem() {
    setDefault();
  }

  public FileItem(
      String fileName,
      String fileMask,
      String excludeFileMask,
      YesNoType fileRequired,
      YesNoType includeSubFolders) {
    this.fileName = fileName;
    this.fileMask = fileMask;
    this.excludeFileMask = excludeFileMask;
    this.includeSubFolders = includeSubFolders;
    this.fileRequired = fileRequired;
  }

  protected void setDefault() {
    this.fileRequired = YesNoType.NO;
    this.includeSubFolders = YesNoType.NO;
  }

  public String getFileName() {
    return fileName;
  }

  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  public String getFileMask() {
    return fileMask;
  }

  public void setFileMask(String fileMask) {
    this.fileMask = fileMask;
  }

  public String getExcludeFileMask() {
    return excludeFileMask;
  }

  public void setExcludeFileMask(String excludeFileMask) {
    this.excludeFileMask = excludeFileMask;
  }

  public YesNoType getFileRequired() {
    return fileRequired;
  }

  public void setFileRequired(YesNoType fileRequired) {
    this.fileRequired = fileRequired;
  }

  public YesNoType getIncludeSubFolders() {
    return includeSubFolders;
  }

  public void setIncludeSubFolders(YesNoType includeSubFolders) {
    this.includeSubFolders = includeSubFolders;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    FileItem fileItems = (FileItem) o;
    return fileName.equals(fileItems.fileName)
        && Objects.equals(fileMask, fileItems.fileMask)
        && Objects.equals(excludeFileMask, fileItems.excludeFileMask);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fileName, fileMask, excludeFileMask);
  }
}
