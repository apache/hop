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
 *
 */

package org.apache.hop.core.fileinput;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;

@Getter
@Setter
public class InputFile {
  /** Array of filenames */
  @HopMetadataProperty(
      key = "name",
      injectionKey = "FILENAME",
      injectionKeyDescription = "TextFileInput.Injection.FILENAME")
  private String fileName;

  /** Wildcard or filemask (regular expression) */
  @HopMetadataProperty(
      key = "filemask",
      injectionKey = "FILEMASK",
      injectionKeyDescription = "TextFileInput.Injection.FILEMASK")
  private String fileMask;

  /** Wildcard or filemask to exclude (regular expression) */
  @HopMetadataProperty(
      key = "exclude_filemask",
      injectionKey = "EXCLUDE_FILEMASK",
      injectionKeyDescription = "TextFileInput.Injection.EXCLUDE_FILEMASK")
  private String excludeFileMask;

  /** Array of boolean values as string, indicating if a file is required. */
  @HopMetadataProperty(
      key = "file_required",
      injectionKey = "FILE_REQUIRED",
      injectionKeyDescription = "TextFileInput.Injection.FILE_REQUIRED")
  private boolean fileRequired;

  /** Array of boolean values as string, indicating if we need to fetch sub folders. */
  @HopMetadataProperty(
      key = "include_subfolders",
      injectionKey = "INCLUDE_SUBFOLDERS",
      injectionKeyDescription = "TextFileInput.Injection.INCLUDE_SUBFOLDERS")
  private boolean includeSubFolders;

  @HopMetadataProperty(key = "type_filter", storeWithCode = true)
  private FileTypeFilter fileTypeFilter;

  public InputFile() {
    this.fileTypeFilter = null;
  }

  public InputFile(InputFile f) {
    this();
    this.fileName = f.fileName;
    this.fileMask = f.fileMask;
    this.excludeFileMask = f.excludeFileMask;
    this.includeSubFolders = f.includeSubFolders;
    this.fileRequired = f.fileRequired;
    this.fileTypeFilter = f.fileTypeFilter;
  }

  public String getFileRequiredDesc() {
    if (fileRequired) {
      return BaseMessages.getString("System.Combo.Yes");
    } else {
      return BaseMessages.getString("System.Combo.No");
    }
  }

  public String getIncludeSubFoldersDesc() {
    if (fileRequired) {
      return BaseMessages.getString("System.Combo.Yes");
    } else {
      return BaseMessages.getString("System.Combo.No");
    }
  }
}
