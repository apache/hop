/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hop.pipeline.transforms.xml.getxmldata;

import java.util.Objects;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.util.Utils;
import org.apache.hop.metadata.api.HopMetadataProperty;

@Getter
@Setter
public class GetXmlFileItem {

  private static final String NO = "N";

  /** Array of filenames */
  @HopMetadataProperty(
      key = "name",
      injectionKeyDescription = "GetXmlDataMeta.Injection.Filename.Label")
  private String fileName;

  /** Wildcard or filemask (regular expression) */
  @HopMetadataProperty(
      key = "filemask",
      injectionKeyDescription = "GetXmlDataMeta.Injection.Filemask.Label")
  private String fileMask;

  /** Wildcard or filemask to exclude (regular expression) */
  @HopMetadataProperty(
      key = "exclude_filemask",
      injectionKeyDescription = "GetXmlDataMeta.Injection.ExcludeFilemask.Label")
  private String excludeFileMask;

  /** Array of boolean values as string, indicating if a file is required. */
  @HopMetadataProperty(
      key = "file_required",
      injectionKeyDescription = "GetXmlDataMeta.Injection.FileRequired.Label")
  private String fileRequired;

  /** Array of boolean values as string, indicating if we need to fetch sub folders. */
  @HopMetadataProperty(
      key = "include_subfolders",
      injectionKeyDescription = "GetXmlDataMeta.Injection.IncludeSubDirs.Label")
  private String includeSubFolders;

  public GetXmlFileItem() {
    setDefault();
  }

  public GetXmlFileItem(
      String fileName,
      String fileMask,
      String excludeFileMask,
      String fileRequired,
      String includeSubFolders) {
    this.fileName = fileName;
    this.fileMask = fileMask;
    this.excludeFileMask = excludeFileMask;
    this.fileRequired = Utils.isEmpty(fileRequired) ? NO : fileRequired;
    this.includeSubFolders = Utils.isEmpty(includeSubFolders) ? NO : includeSubFolders;
  }

  protected void setDefault() {
    this.fileRequired = NO;
    this.includeSubFolders = NO;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GetXmlFileItem getXmlFileItems = (GetXmlFileItem) o;
    return fileName.equals(getXmlFileItems.fileName)
        && Objects.equals(fileMask, getXmlFileItems.fileMask)
        && Objects.equals(excludeFileMask, getXmlFileItems.excludeFileMask);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fileName, fileMask, excludeFileMask);
  }
}
