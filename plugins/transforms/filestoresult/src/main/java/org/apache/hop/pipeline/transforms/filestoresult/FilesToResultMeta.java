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

package org.apache.hop.pipeline.transforms.filestoresult;

import static org.apache.hop.core.ResultFile.FileType;

import java.util.List;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "FilesToResult",
    image = "filestoresult.svg",
    name = "i18n::FilesToResult.Name",
    description = "i18n::FilesToResult.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Workflow",
    keywords = "i18n::FilesToResultMeta.keyword",
    documentationUrl = "/pipeline/transforms/filestoresult.html")
public class FilesToResultMeta extends BaseTransformMeta<FilesToResult, FilesToResultData> {
  private static final Class<?> PKG = FilesToResultMeta.class;

  @HopMetadataProperty(key = "filename_field")
  private String filenameField;

  @HopMetadataProperty(key = "file_type", storeWithCode = true)
  private FileType fileType;

  public FilesToResultMeta() {
    super(); // allocate BaseTransformMeta
    fileType = FileType.GENERAL;
  }

  public FilesToResultMeta(FilesToResultMeta m) {
    this.filenameField = m.filenameField;
    this.fileType = m.fileType;
  }

  @Override
  public FilesToResultMeta clone() {
    return new FilesToResultMeta(this);
  }

  @Override
  public void setDefault() {
    filenameField = null;
    fileType = FileType.GENERAL;
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      IRowMeta prev,
      String[] input,
      String[] output,
      IRowMeta info,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "FilesToResultMeta.CheckResult.TransformReceivingInfoFromOtherTransforms"),
              transformMeta));
    } else {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "FilesToResultMeta.CheckResult.NoInputReceivedError"),
              transformMeta));
    }
  }

  /**
   * @return Returns the fieldname that contains the filename.
   */
  public String getFilenameField() {
    return filenameField;
  }

  /**
   * @param filenameField set the fieldname that contains the filename.
   */
  public void setFilenameField(String filenameField) {
    this.filenameField = filenameField;
  }

  /**
   * Gets fileType
   *
   * @return value of fileType
   */
  public FileType getFileType() {
    return fileType;
  }

  /**
   * Sets fileType
   *
   * @param fileType value of fileType
   */
  public void setFileType(FileType fileType) {
    this.fileType = fileType;
  }
}
