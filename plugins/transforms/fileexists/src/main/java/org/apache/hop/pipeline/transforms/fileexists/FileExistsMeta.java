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

package org.apache.hop.pipeline.transforms.fileexists;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.List;

@Transform(
    id = "FileExists",
    image = "fileexists.svg",
    name = "i18n::BaseTransform.TypeLongDesc.FileExists",
    description = "i18n::BaseTransform.TypeTooltipDesc.FileExists",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Lookup",
    keywords = "i18n::FileExistsMeta.keyword",
    documentationUrl = "/pipeline/transforms/fileexists.html")
public class FileExistsMeta extends BaseTransformMeta<FileExists, FileExistsData> {
  private static final Class<?> PKG = FileExistsMeta.class; // For Translator

  @HopMetadataProperty(
      key = "addresultfilenames",
      injectionKeyDescription = "FileExists.Injection.AddResultFileNames")
  private boolean addresultfilenames;

  /** dynamic filename */
  @HopMetadataProperty(
      key = "filenamefield",
      injectionKeyDescription = "FileExists.Injection.FilenameField")
  private String filenamefield;

  @HopMetadataProperty(
      key = "filetypefieldname",
      injectionKeyDescription = "FileExists.Injection.FileTypeFieldName")
  private String filetypefieldname;

  @HopMetadataProperty(
      key = "includefiletype",
      injectionKeyDescription = "FileExists.Injection.IncludeFileType")
  private boolean includefiletype;

  /** function result: new value name */
  @HopMetadataProperty(
      key = "resultfieldname",
      injectionKeyDescription = "FileExists.Injection.ResultFieldName")
  private String resultfieldname;

  public FileExistsMeta() {
    super(); // allocate BaseTransformMeta
  }

  /** @return Returns the filenamefield. */
  public String getFilenamefield() {
    return filenamefield;
  }

  /** @param filenamefield The filenamefield to set. */
  public void setFilenamefield(String filenamefield) {
    this.filenamefield = filenamefield;
  }

  /** @return Returns the resultName. */
  public String getResultfieldname() {
    return resultfieldname;
  }

  /** @param resultfieldname The resultfieldname to set. */
  public void setResultfieldname(String resultfieldname) {
    this.resultfieldname = resultfieldname;
  }

  /** @param filetypefieldname The filetypefieldname to set. */
  public void setFiletypefieldname(String filetypefieldname) {
    this.filetypefieldname = filetypefieldname;
  }

  /** @return Returns the filetypefieldname. */
  public String getFiletypefieldname() {
    return filetypefieldname;
  }

  public boolean isIncludefiletype() {
    return includefiletype;
  }

  public boolean isAddresultfilenames() {
    return addresultfilenames;
  }

  public void setAddresultfilenames(boolean addresultfilenames) {
    this.addresultfilenames = addresultfilenames;
  }

  public void setIncludefiletype(boolean includefiletype) {
    this.includefiletype = includefiletype;
  }

  @Override
  public Object clone() {
    FileExistsMeta retval = (FileExistsMeta) super.clone();
    return retval;
  }

  @Override
  public void setDefault() {
    resultfieldname = "result";
    filetypefieldname = null;
    includefiletype = false;
    addresultfilenames = false;
  }

  @Override
  public void getFields(
      IRowMeta inputRowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    // Output fields (String)
    if (!Utils.isEmpty(resultfieldname)) {
      IValueMeta v = new ValueMetaBoolean(variables.resolve(resultfieldname));
      v.setOrigin(name);
      inputRowMeta.addValueMeta(v);
    }

    if (includefiletype && !Utils.isEmpty(filetypefieldname)) {
      IValueMeta v = new ValueMetaString(variables.resolve(filetypefieldname));
      v.setOrigin(name);
      inputRowMeta.addValueMeta(v);
    }
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
    CheckResult cr;
    String errorMessage = "";

    if (Utils.isEmpty(resultfieldname)) {
      errorMessage = BaseMessages.getString(PKG, "FileExistsMeta.CheckResult.ResultFieldMissing");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    } else {
      errorMessage = BaseMessages.getString(PKG, "FileExistsMeta.CheckResult.ResultFieldOK");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
      remarks.add(cr);
    }
    if (Utils.isEmpty(filenamefield)) {
      errorMessage = BaseMessages.getString(PKG, "FileExistsMeta.CheckResult.FileFieldMissing");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    } else {
      errorMessage = BaseMessages.getString(PKG, "FileExistsMeta.CheckResult.FileFieldOK");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
      remarks.add(cr);
    }
    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "FileExistsMeta.CheckResult.ReceivingInfoFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "FileExistsMeta.CheckResult.NoInpuReceived"),
              transformMeta);
      remarks.add(cr);
    }
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }
}
