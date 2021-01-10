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
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

import java.util.List;

/*
 * Created on 03-Juin-2008
 *
 */

@Transform(
    id = "FileExists",
    image = "fileexists.svg",
    name = "i18n::BaseTransform.TypeLongDesc.FileExists",
    description = "i18n::BaseTransform.TypeTooltipDesc.FileExists",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Lookup",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/fileexists.html")
public class FileExistsMeta extends BaseTransformMeta
    implements ITransformMeta<FileExists, FileExistsData> {
  private static final Class<?> PKG = FileExistsMeta.class; // For Translator

  private boolean addresultfilenames;

  /** dynamic filename */
  private String filenamefield;

  private String filetypefieldname;

  private boolean includefiletype;

  /** function result: new value name */
  private String resultfieldname;

  public FileExistsMeta() {
    super(); // allocate BaseTransformMeta
  }

  /** @return Returns the filenamefield. */
  public String getDynamicFilenameField() {
    return filenamefield;
  }

  /** @param filenamefield The filenamefield to set. */
  public void setDynamicFilenameField(String filenamefield) {
    this.filenamefield = filenamefield;
  }

  /** @return Returns the resultName. */
  public String getResultFieldName() {
    return resultfieldname;
  }

  /** @param resultfieldname The resultfieldname to set. */
  public void setResultFieldName(String resultfieldname) {
    this.resultfieldname = resultfieldname;
  }

  /** @param filetypefieldname The filetypefieldname to set. */
  public void setFileTypeFieldName(String filetypefieldname) {
    this.filetypefieldname = filetypefieldname;
  }

  /** @return Returns the filetypefieldname. */
  public String getFileTypeFieldName() {
    return filetypefieldname;
  }

  public boolean includeFileType() {
    return includefiletype;
  }

  public boolean addResultFilenames() {
    return addresultfilenames;
  }

  public void setaddResultFilenames(boolean addresultfilenames) {
    this.addresultfilenames = addresultfilenames;
  }

  public void setincludeFileType(boolean includefiletype) {
    this.includefiletype = includefiletype;
  }

  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode, metadataProvider);
  }

  public Object clone() {
    FileExistsMeta retval = (FileExistsMeta) super.clone();

    return retval;
  }

  public void setDefault() {
    resultfieldname = "result";
    filetypefieldname = null;
    includefiletype = false;
    addresultfilenames = false;
  }

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

  public String getXml() {
    StringBuilder retval = new StringBuilder();

    retval.append("    " + XmlHandler.addTagValue("filenamefield", filenamefield));
    retval.append("    " + XmlHandler.addTagValue("resultfieldname", resultfieldname));
    retval.append("    ").append(XmlHandler.addTagValue("includefiletype", includefiletype));
    retval.append("    " + XmlHandler.addTagValue("filetypefieldname", filetypefieldname));
    retval.append("    ").append(XmlHandler.addTagValue("addresultfilenames", addresultfilenames));
    return retval.toString();
  }

  private void readData(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    try {
      filenamefield = XmlHandler.getTagValue(transformNode, "filenamefield");
      resultfieldname = XmlHandler.getTagValue(transformNode, "resultfieldname");
      includefiletype =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "includefiletype"));
      filetypefieldname = XmlHandler.getTagValue(transformNode, "filetypefieldname");
      addresultfilenames =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "addresultfilenames"));
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "FileExistsMeta.Exception.UnableToReadTransformMeta"), e);
    }
  }

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
      cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    } else {
      errorMessage = BaseMessages.getString(PKG, "FileExistsMeta.CheckResult.ResultFieldOK");
      cr = new CheckResult(CheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
      remarks.add(cr);
    }
    if (Utils.isEmpty(filenamefield)) {
      errorMessage = BaseMessages.getString(PKG, "FileExistsMeta.CheckResult.FileFieldMissing");
      cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    } else {
      errorMessage = BaseMessages.getString(PKG, "FileExistsMeta.CheckResult.FileFieldOK");
      cr = new CheckResult(CheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
      remarks.add(cr);
    }
    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "FileExistsMeta.CheckResult.ReceivingInfoFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "FileExistsMeta.CheckResult.NoInpuReceived"),
              transformMeta);
      remarks.add(cr);
    }
  }

  public FileExists createTransform(
      TransformMeta transformMeta,
      FileExistsData data,
      int cnr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new FileExists(transformMeta, this, data, cnr, pipelineMeta, pipeline);
  }

  public FileExistsData getTransformData() {
    return new FileExistsData();
  }

  public boolean supportsErrorHandling() {
    return true;
  }
}
