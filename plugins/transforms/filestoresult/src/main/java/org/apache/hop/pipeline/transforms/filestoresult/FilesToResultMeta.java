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

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
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

/**
 * @author matt
 * @since 26-may-2006
 */
@Transform(
    id = "FilesToResult",
    image = "filestoresult.svg",
    name = "i18n::BaseTransform.TypeLongDesc.FilesToResult",
    description = "i18n::BaseTransform.TypeTooltipDesc.FilesToResult",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Workflow",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/filestoresult.html")
public class FilesToResultMeta extends BaseTransformMeta
    implements ITransformMeta<FilesToResult, FilesToResultData> {
  private static final Class<?> PKG = FilesToResultMeta.class; // For Translator

  private String filenameField;

  private int fileType;

  /** @return Returns the fieldname that contains the filename. */
  public String getFilenameField() {
    return filenameField;
  }

  /** @param filenameField set the fieldname that contains the filename. */
  public void setFilenameField(String filenameField) {
    this.filenameField = filenameField;
  }

  /**
   * @return Returns the fileType.
   * @see ResultFile
   */
  public int getFileType() {
    return fileType;
  }

  /**
   * @param fileType The fileType to set.
   * @see ResultFile
   */
  public void setFileType(int fileType) {
    this.fileType = fileType;
  }

  public FilesToResultMeta() {
    super(); // allocate BaseTransformMeta
  }

  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode);
  }

  public Object clone() {
    Object retval = super.clone();
    return retval;
  }

  public String getXml() {
    StringBuilder xml = new StringBuilder();

    xml.append(XmlHandler.addTagValue("filename_field", filenameField));
    xml.append(XmlHandler.addTagValue("file_type", ResultFile.getTypeCode(fileType)));

    return xml.toString();
  }

  private void readData(Node transformNode) {
    filenameField = XmlHandler.getTagValue(transformNode, "filename_field");
    fileType = ResultFile.getType(XmlHandler.getTagValue(transformNode, "file_type"));
  }

  public void setDefault() {
    filenameField = null;
    fileType = ResultFile.FILE_TYPE_GENERAL;
  }

  public void getFields(
      IRowMeta rowMeta,
      String origin,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    // Default: nothing changes to rowMeta
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
    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      CheckResult cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "FilesToResultMeta.CheckResult.TransformReceivingInfoFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      CheckResult cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "FilesToResultMeta.CheckResult.NoInputReceivedError"),
              transformMeta);
      remarks.add(cr);
    }
  }

  public FilesToResult createTransform(
      TransformMeta transformMeta,
      FilesToResultData data,
      int cnr,
      PipelineMeta tr,
      Pipeline pipeline) {
    return new FilesToResult(transformMeta, this, data, cnr, tr, pipeline);
  }

  public FilesToResultData getTransformData() {
    return new FilesToResultData();
  }
}
