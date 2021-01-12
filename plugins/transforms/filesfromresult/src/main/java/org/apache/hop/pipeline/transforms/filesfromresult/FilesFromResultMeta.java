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

package org.apache.hop.pipeline.transforms.filesfromresult;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
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
 * Created on 02-jun-2003
 *
 */

@Transform(
    id = "FilesFromResult",
    image = "filesfromresult.svg",
    name = "i18n::BaseTransform.TypeLongDesc.FilesFromResult",
    description = "i18n::BaseTransform.TypeTooltipDesc.FilesFromResult",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Workflow",
    documentationUrl =
        "https://hop.apache.org/manual/latest/plugins/transforms/filesfromresult.html")
public class FilesFromResultMeta extends BaseTransformMeta
    implements ITransformMeta<FilesFromResult, FilesFromResultData> {
  private static final Class<?> PKG = FilesFromResult.class; // For Translator

  public FilesFromResultMeta() {
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

  private void readData(Node transformNode) {}

  public void setDefault() {}

  public void getFields(
      IRowMeta r,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {

    // Add the fields from a ResultFile
    try {
      ResultFile resultFile =
          new ResultFile(
              ResultFile.FILE_TYPE_GENERAL,
              HopVfs.getFileObject("foo.bar"),
              "parentOrigin",
              "origin");
      RowMetaAndData add = resultFile.getRow();

      // Set the origin on the fields...
      for (int i = 0; i < add.size(); i++) {
        add.getValueMeta(i).setOrigin(name);
      }
      r.addRowMeta(add.getRowMeta());
    } catch (HopFileException e) {
      throw new HopTransformException(e);
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
    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      CheckResult cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG,
                  "FilesFromResultMeta.CheckResult.TransformExpectingNoReadingInfoFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      CheckResult cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "FilesFromResultMeta.CheckResult.NoInputReceivedError"),
              transformMeta);
      remarks.add(cr);
    }
  }

  public FilesFromResult createTransform(
      TransformMeta transformMeta,
      FilesFromResultData data,
      int cnr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new FilesFromResult(transformMeta, this, data, cnr, pipelineMeta, pipeline);
  }

  public FilesFromResultData getTransformData() {
    return new FilesFromResultData();
  }
}
