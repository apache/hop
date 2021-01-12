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

package org.apache.hop.pipeline.transforms.blockuntiltransformsfinish;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
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
import org.apache.hop.pipeline.PipelineMeta.PipelineType;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

import java.util.List;

/*
 * Created on 30-06-2008
 *
 */

@Transform(
    id = "BlockUntilTransformsFinish",
    image = "blockinguntiltransformsfinish.svg",
    name = "i18n::BaseTransform.TypeLongDesc.BlockUntilTransformsFinish",
    description = "i18n::BaseTransform.TypeLongDesc.BlockUntilTransformsFinish",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Flow",
    documentationUrl =
        "https://hop.apache.org/manual/latest/plugins/transforms/blockuntiltransformsfinish.html")
public class BlockUntilTransformsFinishMeta extends BaseTransformMeta
    implements ITransformMeta<BlockUntilTransformsFinish, BlockUntilTransformsFinishData> {

  private static final Class<?> PKG = BlockUntilTransformsFinishMeta.class; // For Translator

  /** by which transforms to display? */
  private String[] transformName;

  private String[] transformCopyNr;

  public BlockUntilTransformsFinishMeta() {
    super(); // allocate BaseTransformMeta
  }

  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode);
  }

  public Object clone() {
    BlockUntilTransformsFinishMeta retval = (BlockUntilTransformsFinishMeta) super.clone();

    int nrFields = transformName.length;

    retval.allocate(nrFields);
    System.arraycopy(transformName, 0, retval.transformName, 0, nrFields);
    System.arraycopy(transformCopyNr, 0, retval.transformCopyNr, 0, nrFields);
    return retval;
  }

  public void allocate(int nrFields) {
    transformName = new String[nrFields];
    transformCopyNr = new String[nrFields];
  }

  /** @return Returns the transformName. */
  public String[] getTransformName() {
    return transformName;
  }

  /** @return Returns the transformCopyNr. */
  public String[] getTransformCopyNr() {
    return transformCopyNr;
  }

  /** @param transformName The transformName to set. */
  public void setTransformName(String[] transformName) {
    this.transformName = transformName;
  }

  /** @param transformCopyNr The transformCopyNr to set. */
  public void setTransformCopyNr(String[] transformCopyNr) {
    this.transformCopyNr = transformCopyNr;
  }

  public void getFields(
      IRowMeta r,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {}

  private void readData(Node transformNode) throws HopXmlException {
    try {
      Node transforms = XmlHandler.getSubNode(transformNode, "transforms");
      int nrTransforms = XmlHandler.countNodes(transforms, "transform");

      allocate(nrTransforms);

      for (int i = 0; i < nrTransforms; i++) {
        Node fnode = XmlHandler.getSubNodeByNr(transforms, "transform", i);
        transformName[i] = XmlHandler.getTagValue(fnode, "name");
        transformCopyNr[i] = XmlHandler.getTagValue(fnode, "CopyNr");
      }
    } catch (Exception e) {
      throw new HopXmlException("Unable to load transform info from XML", e);
    }
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder();

    retval.append("    <transforms>" + Const.CR);
    for (int i = 0; i < transformName.length; i++) {
      retval.append("      <transform>" + Const.CR);
      retval.append("        " + XmlHandler.addTagValue("name", transformName[i]));
      retval.append("        " + XmlHandler.addTagValue("CopyNr", transformCopyNr[i]));

      retval.append("        </transform>" + Const.CR);
    }
    retval.append("      </transforms>" + Const.CR);

    return retval.toString();
  }

  public void setDefault() {
    int nrTransforms = 0;

    allocate(nrTransforms);

    for (int i = 0; i < nrTransforms; i++) {
      transformName[i] = "transform" + i;
      transformCopyNr[i] = "CopyNr" + i;
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

    if (prev == null || prev.size() == 0) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_WARNING,
              BaseMessages.getString(
                  PKG, "BlockUntilTransformsFinishMeta.CheckResult.NotReceivingFields"),
              transformMeta);
    } else {
      if (transformName.length > 0) {
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(
                    PKG, "BlockUntilTransformsFinishMeta.CheckResult.AllTransformsFound"),
                transformMeta);
      } else {
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_WARNING,
                BaseMessages.getString(
                    PKG, "BlockUntilTransformsFinishMeta.CheckResult.NoTransformsEntered"),
                transformMeta);
      }
    }
    remarks.add(cr);

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "BlockUntilTransformsFinishMeta.CheckResult.TransformRecevingData2"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG,
                  "BlockUntilTransformsFinishMeta.CheckResult.NoInputReceivedFromOtherTransforms"),
              transformMeta);
    }
    remarks.add(cr);
  }

  public BlockUntilTransformsFinish createTransform(
      TransformMeta transformMeta,
      BlockUntilTransformsFinishData data,
      int cnr,
      PipelineMeta tr,
      Pipeline pipeline) {
    return new BlockUntilTransformsFinish(transformMeta, this, data, cnr, tr, pipeline);
  }

  public BlockUntilTransformsFinishData getTransformData() {
    return new BlockUntilTransformsFinishData();
  }

  public PipelineType[] getSupportedPipelineTypes() {
    return new PipelineType[] {
      PipelineType.Normal,
    };
  }
}
