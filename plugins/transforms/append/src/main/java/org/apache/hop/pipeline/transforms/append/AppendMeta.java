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

package org.apache.hop.pipeline.transforms.append;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelineMeta.PipelineType;
import org.apache.hop.pipeline.transform.*;
import org.apache.hop.pipeline.transform.stream.IStream;
import org.apache.hop.pipeline.transform.stream.IStream.StreamType;
import org.apache.hop.pipeline.transform.stream.Stream;
import org.apache.hop.pipeline.transform.stream.StreamIcon;

import java.util.List;

@Transform(
    id = "Append",
    image = "append.svg",
    name = "i18n::Append.Name",
    description = "i18n::Append.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Flow",
    keywords = "i18n::AppendMeta.keyword",
    documentationUrl = "/pipeline/transforms/append.html")
public class AppendMeta extends BaseTransformMeta<Append, AppendData> {

  private static final Class<?> PKG = Append.class; // For Translator

  @HopMetadataProperty(
      key = "head_name",
      injectionKey = "HEAD_TRANSFORM",
      injectionKeyDescription = "AppendMeta.Injection.HEAD_TRANSFORM")
  public String headTransformName;

  @HopMetadataProperty(
      key = "tail_name",
      injectionKey = "TAIL_TRANSFORM",
      injectionKeyDescription = "AppendMeta.Injection.TAIL_TRANSFORM")
  public String tailTransformName;

  public AppendMeta() {
    super(); // allocate BaseTransformMeta
  }

  @Override
  public void convertIOMetaToTransformNames() {
    List<IStream> streams = getTransformIOMeta().getInfoStreams();
    headTransformName = streams.get(0).getTransformName();
    tailTransformName = streams.get(1).getTransformName();
  }

  @Override
  public void searchInfoAndTargetTransforms(List<TransformMeta> transforms) {
    List<IStream> streams = getTransformIOMeta().getInfoStreams();
    streams.get(0).setTransformMeta(TransformMeta.findTransform(transforms, headTransformName));
    streams.get(1).setTransformMeta(TransformMeta.findTransform(transforms, tailTransformName));
  }

  @Override
  public void getFields(
      IRowMeta r,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    // We don't have any input fields here in "r" as they are all info fields.
    // So we just take the info fields.
    //
    if (info != null) {
      if (info.length > 0 && info[0] != null) {
        r.mergeRowMeta(info[0]);
      }
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

    List<IStream> infoStreams = getTransformIOMeta().getInfoStreams();
    IStream headStream = infoStreams.get(0);
    IStream tailStream = infoStreams.get(1);

    if (headStream.getTransformName() != null && tailStream.getTransformName() != null) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "AppendMeta.CheckResult.SourceTransformsOK"),
              transformMeta);
      remarks.add(cr);
    } else if (headStream.getTransformName() == null && tailStream.getTransformName() == null) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "AppendMeta.CheckResult.SourceTransformsMissing"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "AppendMeta.CheckResult.OneSourceTransformMissing"),
              transformMeta);
      remarks.add(cr);
    }
  }

  /** Returns the Input/Output metadata for this transform. */
  @Override
  public ITransformIOMeta getTransformIOMeta() {
    ITransformIOMeta ioMeta = super.getTransformIOMeta(false);
    if (ioMeta == null) {

      ioMeta = new TransformIOMeta(true, true, false, false, false, false);

      IStream headStream =
          new Stream(
              StreamType.INFO,
              null,
              BaseMessages.getString(PKG, "AppendMeta.InfoStream.FirstStream.Description"),
              StreamIcon.INFO,
              null);
      ioMeta.addStream(headStream);

      IStream tailStream =
          new Stream(
              StreamType.INFO,
              null,
              BaseMessages.getString(PKG, "AppendMeta.InfoStream.SecondStream.Description"),
              StreamIcon.INFO,
              null);
      ioMeta.addStream(tailStream);

      setTransformIOMeta(ioMeta);
    }

    return ioMeta;
  }

  @Override
  public void resetTransformIoMeta() {}

  @Override
  public PipelineType[] getSupportedPipelineTypes() {
    return new PipelineType[] {
      PipelineType.Normal,
    };
  }

  /**
   * Gets headTransformName
   *
   * @return value of headTransformName
   */
  public String getHeadTransformName() {
    return headTransformName;
  }

  /** @param headTransformName The headTransformName to set */
  public void setHeadTransformName(String headTransformName) {
    this.headTransformName = headTransformName;
  }

  /**
   * Gets tailTransformName
   *
   * @return value of tailTransformName
   */
  public String getTailTransformName() {
    return tailTransformName;
  }

  /** @param tailTransformName The tailTransformName to set */
  public void setTailTransformName(String tailTransformName) {
    this.tailTransformName = tailTransformName;
  }
}
