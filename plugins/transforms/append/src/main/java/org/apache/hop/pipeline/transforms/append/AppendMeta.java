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

import java.util.List;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelineMeta.PipelineType;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformIOMeta;
import org.apache.hop.pipeline.transform.TransformIOMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.stream.IStream;
import org.apache.hop.pipeline.transform.stream.IStream.StreamType;
import org.apache.hop.pipeline.transform.stream.Stream;
import org.apache.hop.pipeline.transform.stream.StreamIcon;

@Transform(
    id = "Append",
    image = "append.svg",
    name = "i18n::Append.Name",
    description = "i18n::Append.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Flow",
    keywords = "i18n::AppendMeta.keyword",
    documentationUrl = "/pipeline/transforms/append.html")
public class AppendMeta extends BaseTransformMeta<Append, AppendData> {

  private static final Class<?> PKG = Append.class;

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
    List<IStream> infoStreams = getTransformIOMeta().getInfoStreams();
    if (infoStreams.size() < 2) {
      return;
    }
    IStream headStream = infoStreams.get(0);
    IStream tailStream = infoStreams.get(1);
    // Respect user choice: only re-fill from stream/prev when they had set a value (e.g. rename).
    boolean hadHeadFilledIn = !Utils.isEmpty(headTransformName);
    boolean hadTailFilledIn = !Utils.isEmpty(tailTransformName);

    if (parentTransformMeta != null) {
      PipelineMeta pipelineMeta = parentTransformMeta.getParentPipelineMeta();
      if (pipelineMeta != null) {
        String[] prev = pipelineMeta.getPrevTransformNames(parentTransformMeta);
        // Only auto-fill when both are empty (initial connect). Do not re-fill when user cleared one.
        if (prev != null && prev.length == 2) {
          if (Utils.isEmpty(headTransformName) && Utils.isEmpty(tailTransformName)) {
            if (headStream.getTransformMeta() != null && tailStream.getTransformMeta() != null) {
              headTransformName = headStream.getTransformName();
              tailTransformName = tailStream.getTransformName();
            } else {
              headTransformName = prev[0];
              tailTransformName = prev[1];
              setChanged();
            }
          }
        }
        // Clear names that no longer exist in prev (e.g. transform was removed)
        if (headTransformName != null && !ArrayUtils.contains(prev, headTransformName)) {
          headTransformName = null;
          setChanged();
        }
        if (tailTransformName != null && !ArrayUtils.contains(prev, tailTransformName)) {
          tailTransformName = null;
          setChanged();
        }
      }
    }

    // Resolve by name. Prefer stream only when the stored name is "stale": empty (auto-fill),
    // not in prev (insert-in-the-middle), or transform not found (rename).
    String[] prev = null;
    if (parentTransformMeta != null && parentTransformMeta.getParentPipelineMeta() != null) {
      prev = parentTransformMeta.getParentPipelineMeta().getPrevTransformNames(parentTransformMeta);
    }
    TransformMeta tmHead = null;
    boolean headNameStale =
        Utils.isEmpty(headTransformName)
            || (prev != null && !ArrayUtils.contains(prev, headTransformName))
            || TransformMeta.findTransform(transforms, headTransformName) == null;
    boolean preferHeadStream =
        headStream.getTransformMeta() != null
            && prev != null
            && ArrayUtils.contains(prev, headStream.getTransformName())
            && headNameStale
            && hadHeadFilledIn;
    if (preferHeadStream) {
      headTransformName = headStream.getTransformName();
      tmHead = headStream.getTransformMeta();
      setChanged();
    }
    if (tmHead == null) {
      tmHead = TransformMeta.findTransform(transforms, headTransformName);
      if (tmHead == null
          && headStream.getTransformMeta() != null
          && prev != null
          && ArrayUtils.contains(prev, headStream.getTransformName())
          && hadHeadFilledIn) {
        headTransformName = headStream.getTransformName();
        tmHead = TransformMeta.findTransform(transforms, headTransformName);
      }
    }
    headStream.setTransformMeta(tmHead);
    if (tmHead != null) {
      headStream.setSubject(tmHead.getName());
    }

    TransformMeta tmTail = null;
    boolean tailNameStale =
        Utils.isEmpty(tailTransformName)
            || (prev != null && !ArrayUtils.contains(prev, tailTransformName))
            || TransformMeta.findTransform(transforms, tailTransformName) == null;
    boolean preferTailStream =
        tailStream.getTransformMeta() != null
            && prev != null
            && ArrayUtils.contains(prev, tailStream.getTransformName())
            && tailNameStale
            && hadTailFilledIn;
    if (preferTailStream) {
      tailTransformName = tailStream.getTransformName();
      tmTail = tailStream.getTransformMeta();
      setChanged();
    }
    if (tmTail == null) {
      tmTail = TransformMeta.findTransform(transforms, tailTransformName);
      if (tmTail == null
          && tailStream.getTransformMeta() != null
          && prev != null
          && ArrayUtils.contains(prev, tailStream.getTransformName())
          && hadTailFilledIn) {
        tailTransformName = tailStream.getTransformName();
        tmTail = TransformMeta.findTransform(transforms, tailTransformName);
      }
    }
    tailStream.setTransformMeta(tmTail);
    if (tmTail != null) {
      tailStream.setSubject(tmTail.getName());
    }
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
    if (info != null && info.length > 0 && info[0] != null) {
      r.mergeRowMeta(info[0]);
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
  public void resetTransformIoMeta() {
    // Do nothing
  }

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

  /**
   * @param headTransformName The headTransformName to set
   */
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

  /**
   * @param tailTransformName The tailTransformName to set
   */
  public void setTailTransformName(String tailTransformName) {
    this.tailTransformName = tailTransformName;
  }
}
