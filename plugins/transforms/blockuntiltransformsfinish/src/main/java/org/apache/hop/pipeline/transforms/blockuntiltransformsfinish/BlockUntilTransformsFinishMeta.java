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

package org.apache.hop.pipeline.transforms.blockuntiltransformsfinish;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelineMeta.PipelineType;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.ArrayList;
import java.util.List;

@Transform(
    id = "BlockUntilTransformsFinish",
    image = "blockinguntiltransformsfinish.svg",
    name = "i18n::BaseTransform.TypeLongDesc.BlockUntilTransformsFinish",
    description = "i18n::BaseTransform.TypeLongDesc.BlockUntilTransformsFinish",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Flow",
    documentationUrl = "/pipeline/transforms/blockuntiltransformsfinish.html")
public class BlockUntilTransformsFinishMeta extends BaseTransformMeta
    implements ITransformMeta<BlockUntilTransformsFinish, BlockUntilTransformsFinishData> {

  private static final Class<?> PKG = BlockUntilTransformsFinishMeta.class; // For Translator

  @HopMetadataProperty(groupKey = "transforms", key = "transform")
  private List<BlockingTransform> blockingTransforms;

  public BlockUntilTransformsFinishMeta() {
    blockingTransforms = new ArrayList<>();
  }

  @Override
  public BlockUntilTransformsFinishMeta clone() {
    BlockUntilTransformsFinishMeta meta = new BlockUntilTransformsFinishMeta();
    for (BlockingTransform blockingTransform : blockingTransforms) {
      meta.blockingTransforms.add(new BlockingTransform(blockingTransform));
    }
    return meta;
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

    if (prev == null || prev.size() == 0) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_WARNING,
              BaseMessages.getString(
                  PKG, "BlockUntilTransformsFinishMeta.CheckResult.NotReceivingFields"),
              transformMeta);
    } else {
      if (blockingTransforms.size() > 0) {
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

  @Override
  public BlockUntilTransformsFinish createTransform(
      TransformMeta transformMeta,
      BlockUntilTransformsFinishData data,
      int cnr,
      PipelineMeta tr,
      Pipeline pipeline) {
    return new BlockUntilTransformsFinish(transformMeta, this, data, cnr, tr, pipeline);
  }

  @Override
  public BlockUntilTransformsFinishData getTransformData() {
    return new BlockUntilTransformsFinishData();
  }

  @Override
  public PipelineType[] getSupportedPipelineTypes() {
    return new PipelineType[] {
      PipelineType.Normal,
    };
  }

  /**
   * Gets blockingTransforms
   *
   * @return value of blockingTransforms
   */
  public List<BlockingTransform> getBlockingTransforms() {
    return blockingTransforms;
  }

  /** @param blockingTransforms The blockingTransforms to set */
  public void setBlockingTransforms(List<BlockingTransform> blockingTransforms) {
    this.blockingTransforms = blockingTransforms;
  }
}
