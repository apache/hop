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

package org.apache.hop.pipeline.transforms.mergejoin;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelineMeta.PipelineType;
import org.apache.hop.pipeline.transform.*;
import org.apache.hop.pipeline.transform.errorhandling.IStream;
import org.apache.hop.pipeline.transform.errorhandling.IStream.StreamType;
import org.apache.hop.pipeline.transform.errorhandling.Stream;
import org.apache.hop.pipeline.transform.errorhandling.StreamIcon;

import java.util.ArrayList;
import java.util.List;

@Transform(
    id = "MergeJoin",
    image = "mergejoin.svg",
    name = "i18n::BaseTransform.TypeLongDesc.MergeJoin",
    description = "i18n::BaseTransform.TypeTooltipDesc.MergeJoin",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Joins",
        keywords = "i18n::MergeJoinMeta.keyword",
    documentationUrl = "/pipeline/transforms/mergejoin.html")
public class MergeJoinMeta extends BaseTransformMeta
    implements ITransformMeta<MergeJoin, MergeJoinData> {
  private static final Class<?> PKG = MergeJoinMeta.class; // For Translator

  public static final String[] joinTypes = {"INNER", "LEFT OUTER", "RIGHT OUTER", "FULL OUTER"};
  public static final boolean[] one_optionals = {false, false, true, true};
  public static final boolean[] two_optionals = {false, true, false, true};

  @HopMetadataProperty(
      key = "join_type",
      injectionKey = "JOIN_TYPE",
      injectionKeyDescription = "MergeJoin.Injection.JOIN_TYPE")
  private String joinType;

  @HopMetadataProperty(
      key = "transform1",
      injectionKey = "LEFT_TRANSFORM",
      injectionKeyDescription = "MergeJoin.Injection.LEFT_TRANSFORM")
  private String leftTransformName;

  @HopMetadataProperty(
      key = "transform2",
      injectionKey = "RIGHT_TRANSFORM",
      injectionKeyDescription = "MergeJoin.Injection.RIGHT_TRANSFORM")
  private String rightTransformName;

  @HopMetadataProperty(
      groupKey = "keys_1",
      key = "key",
      injectionGroupKey = "KEY_FIELDS1",
      injectionGroupDescription = "MergeJoin.Injection.KEY_FIELDS1",
      injectionKey = "KEY_FIELD1",
      injectionKeyDescription = "MergeJoin.Injection.KEY_FIELD1")
  private List<String> keyFields1;

  @HopMetadataProperty(
      groupKey = "keys_2",
      key = "key",
      injectionGroupKey = "KEY_FIELDS2",
      injectionGroupDescription = "MergeJoin.Injection.KEY_FIELDS2",
      injectionKey = "KEY_FIELD2",
      injectionKeyDescription = "MergeJoin.Injection.KEY_FIELD2")
  private List<String> keyFields2;

  @Override
  public boolean excludeFromRowLayoutVerification() {
    return true;
  }

  @Override
  public MergeJoin createTransform(
      TransformMeta transformMeta,
      MergeJoinData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new MergeJoin(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  public MergeJoinMeta() {
    keyFields1 = new ArrayList<>();
    keyFields2 = new ArrayList<>();
  }

  @Override
  public MergeJoinMeta clone() {
    MergeJoinMeta meta = new MergeJoinMeta();

    meta.leftTransformName = this.leftTransformName;
    meta.rightTransformName = this.rightTransformName;
    meta.joinType = this.joinType;
    meta.keyFields1.addAll(this.keyFields1);
    meta.keyFields2.addAll(this.keyFields2);

    return meta;
  }

  @Override
  public void searchInfoAndTargetTransforms(List<TransformMeta> transforms) {
    List<IStream> infoStreams = getTransformIOMeta().getInfoStreams();
    infoStreams.get(0).setTransformMeta(TransformMeta.findTransform(transforms, leftTransformName));
    infoStreams
        .get(1)
        .setTransformMeta(TransformMeta.findTransform(transforms, rightTransformName));
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
    /*
     * @todo Need to check for the following: 1) Join type must be one of INNER / LEFT OUTER / RIGHT OUTER / FULL OUTER
     * 2) Number of input streams must be two (for now at least) 3) The field names of input streams must be unique
     */
    CheckResult cr =
        new CheckResult(
            ICheckResult.TYPE_RESULT_WARNING,
            BaseMessages.getString(PKG, "MergeJoinMeta.CheckResult.TransformNotVerified"),
            transformMeta);
    remarks.add(cr);
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
    // So we just merge in the info fields.
    //
    if (info != null) {
      for (int i = 0; i < info.length; i++) {
        if (info[i] != null) {
          r.mergeRowMeta(info[i], name);
        }
      }
    }

    for (int i = 0; i < r.size(); i++) {
      IValueMeta vmi = r.getValueMeta(i);
      if (vmi != null && Utils.isEmpty(vmi.getName())) {
        vmi.setOrigin(name);
      }
    }
  }

  public ITransform getTransform(
      TransformMeta transformMeta,
      MergeJoinData data,
      int cnr,
      PipelineMeta tr,
      Pipeline pipeline) {
    return new MergeJoin(transformMeta, this, data, cnr, tr, pipeline);
  }

  @Override
  public MergeJoinData getTransformData() {
    return new MergeJoinData();
  }

  /**
   * Returns the Input/Output metadata for this transform. The generator transform only produces
   * output, does not accept input!
   */
  @Override
  public ITransformIOMeta getTransformIOMeta() {
    ITransformIOMeta ioMeta = super.getTransformIOMeta(false);
    if (ioMeta == null) {

      ioMeta = new TransformIOMeta(true, true, false, false, false, false);

      ioMeta.addStream(
          new Stream(
              StreamType.INFO,
              null,
              BaseMessages.getString(PKG, "MergeJoinMeta.InfoStream.FirstStream.Description"),
              StreamIcon.INFO,
              null));
      ioMeta.addStream(
          new Stream(
              StreamType.INFO,
              null,
              BaseMessages.getString(PKG, "MergeJoinMeta.InfoStream.SecondStream.Description"),
              StreamIcon.INFO,
              null));
      setTransformIOMeta(ioMeta);
    }

    return ioMeta;
  }

  @Override
  public void resetTransformIoMeta() {
    // Don't reset!
  }

  @Override
  public PipelineType[] getSupportedPipelineTypes() {
    return new PipelineType[] {
      PipelineType.Normal,
    };
  }

  /**
   * Gets joinType
   *
   * @return value of joinType
   */
  public String getJoinType() {
    return joinType;
  }

  /** @param joinType The joinType to set */
  public void setJoinType(String joinType) {
    this.joinType = joinType;
  }

  /**
   * Gets leftTransformName
   *
   * @return value of leftTransformName
   */
  public String getLeftTransformName() {
    return leftTransformName;
  }

  /** @param leftTransformName The leftTransformName to set */
  public void setLeftTransformName(String leftTransformName) {
    this.leftTransformName = leftTransformName;
  }

  /**
   * Gets rightTransformName
   *
   * @return value of rightTransformName
   */
  public String getRightTransformName() {
    return rightTransformName;
  }

  /** @param rightTransformName The rightTransformName to set */
  public void setRightTransformName(String rightTransformName) {
    this.rightTransformName = rightTransformName;
  }

  /**
   * Gets keyFields1
   *
   * @return value of keyFields1
   */
  public List<String> getKeyFields1() {
    return keyFields1;
  }

  /** @param keyFields1 The keyFields1 to set */
  public void setKeyFields1(List<String> keyFields1) {
    this.keyFields1 = keyFields1;
  }

  /**
   * Gets keyFields2
   *
   * @return value of keyFields2
   */
  public List<String> getKeyFields2() {
    return keyFields2;
  }

  /** @param keyFields2 The keyFields2 to set */
  public void setKeyFields2(List<String> keyFields2) {
    this.keyFields2 = keyFields2;
  }
}
