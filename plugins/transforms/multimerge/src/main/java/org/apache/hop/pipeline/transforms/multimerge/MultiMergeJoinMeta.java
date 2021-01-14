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

package org.apache.hop.pipeline.transforms.multimerge;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformIOMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.errorhandling.IStream.StreamType;
import org.apache.hop.pipeline.transform.errorhandling.Stream;
import org.apache.hop.pipeline.transform.errorhandling.StreamIcon;
import org.w3c.dom.Node;

import java.util.List;

@InjectionSupported(localizationPrefix = "MultiMergeJoin.Injection.")
@Transform(
    id = "MultiwayMergeJoin",
    image = "multimergejoin.svg",
    name = "i18n::MultiwayMergeJoin.Name",
    description = "i18n::MultiwayMergeJoin.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Joins",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/multimerge.html")
public class MultiMergeJoinMeta extends BaseTransformMeta
    implements ITransformMeta<MultiMergeJoin, MultiMergeJoinData> {
  private static final Class<?> PKG = MultiMergeJoinMeta.class; // For Translator

  public static final String[] joinTypes = {"INNER", "FULL OUTER"};
  public static final boolean[] optionals = {false, true};

  @Override
  public ITransform createTransform(
      TransformMeta transformMeta,
      MultiMergeJoinData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new MultiMergeJoin(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  @Injection(name = "JOIN_TYPE")
  private String joinType;

  /** comma separated key values for each stream */
  @Injection(name = "KEY_FIELDS")
  private String[] keyFields;

  /** input stream names */
  @Injection(name = "INPUT_TRANSFORMS")
  private String[] inputTransforms;

  /**
   * The supported join types are INNER, LEFT OUTER, RIGHT OUTER and FULL OUTER
   *
   * @return The type of join
   */
  public String getJoinType() {
    return joinType;
  }

  /**
   * Sets the type of join
   *
   * @param joinType The type of join, e.g. INNER/FULL OUTER
   */
  public void setJoinType(String joinType) {
    this.joinType = joinType;
  }

  /** @return Returns the keyFields1. */
  public String[] getKeyFields() {
    return keyFields;
  }

  /** @param keyFields The keyFields1 to set. */
  public void setKeyFields(String[] keyFields) {
    this.keyFields = keyFields;
  }

  @Override
  public boolean excludeFromRowLayoutVerification() {
    return true;
  }

  public MultiMergeJoinMeta() {
    super(); // allocate BaseTransformMeta
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode);
  }

  public void allocateKeys(int nrKeys) {
    keyFields = new String[nrKeys];
  }

  @Override
  public Object clone() {
    MultiMergeJoinMeta retval = (MultiMergeJoinMeta) super.clone();
    int nrKeys = keyFields == null ? 0 : keyFields.length;
    int nrTransforms = inputTransforms == null ? 0 : inputTransforms.length;
    retval.allocateKeys(nrKeys);
    retval.allocateInputTransforms(nrTransforms);
    System.arraycopy(keyFields, 0, retval.keyFields, 0, nrKeys);
    System.arraycopy(inputTransforms, 0, retval.inputTransforms, 0, nrTransforms);
    return retval;
  }

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder();

    String[] inputTransformsNames =
        inputTransforms != null ? inputTransforms : ArrayUtils.EMPTY_STRING_ARRAY;
    retval.append("    ").append(XmlHandler.addTagValue("join_type", getJoinType()));
    for (int i = 0; i < inputTransformsNames.length; i++) {
      retval
          .append("    ")
          .append(XmlHandler.addTagValue("transform" + i, inputTransformsNames[i]));
    }

    retval
        .append("    ")
        .append(XmlHandler.addTagValue("number_input", inputTransformsNames.length));
    retval.append("    ").append(XmlHandler.openTag("keys")).append(Const.CR);
    for (int i = 0; i < keyFields.length; i++) {
      retval.append("      ").append(XmlHandler.addTagValue("key", keyFields[i]));
    }
    retval.append("    ").append(XmlHandler.closeTag("keys")).append(Const.CR);

    return retval.toString();
  }

  private void readData(Node transformNode) throws HopXmlException {
    try {

      Node keysNode = XmlHandler.getSubNode(transformNode, "keys");

      int nrKeys = XmlHandler.countNodes(keysNode, "key");

      allocateKeys(nrKeys);

      for (int i = 0; i < nrKeys; i++) {
        Node keynode = XmlHandler.getSubNodeByNr(keysNode, "key", i);
        keyFields[i] = XmlHandler.getNodeValue(keynode);
      }

      int nInputStreams = Integer.parseInt(XmlHandler.getTagValue(transformNode, "number_input"));

      allocateInputTransforms(nInputStreams);

      for (int i = 0; i < nInputStreams; i++) {
        inputTransforms[i] = XmlHandler.getTagValue(transformNode, "transform" + i);
      }

      joinType = XmlHandler.getTagValue(transformNode, "join_type");
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "MultiMergeJoinMeta.Exception.UnableToLoadTransformMeta"), e);
    }
  }

  @Override
  public void setDefault() {
    joinType = joinTypes[0];
    allocateKeys(0);
    allocateInputTransforms(0);
  }

  @Override
  public void searchInfoAndTargetTransforms(List<TransformMeta> transforms) {
    ITransformIOMeta ioMeta = getTransformIOMeta();
    ioMeta.getInfoStreams().clear();
    for (int i = 0; i < inputTransforms.length; i++) {
      String inputTransformName = inputTransforms[i];
      if (i >= ioMeta.getInfoStreams().size()) {
        ioMeta.addStream(
            new Stream(
                StreamType.INFO,
                TransformMeta.findTransform(transforms, inputTransformName),
                BaseMessages.getString(PKG, "MultiMergeJoin.InfoStream.Description"),
                StreamIcon.INFO,
                inputTransformName));
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
    /*
     * @todo Need to check for the following: 1) Join type must be one of INNER / LEFT OUTER / RIGHT OUTER / FULL OUTER
     * 2) Number of input streams must be two (for now at least) 3) The field names of input streams must be unique
     */
    CheckResult cr =
        new CheckResult(
            ICheckResult.TYPE_RESULT_WARNING,
            BaseMessages.getString(PKG, "MultiMergeJoinMeta.CheckResult.TransformNotVerified"),
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
          r.mergeRowMeta(info[i]);
        }
      }
    }

    for (int i = 0; i < r.size(); i++) {
      r.getValueMeta(i).setOrigin(name);
    }
    return;
  }

  @Override
  public MultiMergeJoinData getTransformData() {
    return new MultiMergeJoinData();
  }

  @Override
  public void resetTransformIoMeta() {
    // Don't reset!
  }

  public void setInputTransforms(String[] inputTransforms) {
    this.inputTransforms = inputTransforms;
  }

  public String[] getInputTransforms() {
    return inputTransforms;
  }

  public void allocateInputTransforms(int count) {
    inputTransforms = new String[count];
  }
}
