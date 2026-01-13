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

import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformIOMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.stream.IStream.StreamType;
import org.apache.hop.pipeline.transform.stream.Stream;
import org.apache.hop.pipeline.transform.stream.StreamIcon;
import org.w3c.dom.Node;

@Transform(
    id = "MultiwayMergeJoin",
    image = "multimergejoin.svg",
    name = "i18n::MultiwayMergeJoin.Name",
    description = "i18n::MultiwayMergeJoin.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Joins",
    keywords = "i18n::MultiMergeJoinMeta.keyword",
    documentationUrl = "/pipeline/transforms/multimerge.html")
@Getter
@Setter
public class MultiMergeJoinMeta extends BaseTransformMeta<MultiMergeJoin, MultiMergeJoinData> {
  private static final Class<?> PKG = MultiMergeJoinMeta.class;

  public static final String[] joinTypes = {"INNER", "FULL OUTER"};
  public static final boolean[] optionals = {false, true};

  @HopMetadataProperty(key = "join_type", injectionKey = "JOIN_TYPE")
  private String joinType;

  /** comma separated key values for each stream */
  @HopMetadataProperty(key = "key", groupKey = "keys", injectionKey = "JOIN_TYPE")
  private List<String> keyFields;

  /** input stream names */
  @HopMetadataProperty(
      key = "transform",
      groupKey = "transforms",
      injectionKey = "INPUT_TRANSFORMS")
  private List<String> inputTransforms;

  @Override
  public boolean excludeFromRowLayoutVerification() {
    return true;
  }

  public MultiMergeJoinMeta() {
    super(); // allocate BaseTransformMeta
  }

  /**
   * keep loadXml to load old style xml information for the transform
   *
   * @deprecated
   * @param transformNode the XML node from the pipeline
   * @param metadataProvider metadata provider to resolve things
   * @throws HopXmlException when we can't read the XML correctly
   */
  @Override
  @Deprecated(since = "2.17")
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    super.loadXml(transformNode, metadataProvider);
    // keep for backwards compatibility
    readData(transformNode);
  }

  private void readData(Node transformNode) throws HopXmlException {
    try {
      String numInputStreamsStr = XmlHandler.getTagValue(transformNode, "number_input");

      // Skip if number_input doesn't exist (null or empty)
      if (numInputStreamsStr == null || numInputStreamsStr.trim().isEmpty()) {
        return;
      }

      int nInputStreams = Integer.parseInt(numInputStreamsStr);
      for (int i = 0; i < nInputStreams; i++) {
        inputTransforms.add(XmlHandler.getTagValue(transformNode, "transform" + i));
      }
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "MultiMergeJoinMeta.Exception.UnableToLoadTransformMeta"), e);
    }
  }

  @Override
  public void setDefault() {
    joinType = joinTypes[0];
  }

  @Override
  public void searchInfoAndTargetTransforms(List<TransformMeta> transforms) {
    ITransformIOMeta ioMeta = getTransformIOMeta();
    ioMeta.getInfoStreams().clear();
    for (int i = 0; i < inputTransforms.size(); i++) {
      String inputTransformName = inputTransforms.get(i);
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
      for (IRowMeta iRowMeta : info) {
        if (iRowMeta != null) {
          r.mergeRowMeta(iRowMeta);
        }
      }
    }

    for (int i = 0; i < r.size(); i++) {
      r.getValueMeta(i).setOrigin(name);
    }
  }

  @Override
  public void resetTransformIoMeta() {
    // Don't reset!
  }
}
