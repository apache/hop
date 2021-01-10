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

package org.apache.hop.pipeline.transforms.mergejoin;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelineMeta.PipelineType;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformIOMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformIOMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.errorhandling.IStream;
import org.apache.hop.pipeline.transform.errorhandling.IStream.StreamType;
import org.apache.hop.pipeline.transform.errorhandling.Stream;
import org.apache.hop.pipeline.transform.errorhandling.StreamIcon;
import org.w3c.dom.Node;

import java.util.List;

@InjectionSupported(localizationPrefix = "MergeJoin.Injection.")
@Transform(
    id = "MergeJoin",
    image = "mergejoin.svg",
    name = "i18n::BaseTransform.TypeLongDesc.MergeJoin",
    description = "i18n::BaseTransform.TypeTooltipDesc.MergeJoin",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Joins",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/mergejoin.html")
public class MergeJoinMeta extends BaseTransformMeta
    implements ITransformMeta<MergeJoin, MergeJoinData> {
  private static final Class<?> PKG = MergeJoinMeta.class; // For Translator

  public static final String[] joinTypes = {"INNER", "LEFT OUTER", "RIGHT OUTER", "FULL OUTER"};
  public static final boolean[] one_optionals = {false, false, true, true};
  public static final boolean[] two_optionals = {false, true, false, true};

  @Injection(name = "JOIN_TYPE")
  private String joinType;

  @Injection(name = "KEY_FIELD1")
  private String[] keyFields1;

  @Injection(name = "KEY_FIELD2")
  private String[] keyFields2;

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
  public String[] getKeyFields1() {
    return keyFields1;
  }

  /** @param keyFields1 The keyFields1 to set. */
  public void setKeyFields1(String[] keyFields1) {
    this.keyFields1 = keyFields1;
  }

  /** @return Returns the keyFields2. */
  public String[] getKeyFields2() {
    return keyFields2;
  }

  /** @param keyFields2 The keyFields2 to set. */
  public void setKeyFields2(String[] keyFields2) {
    this.keyFields2 = keyFields2;
  }

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
    super(); // allocate BaseTransformMeta
  }

  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode);
  }

  public void allocate(int nrKeys1, int nrKeys2) {
    keyFields1 = new String[nrKeys1];
    keyFields2 = new String[nrKeys2];
  }

  public Object clone() {
    MergeJoinMeta retval = (MergeJoinMeta) super.clone();
    int nrKeys1 = keyFields1.length;
    int nrKeys2 = keyFields2.length;
    retval.allocate(nrKeys1, nrKeys2);
    System.arraycopy(keyFields1, 0, retval.keyFields1, 0, nrKeys1);
    System.arraycopy(keyFields2, 0, retval.keyFields2, 0, nrKeys2);

    ITransformIOMeta transformIOMeta = new TransformIOMeta(true, true, false, false, false, false);
    List<IStream> infoStreams = getTransformIOMeta().getInfoStreams();

    for (IStream infoStream : infoStreams) {
      transformIOMeta.addStream(new Stream(infoStream));
    }
    retval.setTransformIOMeta(transformIOMeta);

    return retval;
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder();

    List<IStream> infoStreams = getTransformIOMeta().getInfoStreams();

    retval.append(XmlHandler.addTagValue("join_type", getJoinType()));
    retval.append(XmlHandler.addTagValue("transform1", infoStreams.get(0).getTransformName()));
    retval.append(XmlHandler.addTagValue("transform2", infoStreams.get(1).getTransformName()));

    retval.append("    <keys_1>" + Const.CR);
    for (int i = 0; i < keyFields1.length; i++) {
      retval.append("      " + XmlHandler.addTagValue("key", keyFields1[i]));
    }
    retval.append("    </keys_1>" + Const.CR);

    retval.append("    <keys_2>" + Const.CR);
    for (int i = 0; i < keyFields2.length; i++) {
      retval.append("      " + XmlHandler.addTagValue("key", keyFields2[i]));
    }
    retval.append("    </keys_2>" + Const.CR);

    return retval.toString();
  }

  private void readData(Node transformNode) throws HopXmlException {
    try {

      Node keysNode1 = XmlHandler.getSubNode(transformNode, "keys_1");
      Node keysNode2 = XmlHandler.getSubNode(transformNode, "keys_2");

      int nrKeys1 = XmlHandler.countNodes(keysNode1, "key");
      int nrKeys2 = XmlHandler.countNodes(keysNode2, "key");

      allocate(nrKeys1, nrKeys2);

      for (int i = 0; i < nrKeys1; i++) {
        Node keynode = XmlHandler.getSubNodeByNr(keysNode1, "key", i);
        keyFields1[i] = XmlHandler.getNodeValue(keynode);
      }

      for (int i = 0; i < nrKeys2; i++) {
        Node keynode = XmlHandler.getSubNodeByNr(keysNode2, "key", i);
        keyFields2[i] = XmlHandler.getNodeValue(keynode);
      }

      List<IStream> infoStreams = getTransformIOMeta().getInfoStreams();
      infoStreams.get(0).setSubject(XmlHandler.getTagValue(transformNode, "transform1"));
      infoStreams.get(1).setSubject(XmlHandler.getTagValue(transformNode, "transform2"));
      joinType = XmlHandler.getTagValue(transformNode, "join_type");
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "MergeJoinMeta.Exception.UnableToLoadTransformMeta"), e);
    }
  }

  public void setDefault() {
    joinType = joinTypes[0];
    allocate(0, 0);
  }

  @Override
  public void searchInfoAndTargetTransforms(List<TransformMeta> transforms) {
    List<IStream> infoStreams = getTransformIOMeta().getInfoStreams();
    for (IStream stream : infoStreams) {
      stream.setTransformMeta(
          TransformMeta.findTransform(transforms, (String) stream.getSubject()));
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
    return;
  }

  public ITransform getTransform(
      TransformMeta transformMeta,
      MergeJoinData data,
      int cnr,
      PipelineMeta tr,
      Pipeline pipeline) {
    return new MergeJoin(transformMeta, this, data, cnr, tr, pipeline);
  }

  public MergeJoinData getTransformData() {
    return new MergeJoinData();
  }

  /**
   * Returns the Input/Output metadata for this transform. The generator transform only produces
   * output, does not accept input!
   */
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

  public void resetTransformIoMeta() {
    // Don't reset!
  }

  public PipelineType[] getSupportedPipelineTypes() {
    return new PipelineType[] {
      PipelineType.Normal,
    };
  }
}
