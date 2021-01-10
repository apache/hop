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

package org.apache.hop.pipeline.transforms.getserverequence;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
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

@Transform(
    id = "GetServerSequence",
    image = "get-server-sequence.svg",
    name = "i18n::GetServerSequence.Name",
    description = "i18n::GetServerSequence.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    documentationUrl =
        "https://hop.apache.org/manual/latest/plugins/transforms/getserversequence.html")
public class GetServerSequenceMeta extends BaseTransformMeta
    implements ITransformMeta<GetServerSequence, GetServerSequenceData> {
  private static final Class<?> PKG = GetServerSequenceMeta.class; // For Translator

  private String valuename;
  private String hopServerName;
  private String sequenceName;
  private String increment;

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode, metadataProvider);
  }

  @Override
  public Object clone() {
    Object retval = super.clone();
    return retval;
  }

  private void readData(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    try {
      valuename = XmlHandler.getTagValue(transformNode, "valuename");
      hopServerName = XmlHandler.getTagValue(transformNode, "server");
      sequenceName = XmlHandler.getTagValue(transformNode, "seqname");
      increment = XmlHandler.getTagValue(transformNode, "increment");
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "GetSequenceMeta.Exception.ErrorLoadingTransformMeta"), e);
    }
  }

  @Override
  public void setDefault() {
    valuename = "id";
    hopServerName = "";
    sequenceName = "Server Sequence Name -- To be configured";
    increment = "10000";
  }

  @Override
  public void getFields(
      IRowMeta row,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    IValueMeta v = new ValueMetaInteger(valuename);
    v.setOrigin(name);
    row.addValueMeta(v);
  }

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder(300);

    retval.append("      ").append(XmlHandler.addTagValue("valuename", valuename));
    retval.append("      ").append(XmlHandler.addTagValue("server", hopServerName));
    retval.append("      ").append(XmlHandler.addTagValue("seqname", sequenceName));
    retval.append("      ").append(XmlHandler.addTagValue("increment", increment));

    return retval.toString();
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

    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "GetSequenceMeta.CheckResult.TransformIsReceving.Title"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "GetSequenceMeta.CheckResult.NoInputReceived.Title"),
              transformMeta);
      remarks.add(cr);
    }
  }

  @Override
  public GetServerSequence createTransform(
      TransformMeta transformMeta,
      GetServerSequenceData data,
      int cnr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new GetServerSequence(transformMeta, this, data, cnr, pipelineMeta, pipeline);
  }

  @Override
  public GetServerSequenceData getTransformData() {
    return new GetServerSequenceData();
  }

  /** @return the valuename */
  public String getValuename() {
    return valuename;
  }

  /** @param valuename the valuename to set */
  public void setValuename(String valuename) {
    this.valuename = valuename;
  }

  /** @return the hopServerName */
  public String getHopServerName() {
    return hopServerName;
  }

  /** @param hopServerName the hopServerName to set */
  public void setHopServerName(String hopServerName) {
    this.hopServerName = hopServerName;
  }

  /** @return the sequenceName */
  public String getSequenceName() {
    return sequenceName;
  }

  /** @param sequenceName the sequenceName to set */
  public void setSequenceName(String sequenceName) {
    this.sequenceName = sequenceName;
  }

  /** @return the increment */
  public String getIncrement() {
    return increment;
  }

  /** @param increment the increment to set */
  public void setIncrement(String increment) {
    this.increment = increment;
  }
}
