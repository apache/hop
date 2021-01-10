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

package org.apache.hop.pipeline.transforms.samplerows;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

import java.util.List;

/*
 * Created on 02-jun-2008
 *
 */

@Transform(
    id = "SampleRows",
    image = "samplerows.svg",
    name = "i18n::BaseTransform.TypeLongDesc.SampleRows",
    description = "i18n::BaseTransform.TypeTooltipDesc.SampleRows",
    categoryDescription =
        "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Statistics",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/samplerows.html")
public class SampleRowsMeta extends BaseTransformMeta
    implements ITransformMeta<SampleRows, SampleRowsData> {
  private static final Class<?> PKG = SampleRowsMeta.class; // For Translator

  private String linesrange;
  private String linenumfield;
  public static String DEFAULT_RANGE = "1";

  public SampleRowsMeta() {
    super(); // allocate BaseTransformMeta
  }

  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode, metadataProvider);
  }

  public Object clone() {
    Object retval = super.clone();
    return retval;
  }

  public void getFields(
      IRowMeta inputRowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    if (!Utils.isEmpty(linenumfield)) {

      IValueMeta v = new ValueMetaInteger(variables.resolve(linenumfield));
      v.setLength(IValueMeta.DEFAULT_INTEGER_LENGTH, 0);
      v.setOrigin(name);
      inputRowMeta.addValueMeta(v);
    }
  }

  private void readData(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    try {
      linesrange = XmlHandler.getTagValue(transformNode, "linesrange");
      linenumfield = XmlHandler.getTagValue(transformNode, "linenumfield");
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "SampleRowsMeta.Exception.UnableToReadTransformMeta"), e);
    }
  }

  public String getLinesRange() {
    return this.linesrange;
  }

  public void setLinesRange(String linesrange) {
    this.linesrange = linesrange;
  }

  public String getLineNumberField() {
    return this.linenumfield;
  }

  public void setLineNumberField(String linenumfield) {
    this.linenumfield = linenumfield;
  }

  public void setDefault() {
    linesrange = DEFAULT_RANGE;
    linenumfield = null;
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder();
    retval.append("    " + XmlHandler.addTagValue("linesrange", linesrange));
    retval.append("    " + XmlHandler.addTagValue("linenumfield", linenumfield));

    return retval.toString();
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

    if (Utils.isEmpty(linesrange)) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "SampleRowsMeta.CheckResult.LinesRangeMissing"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "SampleRowsMeta.CheckResult.LinesRangeOk"),
              transformMeta);
    }
    remarks.add(cr);

    if (prev == null || prev.size() == 0) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_WARNING,
              BaseMessages.getString(PKG, "SampleRowsMeta.CheckResult.NotReceivingFields"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "SampleRowsMeta.CheckResult.TransformRecevingData", prev.size() + ""),
              transformMeta);
    }
    remarks.add(cr);

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "SampleRowsMeta.CheckResult.TransformRecevingData2"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "SampleRowsMeta.CheckResult.NoInputReceivedFromOtherTransforms"),
              transformMeta);
    }
    remarks.add(cr);
  }

  @Override
  public ITransform createTransform(
      TransformMeta transformMeta,
      SampleRowsData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new SampleRows(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  public SampleRowsData getTransformData() {
    return new SampleRowsData();
  }
}
