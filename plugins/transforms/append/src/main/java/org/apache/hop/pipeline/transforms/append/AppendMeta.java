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

package org.apache.hop.pipeline.transforms.append;

import org.apache.hop.core.CheckResult;
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
import org.apache.hop.pipeline.PipelineMeta.PipelineType;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
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

/**
 * @author Sven Boden
 * @since 3-june-2007
 */
@Transform(
    id = "Append",
    image = "append.svg",
    name = "i18n::Append.Name",
    description = "i18n::Append.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Flow",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/append.html")
@InjectionSupported(localizationPrefix = "AppendMeta.Injection.")
public class AppendMeta extends BaseTransformMeta implements ITransformMeta<Append, AppendData> {

  private static final Class<?> PKG = Append.class; // For Translator

  @Injection(name = "HEAD_TRANSFORM")
  public String headTransformName;

  @Injection(name = "TAIL_TRANSFORM")
  public String tailTransformName;

  public AppendMeta() {
    super(); // allocate BaseTransformMeta
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode);
  }

  public Object clone() {
    AppendMeta retval = (AppendMeta) super.clone();

    return retval;
  }

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder();

    List<IStream> infoStreams = getTransformIOMeta().getInfoStreams();
    retval.append(XmlHandler.addTagValue("head_name", infoStreams.get(0).getTransformName()));
    retval.append(XmlHandler.addTagValue("tail_name", infoStreams.get(1).getTransformName()));

    return retval.toString();
  }

  private void readData(Node transformNode) throws HopXmlException {
    try {
      List<IStream> infoStreams = getTransformIOMeta().getInfoStreams();
      IStream headStream = infoStreams.get(0);
      IStream tailStream = infoStreams.get(1);
      headStream.setSubject(XmlHandler.getTagValue(transformNode, "head_name"));
      tailStream.setSubject(XmlHandler.getTagValue(transformNode, "tail_name"));
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "AppendMeta.Exception.UnableToLoadTransformMeta"), e);
    }
  }

  public void setDefault() {}

  @Override
  public void searchInfoAndTargetTransforms(List<TransformMeta> transforms) {
    ITransformIOMeta ioMeta = getTransformIOMeta();
    List<IStream> infoStreams = ioMeta.getInfoStreams();
    for (IStream stream : infoStreams) {
      stream.setTransformMeta(
          TransformMeta.findTransform(transforms, (String) stream.getSubject()));
    }
  }

  public boolean chosesTargetTransforms() {
    return false;
  }

  public String[] getTargetTransforms() {
    return null;
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

  @Override
  public Append createTransform(
      TransformMeta transformMeta, AppendData data, int cnr, PipelineMeta tr, Pipeline pipeline) {
    return new Append(transformMeta, this, data, cnr, tr, pipeline);
  }

  @Override
  public AppendData getTransformData() {
    return new AppendData();
  }

  /** Returns the Input/Output metadata for this transform. */
  @Override
  public ITransformIOMeta getTransformIOMeta() {
    ITransformIOMeta ioMeta = super.getTransformIOMeta(false);
    if (ioMeta == null) {

      ioMeta = new TransformIOMeta(true, true, false, false, false, false);

      ioMeta.addStream(
          new Stream(
              StreamType.INFO,
              null,
              BaseMessages.getString(PKG, "AppendMeta.InfoStream.FirstStream.Description"),
              StreamIcon.INFO,
              null));
      ioMeta.addStream(
          new Stream(
              StreamType.INFO,
              null,
              BaseMessages.getString(PKG, "AppendMeta.InfoStream.SecondStream.Description"),
              StreamIcon.INFO,
              null));
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
}
