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

package org.apache.hop.pipeline.transforms.mergerows;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopRowException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
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

/*
 * Created on 02-jun-2003
 *
 */
@InjectionSupported(localizationPrefix = "MergeRows.Injection.")
@Transform(
    id = "MergeRows",
    image = "mergerows.svg",
    name = "i18n::BaseTransform.TypeLongDesc.MergeRows",
    description = "i18n::BaseTransform.TypeTooltipDesc.MergeRows",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Joins",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/mergerows.html")
public class MergeRowsMeta extends BaseTransformMeta
    implements ITransformMeta<MergeRows, MergeRowsData> {
  private static final Class<?> PKG = MergeRowsMeta.class; // For Translator

  @Injection(name = "FLAG_FIELD")
  private String flagField;

  @Injection(name = "KEY_FIELDS")
  private String[] keyFields;

  @Injection(name = "VALUE_FIELDS")
  private String[] valueFields;

  /** @return Returns the keyFields. */
  public String[] getKeyFields() {
    return keyFields;
  }

  /** @param keyFields The keyFields to set. */
  public void setKeyFields(String[] keyFields) {
    this.keyFields = keyFields;
  }

  /** @return Returns the valueFields. */
  public String[] getValueFields() {
    return valueFields;
  }

  /** @param valueFields The valueFields to set. */
  public void setValueFields(String[] valueFields) {
    this.valueFields = valueFields;
  }

  public MergeRowsMeta() {
    super(); // allocate BaseTransformMeta
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode);
  }

  /** @return Returns the flagField. */
  public String getFlagField() {
    return flagField;
  }

  /** @param flagField The flagField to set. */
  public void setFlagField(String flagField) {
    this.flagField = flagField;
  }

  public void allocate(int nrKeys, int nrValues) {
    keyFields = new String[nrKeys];
    valueFields = new String[nrValues];
  }

  @Override
  public Object clone() {
    MergeRowsMeta retval = (MergeRowsMeta) super.clone();
    int nrKeys = keyFields.length;
    int nrValues = valueFields.length;
    retval.allocate(nrKeys, nrValues);
    System.arraycopy(keyFields, 0, retval.keyFields, 0, nrKeys);
    System.arraycopy(valueFields, 0, retval.valueFields, 0, nrValues);
    return retval;
  }

  @Override
  public ITransform createTransform(
      TransformMeta transformMeta,
      MergeRowsData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new MergeRows(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder();

    retval.append("    <keys>" + Const.CR);
    for (int i = 0; i < keyFields.length; i++) {
      retval.append("      " + XmlHandler.addTagValue("key", keyFields[i]));
    }
    retval.append("    </keys>" + Const.CR);

    retval.append("    <values>" + Const.CR);
    for (int i = 0; i < valueFields.length; i++) {
      retval.append("      " + XmlHandler.addTagValue("value", valueFields[i]));
    }
    retval.append("    </values>" + Const.CR);

    retval.append(XmlHandler.addTagValue("flag_field", flagField));

    List<IStream> infoStreams = getTransformIOMeta().getInfoStreams();
    retval.append(XmlHandler.addTagValue("reference", infoStreams.get(0).getTransformName()));
    retval.append(XmlHandler.addTagValue("compare", infoStreams.get(1).getTransformName()));
    retval.append("    <compare>" + Const.CR);

    retval.append("    </compare>" + Const.CR);

    return retval.toString();
  }

  private void readData(Node transformNode) throws HopXmlException {
    try {

      Node keysnode = XmlHandler.getSubNode(transformNode, "keys");
      Node valuesnode = XmlHandler.getSubNode(transformNode, "values");

      int nrKeys = XmlHandler.countNodes(keysnode, "key");
      int nrValues = XmlHandler.countNodes(valuesnode, "value");

      allocate(nrKeys, nrValues);

      for (int i = 0; i < nrKeys; i++) {
        Node keynode = XmlHandler.getSubNodeByNr(keysnode, "key", i);
        keyFields[i] = XmlHandler.getNodeValue(keynode);
      }

      for (int i = 0; i < nrValues; i++) {
        Node valuenode = XmlHandler.getSubNodeByNr(valuesnode, "value", i);
        valueFields[i] = XmlHandler.getNodeValue(valuenode);
      }

      flagField = XmlHandler.getTagValue(transformNode, "flag_field");

      List<IStream> infoStreams = getTransformIOMeta().getInfoStreams();
      IStream referenceStream = infoStreams.get(0);
      IStream compareStream = infoStreams.get(1);

      compareStream.setSubject(XmlHandler.getTagValue(transformNode, "compare"));
      referenceStream.setSubject(XmlHandler.getTagValue(transformNode, "reference"));
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "MergeRowsMeta.Exception.UnableToLoadTransformMeta"), e);
    }
  }

  @Override
  public void setDefault() {
    flagField = "flagfield";
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

  public boolean chosesTargetTransforms() {
    return false;
  }

  public String[] getTargetTransforms() {
    return null;
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
      boolean found = false;
      for (int i = 0; i < info.length && !found; i++) {
        if (info[i] != null) {
          r.mergeRowMeta(info[i], name);
          found = true;
        }
      }
    }

    if (Utils.isEmpty(flagField)) {
      throw new HopTransformException(
          BaseMessages.getString(PKG, "MergeRowsMeta.Exception.FlagFieldNotSpecified"));
    }
    IValueMeta flagFieldValue = new ValueMetaString(flagField);
    flagFieldValue.setOrigin(name);
    r.addValueMeta(flagFieldValue);
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
    IStream referenceStream = infoStreams.get(0);
    IStream compareStream = infoStreams.get(1);

    if (referenceStream.getTransformName() != null && compareStream.getTransformName() != null) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "MergeRowsMeta.CheckResult.SourceTransformsOK"),
              transformMeta);
      remarks.add(cr);
    } else if (referenceStream.getTransformName() == null
        && compareStream.getTransformName() == null) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "MergeRowsMeta.CheckResult.SourceTransformsMissing"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "MergeRowsMeta.CheckResult.OneSourceTransformMissing"),
              transformMeta);
      remarks.add(cr);
    }

    IRowMeta referenceRowMeta = null;
    IRowMeta compareRowMeta = null;
    try {
      referenceRowMeta =
          pipelineMeta.getPrevTransformFields(variables, referenceStream.getTransformName());
      compareRowMeta =
          pipelineMeta.getPrevTransformFields(variables, compareStream.getTransformName());
    } catch (HopTransformException kse) {
      new CheckResult(
          ICheckResult.TYPE_RESULT_ERROR,
          BaseMessages.getString(PKG, "MergeRowsMeta.CheckResult.ErrorGettingPrevTransformFields"),
          transformMeta);
    }
    if (referenceRowMeta != null && compareRowMeta != null) {
      boolean rowsMatch = false;
      try {
        MergeRows.checkInputLayoutValid(referenceRowMeta, compareRowMeta);
        rowsMatch = true;
      } catch (HopRowException kre) {
        rowsMatch = false;
      }
      if (rowsMatch) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "MergeRowsMeta.CheckResult.RowDefinitionMatch"),
                transformMeta);
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "MergeRowsMeta.CheckResult.RowDefinitionNotMatch"),
                transformMeta);
        remarks.add(cr);
      }
    }
  }

  @Override
  public MergeRowsData getTransformData() {
    return new MergeRowsData();
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
              BaseMessages.getString(PKG, "MergeRowsMeta.InfoStream.FirstStream.Description"),
              StreamIcon.INFO,
              null));
      ioMeta.addStream(
          new Stream(
              StreamType.INFO,
              null,
              BaseMessages.getString(PKG, "MergeRowsMeta.InfoStream.SecondStream.Description"),
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
