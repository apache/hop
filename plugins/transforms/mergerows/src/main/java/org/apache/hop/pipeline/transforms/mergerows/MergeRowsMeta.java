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

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopRowException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
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

@Getter
@Setter
@Transform(
    id = "MergeRows",
    image = "mergerows.svg",
    name = "i18n::MergeRows.Name",
    description = "i18n::MergeRows.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Joins",
    keywords = "i18n::MergeRowsMeta.keyword",
    documentationUrl = "/pipeline/transforms/mergerows.html")
public class MergeRowsMeta extends BaseTransformMeta<MergeRows, MergeRowsData> {
  private static final Class<?> PKG = MergeRowsMeta.class;

  @HopMetadataProperty(
      key = "flag_field",
      injectionKey = "FLAG_FIELD",
      injectionKeyDescription = "MergeRows.Injection.FLAG_FIELD")
  private String flagField;

  @HopMetadataProperty(
      key = "key",
      groupKey = "keys",
      injectionKey = "KEY_FIELD",
      injectionKeyDescription = "MergeRows.Injection.KEY_FIELD",
      injectionGroupKey = "KEY_FIELDS",
      injectionGroupDescription = "MergeRows.Injection.KEY_FIELDS")
  private List<String> keyFields;

  @HopMetadataProperty(
      key = "value",
      groupKey = "values",
      injectionKey = "VALUE_FIELD",
      injectionKeyDescription = "MergeRows.Injection.VALUE_FIELD",
      injectionGroupKey = "VALUE_FIELDS",
      injectionGroupDescription = "MergeRows.Injection.VALUE_FIELDS")
  private List<String> valueFields;

  @HopMetadataProperty(
      key = "reference",
      injectionKey = "REFERENCE_TRANSFORM",
      injectionKeyDescription = "MergeRowsMeta.InfoStream.FirstStream.Description")
  private String referenceTransform;

  @HopMetadataProperty(
      key = "compare",
      injectionKey = "COMPARE_TRANSFORM",
      injectionKeyDescription = "MergeRowsMeta.InfoStream.SecondStream.Description")
  private String compareTransform;

  @HopMetadataProperty(
      key = "diff-field",
      injectionKey = "DIFF_FIELD",
      injectionKeyDescription = "MergeRows.Injection.DIFF_FIELD")
  private String diffJsonField;

  public MergeRowsMeta() {
    super();
    keyFields = new ArrayList<>();
    valueFields = new ArrayList<>();
  }

  public MergeRowsMeta(MergeRowsMeta m) {
    this.flagField = m.flagField;
    this.keyFields = new ArrayList<>(m.keyFields);
    this.valueFields = new ArrayList<>(m.valueFields);
    this.referenceTransform = m.referenceTransform;
    this.compareTransform = m.compareTransform;
  }

  @Override
  public MergeRowsMeta clone() {
    return new MergeRowsMeta(this);
  }

  @Override
  public void setDefault() {
    flagField = "flagfield";
  }

  @Override
  public void searchInfoAndTargetTransforms(List<TransformMeta> transforms) {
    List<IStream> infoStreams = getTransformIOMeta().getInfoStreams();
    for (IStream stream : infoStreams) {
      stream.setTransformMeta(TransformMeta.findTransform(transforms, stream.getSubject()));
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
    if (StringUtils.isNotEmpty(variables.resolve(diffJsonField))) {
      IValueMeta diffField = new ValueMetaString(variables.resolve(diffJsonField));
      diffField.setOrigin(name);
      r.addValueMeta(diffField);
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
        // Ignore
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
              referenceTransform));
      ioMeta.addStream(
          new Stream(
              StreamType.INFO,
              null,
              BaseMessages.getString(PKG, "MergeRowsMeta.InfoStream.SecondStream.Description"),
              StreamIcon.INFO,
              compareTransform));
      setTransformIOMeta(ioMeta);
    }

    return ioMeta;
  }

  @Override
  public PipelineType[] getSupportedPipelineTypes() {
    return new PipelineType[] {
      PipelineType.Normal,
    };
  }
}
