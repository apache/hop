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

package org.apache.hop.pipeline.transforms.rowgenerator;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.*;

import java.util.ArrayList;
import java.util.List;

@Transform(
    id = "RowGenerator",
    image = "rowgenerator.svg",
    name = "i18n::BaseTransform.TypeLongDesc.GenerateRows",
    description = "i18n::BaseTransform.TypeTooltipDesc.GenerateRows",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
        keywords = "i18n::RowGeneratorMeta.keyword",
    documentationUrl = "/pipeline/transforms/rowgenerator.html")
public class RowGeneratorMeta extends BaseTransformMeta
    implements ITransformMeta<RowGenerator, RowGeneratorData> {
  private static final Class<?> PKG = RowGeneratorMeta.class; // For Translator

  @HopMetadataProperty(
      key = "never_ending",
      injectionKeyDescription = "RowGeneratorMeta.Injection.NeverEnding")
  private boolean neverEnding;

  @HopMetadataProperty(
      key = "interval_in_ms",
      injectionKeyDescription = "RowGeneratorMeta.Injection.IntervalInMs")
  private String intervalInMs;

  @HopMetadataProperty(
      key = "row_time_field",
      injectionKeyDescription = "RowGeneratorMeta.Injection.RowTimeField")
  private String rowTimeField;

  @HopMetadataProperty(
      key = "last_time_field",
      injectionKeyDescription = "RowGeneratorMeta.Injection.LastTimeField")
  private String lastTimeField;

  @HopMetadataProperty(
      key = "limit",
      injectionKeyDescription = "RowGeneratorMeta.Injection.RowLimit")
  private String rowLimit;

  @HopMetadataProperty(
      groupKey = "fields",
      key = "field",
      injectionGroupDescription = "RowGeneratorMeta.Injection.Fields",
      injectionKeyDescription = "RowGeneratorMeta.Injection.Field")
  private List<GeneratorField> fields;

  public RowGeneratorMeta() {
    fields = new ArrayList<>();

    rowLimit = "10";
    neverEnding = false;
    intervalInMs = "5000";
    rowTimeField = "now";
    lastTimeField = "FiveSecondsAgo";
  }

  public RowGeneratorMeta(RowGeneratorMeta m) {
    this.neverEnding = m.neverEnding;
    this.intervalInMs = m.intervalInMs;
    this.rowTimeField = m.rowTimeField;
    this.lastTimeField = m.lastTimeField;
    this.rowLimit = m.rowLimit;
    this.fields = new ArrayList<>();
    for (GeneratorField field : m.fields) {
      this.fields.add(new GeneratorField(field));
    }
  }

  @Override
  public RowGeneratorMeta clone() {
    return new RowGeneratorMeta(this);
  }

  @Override
  public void getFields(
      IRowMeta row,
      String origin,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    try {
      List<ICheckResult> remarks = new ArrayList<>();
      RowMetaAndData rowMetaAndData = RowGenerator.buildRow(this, remarks, origin);
      if (!remarks.isEmpty()) {
        StringBuilder stringRemarks = new StringBuilder();
        for (ICheckResult remark : remarks) {
          stringRemarks.append(remark.toString()).append(Const.CR);
        }
        throw new HopTransformException(stringRemarks.toString());
      }

      for (IValueMeta valueMeta : rowMetaAndData.getRowMeta().getValueMetaList()) {
        valueMeta.setOrigin(origin);
      }

      row.mergeRowMeta(rowMetaAndData.getRowMeta());
    } catch (Exception e) {
      throw new HopTransformException(e);
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
    if (prev != null && prev.size() > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "RowGeneratorMeta.CheckResult.NoInputStreamsError"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "RowGeneratorMeta.CheckResult.NoInputStreamOk"),
              transformMeta);
      remarks.add(cr);

      String strLimit = variables.resolve(rowLimit);
      if (Const.toLong(strLimit, -1L) <= 0) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_WARNING,
                BaseMessages.getString(PKG, "RowGeneratorMeta.CheckResult.WarnNoRows"),
                transformMeta);
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(
                    PKG, "RowGeneratorMeta.CheckResult.WillReturnRows", strLimit),
                transformMeta);
        remarks.add(cr);
      }
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "RowGeneratorMeta.CheckResult.NoInputError"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "RowGeneratorMeta.CheckResult.NoInputOk"),
              transformMeta);
      remarks.add(cr);
    }
  }

  @Override
  public RowGenerator createTransform(
      TransformMeta transformMeta,
      RowGeneratorData data,
      int cnr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new RowGenerator(transformMeta, this, data, cnr, pipelineMeta, pipeline);
  }

  @Override
  public RowGeneratorData getTransformData() {
    return new RowGeneratorData();
  }

  /**
   * Returns the Input/Output metadata for this transform. The generator transform only produces
   * output, does not accept input!
   */
  @Override
  public ITransformIOMeta getTransformIOMeta() {
    return new TransformIOMeta(false, true, false, false, false, false);
  }

  /**
   * Gets neverEnding
   *
   * @return value of neverEnding
   */
  public boolean isNeverEnding() {
    return neverEnding;
  }

  /** @param neverEnding The neverEnding to set */
  public void setNeverEnding(boolean neverEnding) {
    this.neverEnding = neverEnding;
  }

  /**
   * Gets intervalInMs
   *
   * @return value of intervalInMs
   */
  public String getIntervalInMs() {
    return intervalInMs;
  }

  /** @param intervalInMs The intervalInMs to set */
  public void setIntervalInMs(String intervalInMs) {
    this.intervalInMs = intervalInMs;
  }

  /**
   * Gets rowTimeField
   *
   * @return value of rowTimeField
   */
  public String getRowTimeField() {
    return rowTimeField;
  }

  /** @param rowTimeField The rowTimeField to set */
  public void setRowTimeField(String rowTimeField) {
    this.rowTimeField = rowTimeField;
  }

  /**
   * Gets lastTimeField
   *
   * @return value of lastTimeField
   */
  public String getLastTimeField() {
    return lastTimeField;
  }

  /** @param lastTimeField The lastTimeField to set */
  public void setLastTimeField(String lastTimeField) {
    this.lastTimeField = lastTimeField;
  }

  /**
   * Gets rowLimit
   *
   * @return value of rowLimit
   */
  public String getRowLimit() {
    return rowLimit;
  }

  /** @param rowLimit The rowLimit to set */
  public void setRowLimit(String rowLimit) {
    this.rowLimit = rowLimit;
  }

  /**
   * Gets fields
   *
   * @return value of fields
   */
  public List<GeneratorField> getFields() {
    return fields;
  }

  /** @param fields The fields to set */
  public void setFields(List<GeneratorField> fields) {
    this.fields = fields;
  }
}
