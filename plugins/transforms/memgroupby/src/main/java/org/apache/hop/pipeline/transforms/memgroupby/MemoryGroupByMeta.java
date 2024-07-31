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

package org.apache.hop.pipeline.transforms.memgroupby;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hop.beam.core.BeamHop;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.engines.IBeamPipelineEngineRunConfiguration;
import org.apache.hop.beam.pipeline.IBeamPipelineTransformHandler;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.JsonRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaNone;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IEnumHasCode;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.memgroupby.beam.GroupByTransform;

@Transform(
    id = "MemoryGroupBy",
    image = "memorygroupby.svg",
    name = "i18n::MemoryGroupBy.Name",
    description = "i18n::MemoryGroupBy.Description",
    categoryDescription =
        "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Statistics",
    keywords = "i18n::MemoryGroupByMeta.keyword",
    documentationUrl = "/pipeline/transforms/memgroupby.html")
public class MemoryGroupByMeta extends BaseTransformMeta<MemoryGroupBy, MemoryGroupByData>
    implements IBeamPipelineTransformHandler {
  private static final Class<?> PKG = MemoryGroupByMeta.class;

  /** Fields to group over */
  @HopMetadataProperty(groupKey = "group", key = "field")
  private List<GGroup> groups;

  @HopMetadataProperty(
      groupKey = "fields",
      key = "field",
      injectionGroupKey = "AGGREGATES",
      injectionGroupDescription = "MemoryGroupBy.Injection.AGGREGATES",
      injectionKey = "AGGREGATEFIELD",
      injectionKeyDescription = "MemoryGroupBy.Injection.AGGREGATEFIELD")
  private List<GAggregate> aggregates;

  /** Flag to indicate that we always give back one row. Defaults to true for existing pipelines. */
  @HopMetadataProperty(
      key = "give_back_row",
      injectionKey = "ALWAYSGIVINGBACKONEROW",
      injectionKeyDescription = "MemoryGroupBy.Injection.ALWAYSGIVINGBACKONEROW")
  private boolean alwaysGivingBackOneRow;

  public MemoryGroupByMeta() {
    this.groups = new ArrayList<>();
    this.aggregates = new ArrayList<>();
  }

  public MemoryGroupByMeta(MemoryGroupByMeta meta) {
    this();
    for (GGroup group : meta.groups) {
      groups.add(new GGroup(group));
    }
    for (GAggregate aggregate : meta.aggregates) {
      aggregates.add(new GAggregate(aggregate));
    }
    this.alwaysGivingBackOneRow = meta.alwaysGivingBackOneRow;
  }

  @Override
  public MemoryGroupByMeta clone() {
    return new MemoryGroupByMeta(this);
  }

  @Override
  public void getFields(
      IRowMeta r,
      String origin,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {

    // re-assemble a new row of metadata
    //
    IRowMeta fields = new RowMeta();

    // Add the grouping fields in the correct order...
    //
    for (GGroup group : groups) {
      IValueMeta valueMeta = r.searchValueMeta(group.getField());
      if (valueMeta != null) {
        valueMeta.setStorageType(IValueMeta.STORAGE_TYPE_NORMAL);
        fields.addValueMeta(valueMeta);
      }
    }

    // Re-add aggregates
    //
    for (GAggregate aggregate : aggregates) {
      IValueMeta subj = r.searchValueMeta(aggregate.getSubject());
      if (subj != null || aggregate.getType() == GroupType.CountAny) {
        String valueName = aggregate.getField();
        int valueType = IValueMeta.TYPE_NONE;
        int length = -1;
        int precision = -1;
        String mask = null;

        switch (aggregate.getType()) {
          case First, Last, FirstIncludingNull, LastIncludingNull, Minimum, Maximum:
            valueType = subj.getType();
            mask = subj.getConversionMask();
            break;
          case CountDistinct, CountAll, CountAny:
            valueType = IValueMeta.TYPE_INTEGER;
            mask = "0";
            break;
          case Sum, Average:
            if (subj.isNumeric()) {
              valueType = subj.getType();
            } else {
              valueType = IValueMeta.TYPE_NUMBER;
            }
            mask = subj.getConversionMask();
            break;
          case Median, Percentile, StandardDeviation:
            valueType = IValueMeta.TYPE_NUMBER;
            mask = subj.getConversionMask();
            break;
          case ConcatComma, ConcatString, ConcatDistinct:
            valueType = IValueMeta.TYPE_STRING;
            break;
          default:
            break;
        }

        if (aggregate.getType() == GroupType.Sum
            && valueType != IValueMeta.TYPE_INTEGER
            && valueType != IValueMeta.TYPE_NUMBER
            && valueType != IValueMeta.TYPE_BIGNUMBER) {
          // If it ain't numeric, we change it to Number
          //
          valueType = IValueMeta.TYPE_NUMBER;
        }

        if (valueType != IValueMeta.TYPE_NONE) {
          IValueMeta valueMeta;
          try {
            valueMeta = ValueMetaFactory.createValueMeta(valueName, valueType);
          } catch (HopPluginException e) {
            log.logError(
                BaseMessages.getString(PKG, "MemoryGroupByMeta.Exception.UnknownValueMetaType"),
                valueType,
                e);
            valueMeta = new ValueMetaNone(valueName);
          }
          valueMeta.setOrigin(origin);
          valueMeta.setLength(length, precision);

          if (mask != null) {
            valueMeta.setConversionMask(mask);
          }

          fields.addValueMeta(valueMeta);
        }
      }
    }

    // Now that we have all the fields we want, we should clear the original row and replace the
    // values...
    //
    r.clear();
    r.addRowMeta(fields);
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
              BaseMessages.getString(PKG, "MemoryGroupByMeta.CheckResult.ReceivingInfoOK"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "MemoryGroupByMeta.CheckResult.NoInputError"),
              transformMeta);
      remarks.add(cr);
    }
  }

  @Override
  public boolean isInput() {
    return false;
  }

  @Override
  public boolean isOutput() {
    return false;
  }

  /**
   * Handle the transform in a Beam pipeline
   *
   * @param log
   * @param variables
   * @param runConfigurationName
   * @param runConfiguration
   * @param dataSamplersJson
   * @param metadataProvider
   * @param pipelineMeta
   * @param transformMeta
   * @param transformCollectionMap
   * @param pipeline
   * @param rowMeta
   * @param previousTransforms
   * @param input
   * @param parentLogChannelId
   * @throws HopException
   */
  @Override
  public void handleTransform(
      ILogChannel log,
      IVariables variables,
      String runConfigurationName,
      IBeamPipelineEngineRunConfiguration runConfiguration,
      String dataSamplersJson,
      IHopMetadataProvider metadataProvider,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      Map<String, PCollection<HopRow>> transformCollectionMap,
      Pipeline pipeline,
      IRowMeta rowMeta,
      List<TransformMeta> previousTransforms,
      PCollection<HopRow> input,
      String parentLogChannelId)
      throws HopException {

    MemoryGroupByMeta meta = new MemoryGroupByMeta();
    BeamHop.loadTransformMetadata(meta, transformMeta, metadataProvider, pipelineMeta);

    String[] subjectFields = new String[meta.getAggregates().size()];
    String[] aggregateCodes = new String[meta.getAggregates().size()];

    for (int i = 0; i < meta.getAggregates().size(); i++) {
      GAggregate aggregate = meta.getAggregates().get(i);

      aggregateCodes[i] = aggregate.getType().getCode();
      subjectFields[i] = aggregate.getSubject();
    }

    List<String> groups = new ArrayList<>();
    meta.getGroups().forEach(group -> groups.add(group.getField()));

    PTransform<PCollection<HopRow>, PCollection<HopRow>> groupByTransform =
        new GroupByTransform(
            transformMeta.getName(),
            JsonRowMeta.toJson(rowMeta), // The io row
            groups.toArray(new String[0]),
            subjectFields,
            aggregateCodes,
            new String[] {});

    // Apply the transform to the previous io transform PCollection(s)
    //
    PCollection<HopRow> transformPCollection =
        input.apply(transformMeta.getName(), groupByTransform);

    // Save this in the map
    //
    transformCollectionMap.put(transformMeta.getName(), transformPCollection);
    log.logBasic(
        "Handled Group By (TRANSFORM) : "
            + transformMeta.getName()
            + ", gets data from "
            + previousTransforms.size()
            + " previous transform(s)");
  }

  @SuppressWarnings("java:S115")
  public enum GroupType implements IEnumHasCode {
    None("-", "-"),
    Sum("SUM", BaseMessages.getString(PKG, "MemoryGroupByMeta.TypeGroupLongDesc.SUM")),
    Average("AVERAGE", BaseMessages.getString(PKG, "MemoryGroupByMeta.TypeGroupLongDesc.AVERAGE")),
    Median("MEDIAN", BaseMessages.getString(PKG, "MemoryGroupByMeta.TypeGroupLongDesc.MEDIAN")),
    Percentile(
        "PERCENTILE",
        BaseMessages.getString(PKG, "MemoryGroupByMeta.TypeGroupLongDesc.PERCENTILE")),
    Minimum("MIN", BaseMessages.getString(PKG, "MemoryGroupByMeta.TypeGroupLongDesc.MIN")),
    Maximum("MAX", BaseMessages.getString(PKG, "MemoryGroupByMeta.TypeGroupLongDesc.MAX")),
    CountAll(
        "COUNT_ALL", BaseMessages.getString(PKG, "MemoryGroupByMeta.TypeGroupLongDesc.CONCAT_ALL")),
    ConcatComma(
        "CONCAT_COMMA",
        BaseMessages.getString(PKG, "MemoryGroupByMeta.TypeGroupLongDesc.CONCAT_COMMA")),
    First("FIRST", BaseMessages.getString(PKG, "MemoryGroupByMeta.TypeGroupLongDesc.FIRST")),
    Last("LAST", BaseMessages.getString(PKG, "MemoryGroupByMeta.TypeGroupLongDesc.LAST")),
    FirstIncludingNull(
        "FIRST_INCL_NULL",
        BaseMessages.getString(PKG, "MemoryGroupByMeta.TypeGroupLongDesc.FIRST_INCL_NULL")),
    LastIncludingNull(
        "LAST_INCL_NULL",
        BaseMessages.getString(PKG, "MemoryGroupByMeta.TypeGroupLongDesc.LAST_INCL_NULL")),
    StandardDeviation(
        "STD_DEV",
        BaseMessages.getString(PKG, "MemoryGroupByMeta.TypeGroupLongDesc.STANDARD_DEVIATION")),
    ConcatString(
        "CONCAT_STRING",
        BaseMessages.getString(PKG, "MemoryGroupByMeta.TypeGroupLongDesc.CONCAT_STRING")),
    CountDistinct(
        "COUNT_DISTINCT",
        BaseMessages.getString(PKG, "MemoryGroupByMeta.TypeGroupLongDesc.COUNT_DISTINCT")),
    CountAny(
        "COUNT_ANY", BaseMessages.getString(PKG, "MemoryGroupByMeta.TypeGroupLongDesc.COUNT_ANY")),
    ConcatDistinct(
        "CONCAT_DISTINCT",
        BaseMessages.getString(PKG, "MemoryGroupByMeta.TypeGroupLongDesc.CONCAT_DISTINCT"));

    private String code;
    private String description;

    private GroupType(String code, String description) {
      this.code = code;
      this.description = description;
    }

    public static String[] getDescriptions() {
      String[] descriptions = new String[values().length];
      for (int i = 0; i < values().length; i++) {
        descriptions[i] = values()[i].getDescription();
      }
      return descriptions;
    }

    public static GroupType getTypeWithDescription(String description) {
      for (GroupType value : values()) {
        if (value.getDescription().equalsIgnoreCase(description)) {
          return value;
        }
      }
      return None;
    }

    public static GroupType getTypeWithCode(String code) {
      for (GroupType value : values()) {
        if (value.getCode().equalsIgnoreCase(code)) {
          return value;
        }
      }
      return None;
    }

    /**
     * Gets code
     *
     * @return value of code
     */
    public String getCode() {
      return code;
    }

    /**
     * Gets description
     *
     * @return value of description
     */
    public String getDescription() {
      return description;
    }
  }

  /**
   * Gets groups
   *
   * @return value of groups
   */
  public List<GGroup> getGroups() {
    return groups;
  }

  /**
   * Sets groups
   *
   * @param groups value of groups
   */
  public void setGroups(List<GGroup> groups) {
    this.groups = groups;
  }

  /**
   * Gets aggregates
   *
   * @return value of aggregates
   */
  public List<GAggregate> getAggregates() {
    return aggregates;
  }

  /**
   * Sets aggregates
   *
   * @param aggregates value of aggregates
   */
  public void setAggregates(List<GAggregate> aggregates) {
    this.aggregates = aggregates;
  }

  /**
   * @return the alwaysGivingBackOneRow
   */
  public boolean isAlwaysGivingBackOneRow() {
    return alwaysGivingBackOneRow;
  }

  /**
   * @param alwaysGivingBackOneRow the alwaysGivingBackOneRow to set
   */
  public void setAlwaysGivingBackOneRow(boolean alwaysGivingBackOneRow) {
    this.alwaysGivingBackOneRow = alwaysGivingBackOneRow;
  }

  @Override
  public boolean supportsMultiCopyExecution() {
    return false;
  }
}
