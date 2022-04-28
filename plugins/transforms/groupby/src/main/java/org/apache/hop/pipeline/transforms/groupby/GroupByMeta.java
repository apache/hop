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

package org.apache.hop.pipeline.transforms.groupby;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNone;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;

@Transform(
    id = "GroupBy",
    image = "groupby.svg",
    name = "i18n::GroupBy.Name",
    description = "i18n::GroupBy.Description",
    categoryDescription =
        "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Statistics",
    documentationUrl = "/pipeline/transforms/groupby.html",
    keywords = "i18n::GroupByMeta.keyword")
public class GroupByMeta extends BaseTransformMeta<GroupBy, GroupByData> {

  private static final Class<?> PKG = GroupByMeta.class; // For Translator

  /** All rows need to pass, adding an extra row at the end of each group/block. */
  @HopMetadataProperty(key="all_rows", injectionKey = "PASS_ALL_ROWS", injectionKeyDescription = "GroupByMeta.Injection.PASS_ALL_ROWS")
  private boolean passAllRows;

  /** Directory to store the temp files */
  @HopMetadataProperty(injectionKey = "TEMP_DIRECTORY", injectionKeyDescription = "GroupByMeta.Injection.TEMP_DIRECTORY")
  private String directory;

  /** Temp files prefix... */
  @HopMetadataProperty(injectionKey = "TEMP_FILE_PREFIX", injectionKeyDescription = "GroupByMeta.Injection.TEMP_FILE_PREFIX")
  private String prefix;

  /** Indicate that some rows don't need to be considered : TODO: make work in GUI & worker */
  @HopMetadataProperty(key="ignore_aggregate")
  private boolean aggregateIgnored;

  /**
   * name of the boolean field that indicates we need to ignore the row : TODO: make work in GUI &
   * worker
   */
  @HopMetadataProperty(key="field_ignore")
  private String aggregateIgnoredField;

  /** Fields to group over */
  @HopMetadataProperty(groupKey = "group", key = "field", injectionGroupKey = "GROUPS", injectionGroupDescription = "GroupByMeta.Injection.GROUPS")
  private List<GroupingField> groupingFields;

  @HopMetadataProperty(groupKey = "fields", key = "field", injectionGroupKey = "AGGREGATIONS", injectionGroupDescription = "GroupByMeta.Injection.AGGREGATIONS")
  private List<Aggregation> aggregations;

  /** Add a linenr in the group, resetting to 0 in a new group. */
  @HopMetadataProperty(key="add_linenr", injectionKey = "ADD_GROUP_LINENR", injectionKeyDescription = "GroupByMeta.Injection.ADD_GROUP_LINENR")
  private boolean addingLineNrInGroup;

  /** The fieldname that will contain the added integer field */
  @HopMetadataProperty(key="linenr_fieldname", injectionKey = "ADD_GROUP_LINENR_FIELD", injectionKeyDescription = "GroupByMeta.Injection.ADD_GROUP_LINENR_FIELD")
  private String lineNrInGroupField;

  /** Flag to indicate that we always give back one row. Defaults to true for existing pipelines. */
  @HopMetadataProperty(key="give_back_row", injectionKey = "ALWAYS_GIVE_ROW", injectionKeyDescription = "GroupByMeta.Injection.ALWAYS_GIVE_ROW")
  private boolean alwaysGivingBackOneRow;

  public GroupByMeta() {
    super(); // allocate BaseTransformMeta
    groupingFields = new ArrayList<>();
    aggregations = new ArrayList<>();
  }

  /**
   * @return Returns the aggregateIgnored.
   */
  public boolean isAggregateIgnored() {
    return aggregateIgnored;
  }

  /**
   * @param aggregateIgnored The aggregateIgnored to set.
   */
  public void setAggregateIgnored(boolean aggregateIgnored) {
    this.aggregateIgnored = aggregateIgnored;
  }

  /**
   * @return Returns the aggregateIgnoredField.
   */
  public String getAggregateIgnoredField() {
    return aggregateIgnoredField;
  }

  /**
   * @param aggregateIgnoredField The aggregateIgnoredField to set.
   */
  public void setAggregateIgnoredField(String aggregateIgnoredField) {
    this.aggregateIgnoredField = aggregateIgnoredField;
  }

  /**
   * @return Returns the groupField.
   */
  public List<GroupingField> getGroupingFields() {
    return groupingFields;
  }

  /**
   * @param groupingFields The groupField to set.
   */
  public void setGroupingFields(List<GroupingField> groupingFields) {
    this.groupingFields = groupingFields;
  }

  /**
   * @return Returns the passAllRows.
   */
  public boolean isPassAllRows() {
    return passAllRows;
  }

  /**
   * @param passAllRows The passAllRows to set.
   */
  public void setPassAllRows(boolean passAllRows) {
    this.passAllRows = passAllRows;
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {

    super.loadXml(transformNode, metadataProvider);

    boolean hasNumberOfValues = false;
    for (Aggregation item : aggregations) {

      int aggType = item.getType();

     if (aggType == Aggregation.TYPE_GROUP_COUNT_ALL
              || aggType == Aggregation.TYPE_GROUP_COUNT_DISTINCT
              || aggType == Aggregation.TYPE_GROUP_COUNT_ANY) {
        hasNumberOfValues = true;
      }

    }

    if (!alwaysGivingBackOneRow) {
      alwaysGivingBackOneRow = hasNumberOfValues;
    }
  }

  @Override
  public Object clone() {
    GroupByMeta groupByMeta = (GroupByMeta) super.clone();

    List<GroupingField> groupingFieldsCopy = new ArrayList<>();
    for (GroupingField item : groupingFields) {
      groupingFieldsCopy.add(item.clone());
    }
    groupByMeta.setGroupingFields(groupingFieldsCopy);

    List<Aggregation> aggsCopy = new ArrayList<>();
    for (Aggregation aggregation : aggregations) {
      aggsCopy.add(aggregation.clone());
    }
    groupByMeta.setAggregations(aggsCopy);

    return groupByMeta;
  }

  @Override
  public void setDefault() {
    directory = "${java.io.tmpdir}";
    prefix = "grp";

    passAllRows = false;
    aggregateIgnored = false;
    aggregateIgnoredField = null;

    int sizeGroup = 0;
  }

  @Override
  public void getFields(
      IRowMeta rowMeta,
      String origin,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    // re-assemble a new row of metadata
    //
    IRowMeta fields = new RowMeta();

    if (!passAllRows) {
      // Add the grouping fields in the correct order...
      //
      for (int i = 0; i < groupingFields.size(); i++) {
        IValueMeta valueMeta = rowMeta.searchValueMeta(groupingFields.get(i).getName());
        if (valueMeta != null) {
          fields.addValueMeta(valueMeta);
        }
      }
    } else {
      // Add all the original fields from the incoming row meta
      //
      fields.addRowMeta(rowMeta);
    }

    // Re-add aggregates
    //
    for (Aggregation aggregation : aggregations) {
      int aggregationType = aggregation.getType();
      IValueMeta subj = rowMeta.searchValueMeta(aggregation.getSubject());
      if (subj != null || aggregationType == Aggregation.TYPE_GROUP_COUNT_ANY) {
        String valueName = aggregation.getField();
        int valueType = IValueMeta.TYPE_NONE;
        int length = -1;
        int precision = -1;

        switch (aggregationType) {
          case Aggregation.TYPE_GROUP_SUM:
          case Aggregation.TYPE_GROUP_AVERAGE:
          case Aggregation.TYPE_GROUP_CUMULATIVE_SUM:
          case Aggregation.TYPE_GROUP_CUMULATIVE_AVERAGE:
          case Aggregation.TYPE_GROUP_FIRST:
          case Aggregation.TYPE_GROUP_LAST:
          case Aggregation.TYPE_GROUP_FIRST_INCL_NULL:
          case Aggregation.TYPE_GROUP_LAST_INCL_NULL:
          case Aggregation.TYPE_GROUP_MIN:
          case Aggregation.TYPE_GROUP_MAX:
            valueType = subj.getType();
            break;
          case Aggregation.TYPE_GROUP_COUNT_DISTINCT:
          case Aggregation.TYPE_GROUP_COUNT_ANY:
          case Aggregation.TYPE_GROUP_COUNT_ALL:
            valueType = IValueMeta.TYPE_INTEGER;
            break;
          case Aggregation.TYPE_GROUP_CONCAT_COMMA:
            valueType = IValueMeta.TYPE_STRING;
            break;
          case Aggregation.TYPE_GROUP_STANDARD_DEVIATION:
          case Aggregation.TYPE_GROUP_MEDIAN:
          case Aggregation.TYPE_GROUP_STANDARD_DEVIATION_SAMPLE:
          case Aggregation.TYPE_GROUP_PERCENTILE:
          case Aggregation.TYPE_GROUP_PERCENTILE_NEAREST_RANK:
            valueType = IValueMeta.TYPE_NUMBER;
            break;
          case Aggregation.TYPE_GROUP_CONCAT_STRING:
          case Aggregation.TYPE_GROUP_CONCAT_STRING_CRLF:
            valueType = IValueMeta.TYPE_STRING;
            break;
          default:
            break;
        }

        // Change type from integer to number in case off averages for cumulative average
        //
        if (aggregationType == Aggregation.TYPE_GROUP_CUMULATIVE_AVERAGE
            && valueType == IValueMeta.TYPE_INTEGER) {
          valueType = IValueMeta.TYPE_NUMBER;
          precision = -1;
          length = -1;
        } else if (aggregationType == Aggregation.TYPE_GROUP_COUNT_ALL
            || aggregationType == Aggregation.TYPE_GROUP_COUNT_DISTINCT
            || aggregationType == Aggregation.TYPE_GROUP_COUNT_ANY) {
          length = IValueMeta.DEFAULT_INTEGER_LENGTH;
          precision = 0;
        } else if (aggregationType == Aggregation.TYPE_GROUP_SUM
            && valueType != IValueMeta.TYPE_INTEGER
            && valueType != IValueMeta.TYPE_NUMBER
            && valueType != IValueMeta.TYPE_BIGNUMBER) {
          // If it ain't numeric, we change it to Number
          //
          valueType = IValueMeta.TYPE_NUMBER;
          precision = -1;
          length = -1;
        }

        if (valueType != IValueMeta.TYPE_NONE) {
          IValueMeta v;
          try {
            v = ValueMetaFactory.createValueMeta(valueName, valueType);
          } catch (HopPluginException e) {
            v = new ValueMetaNone(valueName);
          }
          v.setOrigin(origin);
          v.setLength(length, precision);

          if (subj != null) {
            v.setConversionMask(subj.getConversionMask());
          }

          fields.addValueMeta(v);
        }
      }
    }

    if (passAllRows) {
      // If we pass all rows, we can add a line nr in the group...
      if (addingLineNrInGroup && !Utils.isEmpty(lineNrInGroupField)) {
        IValueMeta lineNr = new ValueMetaInteger(lineNrInGroupField);
        lineNr.setLength(IValueMeta.DEFAULT_INTEGER_LENGTH, 0);
        lineNr.setOrigin(origin);
        fields.addValueMeta(lineNr);
      }
    }

    // Now that we have all the fields we want, we should clear the original row and replace the
    // values...
    //
    rowMeta.clear();
    rowMeta.addRowMeta(fields);
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
              BaseMessages.getString(PKG, "GroupByMeta.CheckResult.ReceivingInfoOK"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "GroupByMeta.CheckResult.NoInputError"),
              transformMeta);
      remarks.add(cr);
    }
  }

  /**
   * @return Returns the directory.
   */
  public String getDirectory() {
    return directory;
  }

  /**
   * @param directory The directory to set.
   */
  public void setDirectory(String directory) {
    this.directory = directory;
  }

  /**
   * @return Returns the prefix.
   */
  public String getPrefix() {
    return prefix;
  }

  /**
   * @param prefix The prefix to set.
   */
  public void setPrefix(String prefix) {
    this.prefix = prefix;
  }

  /**
   * @return the addingLineNrInGroup
   */
  public boolean isAddingLineNrInGroup() {
    return addingLineNrInGroup;
  }

  /**
   * @param addingLineNrInGroup the addingLineNrInGroup to set
   */
  public void setAddingLineNrInGroup(boolean addingLineNrInGroup) {
    this.addingLineNrInGroup = addingLineNrInGroup;
  }

  /**
   * @return the lineNrInGroupField
   */
  public String getLineNrInGroupField() {
    return lineNrInGroupField;
  }

  /**
   * @param lineNrInGroupField the lineNrInGroupField to set
   */
  public void setLineNrInGroupField(String lineNrInGroupField) {
    this.lineNrInGroupField = lineNrInGroupField;
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
  public PipelineMeta.PipelineType[] getSupportedPipelineTypes() {
    return new PipelineMeta.PipelineType[] {PipelineMeta.PipelineType.Normal};
  }

  /**
   * Gets aggregations
   *
   * @return value of aggregations
   */
  public List<Aggregation> getAggregations() {
    return aggregations;
  }

  /**
   * @param aggregations The aggregations to set
   */
  public void setAggregations(List<Aggregation> aggregations) {
    this.aggregations = aggregations;
  }
}
