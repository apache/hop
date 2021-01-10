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

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.injection.AfterInjection;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaNone;
import org.apache.hop.core.util.Utils;
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

/** Created on 02-jun-2003 */
@InjectionSupported(
    localizationPrefix = "MemoryGroupBy.Injection.",
    groups = {"FIELDS", "AGGREGATES"})
@Transform(
    id = "MemoryGroupBy",
    image = "memorygroupby.svg",
    name = "i18n::BaseTransform.TypeLongDesc.MemoryGroupBy",
    description = "i18n::BaseTransform.TypeTooltipDesc.MemoryGroupBy",
    categoryDescription =
        "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Statistics",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/memgroupby.html")
public class MemoryGroupByMeta extends BaseTransformMeta
    implements ITransformMeta<MemoryGroupBy, MemoryGroupByData> {
  private static final Class<?> PKG = MemoryGroupByMeta.class; // For Translator

  public static final int TYPE_GROUP_NONE = 0;

  public static final int TYPE_GROUP_SUM = 1;

  public static final int TYPE_GROUP_AVERAGE = 2;

  public static final int TYPE_GROUP_MEDIAN = 3;

  public static final int TYPE_GROUP_PERCENTILE = 4;

  public static final int TYPE_GROUP_MIN = 5;

  public static final int TYPE_GROUP_MAX = 6;

  public static final int TYPE_GROUP_COUNT_ALL = 7;

  public static final int TYPE_GROUP_CONCAT_COMMA = 8;

  public static final int TYPE_GROUP_FIRST = 9;

  public static final int TYPE_GROUP_LAST = 10;

  public static final int TYPE_GROUP_FIRST_INCL_NULL = 11;

  public static final int TYPE_GROUP_LAST_INCL_NULL = 12;

  public static final int TYPE_GROUP_STANDARD_DEVIATION = 13;

  public static final int TYPE_GROUP_CONCAT_STRING = 14;

  public static final int TYPE_GROUP_COUNT_DISTINCT = 15;

  public static final int TYPE_GROUP_COUNT_ANY = 16;

  public static final String[]
      typeGroupCode = /* WARNING: DO NOT TRANSLATE THIS. WE ARE SERIOUS, DON'T TRANSLATE! */ {
    "-",
    "SUM",
    "AVERAGE",
    "MEDIAN",
    "PERCENTILE",
    "MIN",
    "MAX",
    "COUNT_ALL",
    "CONCAT_COMMA",
    "FIRST",
    "LAST",
    "FIRST_INCL_NULL",
    "LAST_INCL_NULL",
    "STD_DEV",
    "CONCAT_STRING",
    "COUNT_DISTINCT",
    "COUNT_ANY",
  };

  public static final String[] typeGroupLongDesc = {
    "-",
    BaseMessages.getString(PKG, "MemoryGroupByMeta.TypeGroupLongDesc.SUM"),
    BaseMessages.getString(PKG, "MemoryGroupByMeta.TypeGroupLongDesc.AVERAGE"),
    BaseMessages.getString(PKG, "MemoryGroupByMeta.TypeGroupLongDesc.MEDIAN"),
    BaseMessages.getString(PKG, "MemoryGroupByMeta.TypeGroupLongDesc.PERCENTILE"),
    BaseMessages.getString(PKG, "MemoryGroupByMeta.TypeGroupLongDesc.MIN"),
    BaseMessages.getString(PKG, "MemoryGroupByMeta.TypeGroupLongDesc.MAX"),
    BaseMessages.getString(PKG, "MemoryGroupByMeta.TypeGroupLongDesc.CONCAT_ALL"),
    BaseMessages.getString(PKG, "MemoryGroupByMeta.TypeGroupLongDesc.CONCAT_COMMA"),
    BaseMessages.getString(PKG, "MemoryGroupByMeta.TypeGroupLongDesc.FIRST"),
    BaseMessages.getString(PKG, "MemoryGroupByMeta.TypeGroupLongDesc.LAST"),
    BaseMessages.getString(PKG, "MemoryGroupByMeta.TypeGroupLongDesc.FIRST_INCL_NULL"),
    BaseMessages.getString(PKG, "MemoryGroupByMeta.TypeGroupLongDesc.LAST_INCL_NULL"),
    BaseMessages.getString(PKG, "MemoryGroupByMeta.TypeGroupLongDesc.STANDARD_DEVIATION"),
    BaseMessages.getString(PKG, "MemoryGroupByMeta.TypeGroupLongDesc.CONCAT_STRING"),
    BaseMessages.getString(PKG, "MemoryGroupByMeta.TypeGroupLongDesc.COUNT_DISTINCT"),
    BaseMessages.getString(PKG, "MemoryGroupByMeta.TypeGroupLongDesc.COUNT_ANY"),
  };

  @Injection(name = "GROUPFIELD", group = "FIELDS")
  /** Fields to group over */
  private String[] groupField;

  @Injection(name = "AGGREGATEFIELD", group = "AGGREGATES")
  /** Name of aggregate field */
  private String[] aggregateField;

  @Injection(name = "SUBJECTFIELD", group = "AGGREGATES")
  /** Field name to group over */
  private String[] subjectField;

  @Injection(name = "AGGREGATETYPE", group = "AGGREGATES")
  /** Type of aggregate */
  private int[] aggregateType;

  @Injection(name = "VALUEFIELD", group = "AGGREGATES")
  /** Value to use as separator for ex */
  private String[] valueField;

  @Injection(name = "ALWAYSGIVINGBACKONEROW", group = "FIELDS")
  /** Flag to indicate that we always give back one row. Defaults to true for existing pipelines. */
  private boolean alwaysGivingBackOneRow;

  public MemoryGroupByMeta() {
    super(); // allocate BaseTransformMeta
  }

  /** @return Returns the aggregateField. */
  public String[] getAggregateField() {
    return aggregateField;
  }

  /** @param aggregateField The aggregateField to set. */
  public void setAggregateField(String[] aggregateField) {
    this.aggregateField = aggregateField;
  }

  /** @return Returns the aggregateType. */
  public int[] getAggregateType() {
    return aggregateType;
  }

  /** @param aggregateType The aggregateType to set. */
  public void setAggregateType(int[] aggregateType) {
    this.aggregateType = aggregateType;
  }

  /** @return Returns the groupField. */
  public String[] getGroupField() {
    return groupField;
  }

  /** @param groupField The groupField to set. */
  public void setGroupField(String[] groupField) {
    this.groupField = groupField;
  }

  /** @return Returns the subjectField. */
  public String[] getSubjectField() {
    return subjectField;
  }

  /** @param subjectField The subjectField to set. */
  public void setSubjectField(String[] subjectField) {
    this.subjectField = subjectField;
  }

  /** @return Returns the valueField. */
  public String[] getValueField() {
    return valueField;
  }

  /** @param valueField The valueField to set. */
  public void setValueField(String[] valueField) {
    this.valueField = valueField;
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode);
  }

  public void allocate(int sizegroup, int nrFields) {
    groupField = new String[sizegroup];
    aggregateField = new String[nrFields];
    subjectField = new String[nrFields];
    aggregateType = new int[nrFields];
    valueField = new String[nrFields];
  }

  @Override
  public Object clone() {
    MemoryGroupByMeta retval = (MemoryGroupByMeta) super.clone();
    int nrFields = aggregateField.length;
    int nrGroups = groupField.length;

    retval.allocate(nrGroups, nrFields);
    System.arraycopy(groupField, 0, retval.groupField, 0, nrGroups);
    System.arraycopy(aggregateField, 0, retval.aggregateField, 0, nrFields);
    System.arraycopy(subjectField, 0, retval.subjectField, 0, nrFields);
    System.arraycopy(aggregateType, 0, retval.aggregateType, 0, nrFields);
    System.arraycopy(valueField, 0, retval.valueField, 0, nrFields);
    return retval;
  }

  @Override
  public MemoryGroupBy createTransform(
      TransformMeta transformMeta,
      MemoryGroupByData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new MemoryGroupBy(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  private void readData(Node transformNode) throws HopXmlException {
    try {
      Node groupn = XmlHandler.getSubNode(transformNode, "group");
      Node fields = XmlHandler.getSubNode(transformNode, "fields");

      int sizegroup = XmlHandler.countNodes(groupn, "field");
      int nrFields = XmlHandler.countNodes(fields, "field");

      allocate(sizegroup, nrFields);

      for (int i = 0; i < sizegroup; i++) {
        Node fnode = XmlHandler.getSubNodeByNr(groupn, "field", i);
        groupField[i] = XmlHandler.getTagValue(fnode, "name");
      }

      boolean hasNumberOfValues = false;
      for (int i = 0; i < nrFields; i++) {
        Node fnode = XmlHandler.getSubNodeByNr(fields, "field", i);
        aggregateField[i] = XmlHandler.getTagValue(fnode, "aggregate");
        subjectField[i] = XmlHandler.getTagValue(fnode, "subject");
        aggregateType[i] = getType(XmlHandler.getTagValue(fnode, "type"));

        if (aggregateType[i] == TYPE_GROUP_COUNT_ALL
            || aggregateType[i] == TYPE_GROUP_COUNT_DISTINCT
            || aggregateType[i] == TYPE_GROUP_COUNT_ANY) {
          hasNumberOfValues = true;
        }

        valueField[i] = XmlHandler.getTagValue(fnode, "valuefield");
      }

      String giveBackRow = XmlHandler.getTagValue(transformNode, "give_back_row");
      if (Utils.isEmpty(giveBackRow)) {
        alwaysGivingBackOneRow = hasNumberOfValues;
      } else {
        alwaysGivingBackOneRow = "Y".equalsIgnoreCase(giveBackRow);
      }
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(
              PKG, "MemoryGroupByMeta.Exception.UnableToLoadTransformMetaFromXML"),
          e);
    }
  }

  public static final int getType(String desc) {
    for (int i = 0; i < typeGroupCode.length; i++) {
      if (typeGroupCode[i].equalsIgnoreCase(desc)) {
        return i;
      }
    }
    for (int i = 0; i < typeGroupLongDesc.length; i++) {
      if (typeGroupLongDesc[i].equalsIgnoreCase(desc)) {
        return i;
      }
    }
    return 0;
  }

  public static final String getTypeDesc(int i) {
    if (i < 0 || i >= typeGroupCode.length) {
      return null;
    }
    return typeGroupCode[i];
  }

  public static final String getTypeDescLong(int i) {
    if (i < 0 || i >= typeGroupLongDesc.length) {
      return null;
    }
    return typeGroupLongDesc[i];
  }

  @Override
  public void setDefault() {
    int sizegroup = 0;
    int nrFields = 0;

    allocate(sizegroup, nrFields);
  }

  @Override
  public void getFields(
      IRowMeta r,
      String origin,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    // Check compatibility mode
    boolean compatibilityMode =
        ValueMetaBase.convertStringToBoolean(
            variables.getVariable(
                Const.HOP_COMPATIBILITY_MEMORY_GROUP_BY_SUM_AVERAGE_RETURN_NUMBER_TYPE, "N"));

    // re-assemble a new row of metadata
    //
    IRowMeta fields = new RowMeta();

    // Add the grouping fields in the correct order...
    //
    for (int i = 0; i < groupField.length; i++) {
      IValueMeta valueMeta = r.searchValueMeta(groupField[i]);
      if (valueMeta != null) {
        valueMeta.setStorageType(IValueMeta.STORAGE_TYPE_NORMAL);
        fields.addValueMeta(valueMeta);
      }
    }

    // Re-add aggregates
    //
    for (int i = 0; i < subjectField.length; i++) {
      IValueMeta subj = r.searchValueMeta(subjectField[i]);
      if (subj != null || aggregateType[i] == TYPE_GROUP_COUNT_ANY) {
        String valueName = aggregateField[i];
        int valueType = IValueMeta.TYPE_NONE;
        int length = -1;
        int precision = -1;

        switch (aggregateType[i]) {
          case TYPE_GROUP_FIRST:
          case TYPE_GROUP_LAST:
          case TYPE_GROUP_FIRST_INCL_NULL:
          case TYPE_GROUP_LAST_INCL_NULL:
          case TYPE_GROUP_MIN:
          case TYPE_GROUP_MAX:
            valueType = subj.getType();
            break;
          case TYPE_GROUP_COUNT_DISTINCT:
          case TYPE_GROUP_COUNT_ALL:
          case TYPE_GROUP_COUNT_ANY:
            valueType = IValueMeta.TYPE_INTEGER;
            break;
          case TYPE_GROUP_CONCAT_COMMA:
            valueType = IValueMeta.TYPE_STRING;
            break;
          case TYPE_GROUP_SUM:
          case TYPE_GROUP_AVERAGE:
            if (!compatibilityMode && subj.isNumeric()) {
              valueType = subj.getType();
            } else {
              valueType = IValueMeta.TYPE_NUMBER;
            }
            break;
          case TYPE_GROUP_MEDIAN:
          case TYPE_GROUP_PERCENTILE:
          case TYPE_GROUP_STANDARD_DEVIATION:
            valueType = IValueMeta.TYPE_NUMBER;
            break;
          case TYPE_GROUP_CONCAT_STRING:
            valueType = IValueMeta.TYPE_STRING;
            break;
          default:
            break;
        }

        if (aggregateType[i] == TYPE_GROUP_SUM
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
            log.logError(
                BaseMessages.getString(PKG, "MemoryGroupByMeta.Exception.UnknownValueMetaType"),
                valueType,
                e);
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

    // Now that we have all the fields we want, we should clear the original row and replace the
    // values...
    //
    r.clear();
    r.addRowMeta(fields);
  }

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder(500);

    retval.append("      ").append(XmlHandler.addTagValue("give_back_row", alwaysGivingBackOneRow));

    retval.append("      <group>").append(Const.CR);
    for (int i = 0; i < groupField.length; i++) {
      retval.append("        <field>").append(Const.CR);
      retval.append("          ").append(XmlHandler.addTagValue("name", groupField[i]));
      retval.append("        </field>").append(Const.CR);
    }
    retval.append("      </group>").append(Const.CR);

    retval.append("      <fields>").append(Const.CR);
    for (int i = 0; i < subjectField.length; i++) {
      retval.append("        <field>").append(Const.CR);
      retval.append("          ").append(XmlHandler.addTagValue("aggregate", aggregateField[i]));
      retval.append("          ").append(XmlHandler.addTagValue("subject", subjectField[i]));
      retval
          .append("          ")
          .append(XmlHandler.addTagValue("type", getTypeDesc(aggregateType[i])));
      retval.append("          ").append(XmlHandler.addTagValue("valuefield", valueField[i]));
      retval.append("        </field>").append(Const.CR);
    }
    retval.append("      </fields>").append(Const.CR);

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
  public MemoryGroupByData getTransformData() {
    return new MemoryGroupByData();
  }

  /** @return the alwaysGivingBackOneRow */
  public boolean isAlwaysGivingBackOneRow() {
    return alwaysGivingBackOneRow;
  }

  /** @param alwaysGivingBackOneRow the alwaysGivingBackOneRow to set */
  public void setAlwaysGivingBackOneRow(boolean alwaysGivingBackOneRow) {
    this.alwaysGivingBackOneRow = alwaysGivingBackOneRow;
  }

  /**
   * If we use injection we can have different arrays lengths. We need synchronize them for
   * consistency behavior with UI
   */
  @AfterInjection
  public void afterInjectionSynchronization() {
    int nrFields = (subjectField == null ? -1 : subjectField.length);
    if (nrFields <= 0) {
      return;
    }
    String[][] normalizedStringArrays = Utils.normalizeArrays(nrFields, aggregateField, valueField);
    aggregateField = normalizedStringArrays[0];
    valueField = normalizedStringArrays[1];

    int[][] normalizedIntArrays = Utils.normalizeArrays(nrFields, aggregateType);
    aggregateType = normalizedIntArrays[0];
  }
}
