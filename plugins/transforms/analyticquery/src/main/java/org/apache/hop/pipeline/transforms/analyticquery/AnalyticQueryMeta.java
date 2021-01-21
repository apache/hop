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

package org.apache.hop.pipeline.transforms.analyticquery;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.injection.AfterInjection;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelineMeta.PipelineType;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

import java.util.List;

/**
 * @author ngoodman
 * @since 27-jan-2009
 */
@Transform(
    id = "AnalyticQuery",
    image = "analyticquery.svg",
    name = "i18n::AnalyticQuery.Name",
    description = "i18n::AnalyticQuery.Description",
    categoryDescription =
        "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Statistics",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/analyticquery.html")
@InjectionSupported(localizationPrefix = "AnalyticQuery.Injection.")
public class AnalyticQueryMeta extends BaseTransformMeta
    implements ITransformMeta<AnalyticQuery, AnalyticQueryData> {

  private static final Class<?> PKG = AnalyticQuery.class; // For Translator

  public static final int TYPE_FUNCT_LEAD = 0;
  public static final int TYPE_FUNCT_LAG = 1;

  public static final String[]
      typeGroupCode = /* WARNING: DO NOT TRANSLATE THIS. WE ARE SERIOUS, DON'T TRANSLATE! */ {
    "LEAD", "LAG",
  };

  public static final String[] typeGroupLongDesc = {
    BaseMessages.getString(PKG, "AnalyticQueryMeta.TypeGroupLongDesc.LEAD"),
    BaseMessages.getString(PKG, "AnalyticQueryMeta.TypeGroupLongDesc.LAG")
  };

  /** Fields to partition by ie, CUSTOMER, PRODUCT */
  @Injection(name = "GROUP_FIELDS")
  private String[] groupField;

  private int numberOfFields;
  /** BEGIN arrays (each of size number_of_fields) */

  /** Name of OUTPUT fieldname "MYNEWLEADFUNCTION" */
  @Injection(name = "OUTPUT.AGGREGATE_FIELD")
  private String[] aggregateField;
  /** Name of the input fieldname it operates on "ORDERTOTAL" */
  @Injection(name = "OUTPUT.SUBJECT_FIELD")
  private String[] subjectField;
  /** Aggregate type (LEAD/LAG, etc) */
  @Injection(name = "OUTPUT.AGGREGATE_TYPE")
  private int[] aggregateType;
  /** Offset "N" of how many rows to go forward/back */
  @Injection(name = "OUTPUT.VALUE_FIELD")
  private int[] valueField;

  /** END arrays are one for each configured analytic function */
  public AnalyticQueryMeta() {
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

  /** @return Returns the aggregateTypes. */
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
  public int[] getValueField() {
    return valueField;
  }

  /** @param valueField The valueField to set. */
  public void setValueField(int[] valueField) {
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
    valueField = new int[nrFields];

    numberOfFields = nrFields;
  }

  public Object clone() {
    Object retval = super.clone();
    return retval;
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
      for (int i = 0; i < nrFields; i++) {
        Node fnode = XmlHandler.getSubNodeByNr(fields, "field", i);
        aggregateField[i] = XmlHandler.getTagValue(fnode, "aggregate");
        subjectField[i] = XmlHandler.getTagValue(fnode, "subject");
        aggregateType[i] = getType(XmlHandler.getTagValue(fnode, "type"));

        valueField[i] = Integer.parseInt(XmlHandler.getTagValue(fnode, "valuefield"));
      }

    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(
              PKG, "AnalyticQueryMeta.Exception.UnableToLoadTransformMetaFromXML"),
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
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    // re-assemble a new row of metadata
    //
    IRowMeta fields = new RowMeta();

    // Add existing values
    fields.addRowMeta(r);

    // add analytic values
    for (int i = 0; i < numberOfFields; i++) {

      int indexOf_subject = -1;
      indexOf_subject = r.indexOfValue(subjectField[i]);

      // if we found the subjectField in the IRowMeta, and we should....
      if (indexOf_subject > -1) {
        IValueMeta vmi = r.getValueMeta(indexOf_subject).clone();
        vmi.setOrigin(origin);
        vmi.setName(aggregateField[i]);
        fields.addValueMeta(r.size() + i, vmi);
      } else {
        // we have a condition where the subjectField can't be found from the iRowMeta
        StringBuilder sbfieldNames = new StringBuilder();
        String[] fieldNames = r.getFieldNames();
        for (int j = 0; j < fieldNames.length; j++) {
          sbfieldNames.append("[" + fieldNames[j] + "]" + (j < fieldNames.length - 1 ? ", " : ""));
        }
        throw new HopTransformException(
            BaseMessages.getString(
                PKG,
                "AnalyticQueryMeta.Exception.SubjectFieldNotFound",
                getParentTransformMeta().getName(),
                subjectField[i],
                sbfieldNames.toString()));
      }
    }

    r.clear();
    // Add back to Row Meta
    r.addRowMeta(fields);
  }

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder(500);

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
              BaseMessages.getString(PKG, "AnalyticQueryMeta.CheckResult.ReceivingInfoOK"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "AnalyticQueryMeta.CheckResult.NoInputError"),
              transformMeta);
      remarks.add(cr);
    }
  }

  @Override
  public AnalyticQuery createTransform(
      TransformMeta transformMeta,
      AnalyticQueryData data,
      int cnr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new AnalyticQuery(transformMeta, this, data, cnr, pipelineMeta, pipeline);
  }

  @Override
  public AnalyticQueryData getTransformData() {
    return new AnalyticQueryData();
  }

  public int getNumberOfFields() {
    return numberOfFields;
  }

  public void setNumberOfFields(int numberOfFields) {
    this.numberOfFields = numberOfFields;
  }

  @Override
  public PipelineType[] getSupportedPipelineTypes() {
    return new PipelineType[] {
      PipelineType.Normal,
    };
  }

  /**
   * If we use injection we can have different arrays lengths. We need synchronize them for
   * consistency behavior with UI
   */
  @AfterInjection
  public void afterInjectionSynchronization() {
    int nrFields = (subjectField == null) ? -1 : subjectField.length;
    if (nrFields <= 0) {
      return;
    }
    String[][] rtn = Utils.normalizeArrays(nrFields, aggregateField);
    aggregateField = rtn[0];

    int[][] rtnInt = Utils.normalizeArrays(nrFields, aggregateType, valueField);
    aggregateType = rtnInt[0];
    valueField = rtnInt[1];
  }
}
