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

package org.apache.hop.pipeline.transforms.denormaliser;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
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

/**
 * The Denormaliser pipeline transform meta-data
 *
 * @author Matt
 * @since 17-jan-2006
 */
@Transform(
    id = "Denormaliser",
    image = "denormaliser.svg",
    name = "i18n::Denormaliser.Name",
    description = "i18n::Denormaliser.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/denormaliser.html")
public class DenormaliserMeta extends BaseTransformMeta
    implements ITransformMeta<Denormaliser, DenormaliserData> {
  private static final Class<?> PKG = DenormaliserMeta.class; // For Translator

  /** Fields to group over */
  private String[] groupField;

  /** The key field */
  private String keyField;

  /** The fields to unpivot */
  private DenormaliserTargetField[] denormaliserTargetField;

  public DenormaliserMeta() {
    super(); // allocate BaseTransformMeta
  }

  /** @return Returns the keyField. */
  public String getKeyField() {
    return keyField;
  }

  /** @param keyField The keyField to set. */
  public void setKeyField(String keyField) {
    this.keyField = keyField;
  }

  /** @return Returns the groupField. */
  public String[] getGroupField() {
    return groupField;
  }

  /** @param groupField The groupField to set. */
  public void setGroupField(String[] groupField) {
    this.groupField = groupField;
  }

  public String[] getDenormaliserTargetFields() {
    String[] fields = new String[denormaliserTargetField.length];
    for (int i = 0; i < fields.length; i++) {
      fields[i] = denormaliserTargetField[i].getTargetName();
    }

    return fields;
  }

  public DenormaliserTargetField searchTargetField(String targetName) {
    for (int i = 0; i < denormaliserTargetField.length; i++) {
      DenormaliserTargetField field = denormaliserTargetField[i];
      if (field.getTargetName().equalsIgnoreCase(targetName)) {
        return field;
      }
    }
    return null;
  }

  /** @return Returns the pivotField. */
  public DenormaliserTargetField[] getDenormaliserTargetField() {
    return denormaliserTargetField;
  }

  /** @param pivotField The pivotField to set. */
  public void setDenormaliserTargetField(DenormaliserTargetField[] pivotField) {
    this.denormaliserTargetField = pivotField;
  }

  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode);
  }

  public void allocate(int sizegroup, int nrFields) {
    groupField = new String[sizegroup];
    denormaliserTargetField = new DenormaliserTargetField[nrFields];
  }

  public Object clone() {
    Object retval = super.clone();
    return retval;
  }

  public void setDefault() {
    int sizegroup = 0;
    int nrFields = 0;

    allocate(sizegroup, nrFields);
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

    // Remove the key value (there will be different entries for each output row)
    //
    if (keyField != null && keyField.length() > 0) {
      int idx = row.indexOfValue(keyField);
      if (idx < 0) {
        throw new HopTransformException(
            BaseMessages.getString(
                PKG, "DenormaliserMeta.Exception.UnableToLocateKeyField", keyField));
      }
      row.removeValueMeta(idx);
    } else {
      throw new HopTransformException(
          BaseMessages.getString(PKG, "DenormaliserMeta.Exception.RequiredKeyField"));
    }

    // Remove all field value(s) (there will be different entries for each output row)
    //
    for (int i = 0; i < denormaliserTargetField.length; i++) {
      String fieldname = denormaliserTargetField[i].getFieldName();
      if (fieldname != null && fieldname.length() > 0) {
        int idx = row.indexOfValue(fieldname);
        if (idx >= 0) {
          row.removeValueMeta(idx);
        }
      } else {
        throw new HopTransformException(
            BaseMessages.getString(
                PKG, "DenormaliserMeta.Exception.RequiredTargetFieldName", (i + 1) + ""));
      }
    }

    // Re-add the target fields
    for (int i = 0; i < denormaliserTargetField.length; i++) {
      DenormaliserTargetField field = denormaliserTargetField[i];
      try {
        IValueMeta target =
            ValueMetaFactory.createValueMeta(field.getTargetName(), field.getTargetType());
        target.setLength(field.getTargetLength(), field.getTargetPrecision());
        target.setOrigin(name);
        row.addValueMeta(target);
      } catch (Exception e) {
        throw new HopTransformException(e);
      }
    }
  }

  private void readData(Node transformNode) throws HopXmlException {
    try {
      keyField = XmlHandler.getTagValue(transformNode, "key_field");

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
        denormaliserTargetField[i] = new DenormaliserTargetField();
        denormaliserTargetField[i].setFieldName(XmlHandler.getTagValue(fnode, "field_name"));
        denormaliserTargetField[i].setKeyValue(XmlHandler.getTagValue(fnode, "key_value"));
        denormaliserTargetField[i].setTargetName(XmlHandler.getTagValue(fnode, "target_name"));
        denormaliserTargetField[i].setTargetType(XmlHandler.getTagValue(fnode, "target_type"));
        denormaliserTargetField[i].setTargetFormat(XmlHandler.getTagValue(fnode, "target_format"));
        denormaliserTargetField[i].setTargetLength(
            Const.toInt(XmlHandler.getTagValue(fnode, "target_length"), -1));
        denormaliserTargetField[i].setTargetPrecision(
            Const.toInt(XmlHandler.getTagValue(fnode, "target_precision"), -1));
        denormaliserTargetField[i].setTargetDecimalSymbol(
            XmlHandler.getTagValue(fnode, "target_decimal_symbol"));
        denormaliserTargetField[i].setTargetGroupingSymbol(
            XmlHandler.getTagValue(fnode, "target_grouping_symbol"));
        denormaliserTargetField[i].setTargetCurrencySymbol(
            XmlHandler.getTagValue(fnode, "target_currency_symbol"));
        denormaliserTargetField[i].setTargetNullString(
            XmlHandler.getTagValue(fnode, "target_null_string"));
        denormaliserTargetField[i].setTargetAggregationType(
            XmlHandler.getTagValue(fnode, "target_aggregation_type"));
      }
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(
              PKG, "DenormaliserMeta.Exception.UnableToLoadTransformMetaFromXML"),
          e);
    }
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder();

    retval.append("      " + XmlHandler.addTagValue("key_field", keyField));

    retval.append("      <group>" + Const.CR);
    for (int i = 0; i < groupField.length; i++) {
      retval.append("        <field>" + Const.CR);
      retval.append("          " + XmlHandler.addTagValue("name", groupField[i]));
      retval.append("          </field>" + Const.CR);
    }
    retval.append("        </group>" + Const.CR);

    retval.append("      <fields>" + Const.CR);
    for (int i = 0; i < denormaliserTargetField.length; i++) {
      DenormaliserTargetField field = denormaliserTargetField[i];

      retval.append("        <field>" + Const.CR);
      retval.append("          " + XmlHandler.addTagValue("field_name", field.getFieldName()));
      retval.append("          " + XmlHandler.addTagValue("key_value", field.getKeyValue()));
      retval.append("          " + XmlHandler.addTagValue("target_name", field.getTargetName()));
      retval.append(
          "          " + XmlHandler.addTagValue("target_type", field.getTargetTypeDesc()));
      retval.append(
          "          " + XmlHandler.addTagValue("target_format", field.getTargetFormat()));
      retval.append(
          "          " + XmlHandler.addTagValue("target_length", field.getTargetLength()));
      retval.append(
          "          " + XmlHandler.addTagValue("target_precision", field.getTargetPrecision()));
      retval.append(
          "          "
              + XmlHandler.addTagValue("target_decimal_symbol", field.getTargetDecimalSymbol()));
      retval.append(
          "          "
              + XmlHandler.addTagValue("target_grouping_symbol", field.getTargetGroupingSymbol()));
      retval.append(
          "          "
              + XmlHandler.addTagValue("target_currency_symbol", field.getTargetCurrencySymbol()));
      retval.append(
          "          " + XmlHandler.addTagValue("target_null_string", field.getTargetNullString()));
      retval.append(
          "          "
              + XmlHandler.addTagValue(
                  "target_aggregation_type", field.getTargetAggregationTypeDesc()));
      retval.append("          </field>" + Const.CR);
    }
    retval.append("        </fields>" + Const.CR);

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

    if (input.length > 0) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "DenormaliserMeta.CheckResult.ReceivingInfoFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "DenormaliserMeta.CheckResult.NoInputReceived"),
              transformMeta);
      remarks.add(cr);
    }
  }

  public Denormaliser createTransform(
      TransformMeta transformMeta,
      DenormaliserData data,
      int cnr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new Denormaliser(transformMeta, this, data, cnr, pipelineMeta, pipeline);
  }

  public DenormaliserData getTransformData() {
    return new DenormaliserData();
  }
}
