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

package org.apache.hop.pipeline.transforms.nullif;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionDeep;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
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

@Transform(
    id = "NullIf",
    image = "NullIf.svg",
    name = "i18n::NullIf.Name",
    description = "i18n::NullIf.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Flow",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/nullif.html")
@InjectionSupported(
    localizationPrefix = "Injection.NullIf.",
    groups = {"FIELDS"})
public class NullIfMeta extends BaseTransformMeta implements ITransformMeta<NullIf, NullIfData> {

  private static final Class<?> PKG = NullIfMeta.class; // For Translator

  @Override
  public NullIf createTransform(
      TransformMeta transformMeta,
      NullIfData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new NullIf(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  public static class Field implements Cloneable {

    @Injection(name = "FIELDNAME", group = "FIELDS")
    private String fieldName;

    @Injection(name = "FIELDVALUE", group = "FIELDS")
    private String fieldValue;

    /** @return Returns the fieldName. */
    public String getFieldName() {
      return fieldName;
    }

    /** @param fieldName The fieldName to set. */
    public void setFieldName(String fieldName) {
      this.fieldName = fieldName;
    }

    /** @return Returns the fieldValue. */
    public String getFieldValue() {
      return fieldValue;
    }

    /** @param fieldValue The fieldValue to set. */
    public void setFieldValue(String fieldValue) {
      Boolean isEmptyAndNullDiffer =
          ValueMetaBase.convertStringToBoolean(
              Const.NVL(System.getProperty(Const.HOP_EMPTY_STRING_DIFFERS_FROM_NULL, "N"), "N"));

      this.fieldValue =
          fieldValue == null && isEmptyAndNullDiffer ? Const.EMPTY_STRING : fieldValue;
    }

    public Field clone() {
      try {
        return (Field) super.clone();
      } catch (CloneNotSupportedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @InjectionDeep private Field[] fields;

  public NullIfMeta() {
    super(); // allocate BaseTransformMeta
  }

  public Field[] getFields() {
    return fields;
  }

  public void setFields(Field[] fields) {
    this.fields = fields;
  }

  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode);
  }

  public void allocate(int count) {
    fields = new Field[count];
    for (int i = 0; i < count; i++) {
      fields[i] = new Field();
    }
  }

  public Object clone() {
    NullIfMeta retval = (NullIfMeta) super.clone();

    int count = fields.length;

    retval.allocate(count);

    for (int i = 0; i < count; i++) {
      retval.getFields()[i] = fields[i].clone();
    }
    return retval;
  }

  private void readData(Node transformNode) throws HopXmlException {
    try {
      Node fieldNodes = XmlHandler.getSubNode(transformNode, "fields");
      int count = XmlHandler.countNodes(fieldNodes, "field");

      allocate(count);

      for (int i = 0; i < count; i++) {
        Node fnode = XmlHandler.getSubNodeByNr(fieldNodes, "field", i);

        fields[i].setFieldName(XmlHandler.getTagValue(fnode, "name"));
        fields[i].setFieldValue(XmlHandler.getTagValue(fnode, "value"));
      }
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "NullIfMeta.Exception.UnableToReadTransformMetaFromXML"), e);
    }
  }

  public void setDefault() {
    int count = 0;

    allocate(count);

    for (int i = 0; i < count; i++) {
      fields[i].setFieldName("field" + i);
      fields[i].setFieldValue("");
    }
  }

  public void getFields(
      IRowMeta r,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    if (r == null) {
      r = new RowMeta(); // give back values
      // Meta-data doesn't change here, only the value possibly turns to NULL
    }

    return;
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder();

    retval.append("    <fields>" + Const.CR);

    for (int i = 0; i < fields.length; i++) {
      retval.append("      <field>" + Const.CR);
      retval.append("        " + XmlHandler.addTagValue("name", fields[i].getFieldName()));
      retval.append("        " + XmlHandler.addTagValue("value", fields[i].getFieldValue()));
      retval.append("        </field>" + Const.CR);
    }
    retval.append("      </fields>" + Const.CR);

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
    if (prev == null || prev.size() == 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_WARNING,
              BaseMessages.getString(PKG, "NullIfMeta.CheckResult.NoReceivingFieldsError"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "NullIfMeta.CheckResult.TransformReceivingFieldsOK", prev.size() + ""),
              transformMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "NullIfMeta.CheckResult.TransformRecevingInfoFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "NullIfMeta.CheckResult.NoInputReceivedError"),
              transformMeta);
      remarks.add(cr);
    }
  }

  public NullIfData getTransformData() {
    return new NullIfData();
  }
}
