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

package org.apache.hop.pipeline.transforms.flattener;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

import java.util.List;

/** The flattener transform meta-data */
@Transform(
    id = "Flattener,Flatterner",
    image = "flattener.svg",
    name = "i18n::BaseTransform.TypeLongDesc.RowFlattener",
    description = "i18n::BaseTransform.TypeTooltipDesc.RowFlattener",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    keywords = "i18n::FlattenerMeta.keyword",
    documentationUrl = "/pipeline/transforms/rowflattener.html")
public class FlattenerMeta extends BaseTransformMeta<Flattener, FlattenerData> {
  private static final Class<?> PKG = FlattenerMeta.class; // For Translator

  /** The field to flatten */
  private String fieldName;

  /** Fields to flatten, same data type as input */
  private String[] targetField;

  public FlattenerMeta() {
    super(); // allocate BaseTransformMeta
  }

  public String getFieldName() {
    return fieldName;
  }

  public void setFieldName(String fieldName) {
    this.fieldName = fieldName;
  }

  public String[] getTargetField() {
    return targetField;
  }

  public void setTargetField(String[] targetField) {
    this.targetField = targetField;
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode);
  }

  public void allocate(int nrFields) {
    targetField = new String[nrFields];
  }

  @Override
  public Object clone() {
    Object retval = super.clone();
    return retval;
  }

  @Override
  public void setDefault() {
    int nrFields = 0;

    allocate(nrFields);
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
    if (fieldName != null && fieldName.length() > 0) {
      int idx = row.indexOfValue(fieldName);
      if (idx < 0) {
        throw new HopTransformException(
            BaseMessages.getString(
                PKG, "FlattenerMeta.Exception.UnableToLocateFieldInInputFields", fieldName));
      }

      IValueMeta v = row.getValueMeta(idx);
      row.removeValueMeta(idx);

      for (int i = 0; i < targetField.length; i++) {
        IValueMeta value = v.clone();
        value.setName(targetField[i]);
        value.setOrigin(name);

        row.addValueMeta(value);
      }
    } else {
      throw new HopTransformException(
          BaseMessages.getString(PKG, "FlattenerMeta.Exception.FlattenFieldRequired"));
    }
  }

  private void readData(Node transformNode) throws HopXmlException {
    try {
      fieldName = XmlHandler.getTagValue(transformNode, "field_name");

      Node fields = XmlHandler.getSubNode(transformNode, "fields");
      int nrFields = XmlHandler.countNodes(fields, "field");

      allocate(nrFields);

      for (int i = 0; i < nrFields; i++) {
        Node fnode = XmlHandler.getSubNodeByNr(fields, "field", i);
        targetField[i] = XmlHandler.getTagValue(fnode, "name");
      }
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "FlattenerMeta.Exception.UnableToLoadTransformMetaFromXML"),
          e);
    }
  }

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder();

    retval.append("      " + XmlHandler.addTagValue("field_name", fieldName));

    retval.append("      <fields>" + Const.CR);
    for (int i = 0; i < targetField.length; i++) {
      retval.append("        <field>" + Const.CR);
      retval.append("          " + XmlHandler.addTagValue("name", targetField[i]));
      retval.append("          </field>" + Const.CR);
    }
    retval.append("        </fields>" + Const.CR);

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
              BaseMessages.getString(
                  PKG, "FlattenerMeta.CheckResult.TransformReceivingInfoFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "FlattenerMeta.CheckResult.NoInputReceivedFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    }
  }
}
