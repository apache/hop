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

package org.apache.hop.pipeline.transforms.rowsfromresult;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopPluginException;
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
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

import java.util.List;

/*
 * Created on 02-jun-2003
 *
 */

@Transform(
    id = "RowsFromResult",
    image = "rowsfromresult.svg",
    name = "i18n::BaseTransform.TypeLongDesc.RowsFromResult",
    description = "i18n::BaseTransform.TypeTooltipDesc.RowsFromResult",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Workflow",
    documentationUrl =
        "https://hop.apache.org/manual/latest/plugins/transforms/rowsfromresult.html")
public class RowsFromResultMeta extends BaseTransformMeta
    implements ITransformMeta<RowsFromResult, RowsFromResultData> {
  private static final Class<?> PKG = RowsFromResult.class; // For Translator

  private String[] fieldname;
  private int[] type;
  private int[] length;
  private int[] precision;

  /** @return Returns the length. */
  public int[] getLength() {
    return length;
  }

  /** @param length The length to set. */
  public void setLength(int[] length) {
    this.length = length;
  }

  /** @return Returns the name. */
  public String[] getFieldname() {
    return fieldname;
  }

  /** @param name The name to set. */
  public void setFieldname(String[] name) {
    this.fieldname = name;
  }

  /** @return Returns the precision. */
  public int[] getPrecision() {
    return precision;
  }

  /** @param precision The precision to set. */
  public void setPrecision(int[] precision) {
    this.precision = precision;
  }

  /** @return Returns the type. */
  public int[] getType() {
    return type;
  }

  /** @param type The type to set. */
  public void setType(int[] type) {
    this.type = type;
  }

  public RowsFromResultMeta() {
    super(); // allocate BaseTransformMeta
  }

  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode);
  }

  public Object clone() {
    RowsFromResultMeta retval = (RowsFromResultMeta) super.clone();
    int nrFields = fieldname.length;
    retval.allocate(nrFields);
    System.arraycopy(fieldname, 0, retval.fieldname, 0, nrFields);
    System.arraycopy(type, 0, retval.type, 0, nrFields);
    System.arraycopy(length, 0, retval.length, 0, nrFields);
    System.arraycopy(precision, 0, retval.precision, 0, nrFields);
    return retval;
  }

  @Override
  public ITransform createTransform(
      TransformMeta transformMeta,
      RowsFromResultData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new RowsFromResult(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  public void allocate(int nrFields) {
    fieldname = new String[nrFields];
    type = new int[nrFields];
    length = new int[nrFields];
    precision = new int[nrFields];
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder();
    retval.append("    <fields>");
    for (int i = 0; i < fieldname.length; i++) {
      retval.append("      <field>");
      retval.append("        " + XmlHandler.addTagValue("name", fieldname[i]));
      retval.append(
          "        " + XmlHandler.addTagValue("type", ValueMetaFactory.getValueMetaName(type[i])));
      retval.append("        " + XmlHandler.addTagValue("length", length[i]));
      retval.append("        " + XmlHandler.addTagValue("precision", precision[i]));
      retval.append("        </field>");
    }
    retval.append("      </fields>");

    return retval.toString();
  }

  private void readData(Node transformNode) {
    Node fields = XmlHandler.getSubNode(transformNode, "fields");
    int nrFields = XmlHandler.countNodes(fields, "field");

    allocate(nrFields);

    for (int i = 0; i < nrFields; i++) {
      Node line = XmlHandler.getSubNodeByNr(fields, "field", i);
      fieldname[i] = XmlHandler.getTagValue(line, "name");
      type[i] = ValueMetaFactory.getIdForValueMeta(XmlHandler.getTagValue(line, "type"));
      length[i] = Const.toInt(XmlHandler.getTagValue(line, "length"), -2);
      precision[i] = Const.toInt(XmlHandler.getTagValue(line, "precision"), -2);
    }
  }

  public void setDefault() {
    allocate(0);
  }

  public void getFields(
      IRowMeta r,
      String origin,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    for (int i = 0; i < this.fieldname.length; i++) {
      IValueMeta v;
      try {
        v = ValueMetaFactory.createValueMeta(fieldname[i], type[i], length[i], precision[i]);
        v.setOrigin(origin);
        r.addValueMeta(v);
      } catch (HopPluginException e) {
        throw new HopTransformException(e);
      }
    }
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
    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      CheckResult cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG,
                  "RowsFromResultMeta.CheckResult.TransformExpectingNoReadingInfoFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      CheckResult cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "RowsFromResultMeta.CheckResult.NoInputReceivedError"),
              transformMeta);
      remarks.add(cr);
    }
  }

  public RowsFromResultData getTransformData() {
    return new RowsFromResultData();
  }
}
