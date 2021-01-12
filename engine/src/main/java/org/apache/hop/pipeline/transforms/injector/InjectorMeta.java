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

package org.apache.hop.pipeline.transforms.injector;

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

// TODO: check conversion of types from strings to numbers and back.
//       As compared in the old version.

/**
 * Metadata class to allow a java program to inject rows of data into a pipeline. This transform can
 * be used as a starting point in such a "headless" pipeline.
 *
 * @since 22-jun-2006
 */
@Transform(
    id = "Injector",
    image = "ui/images/injector.svg",
    name = "i18n:org.apache.hop.pipeline.transform:BaseTransform.TypeLongDesc.Injector",
    description = "i18n:org.apache.hop.pipeline.transform:BaseTransform.TypeTooltipDesc.Injector",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Inline",
    keywords = "")
public class InjectorMeta extends BaseTransformMeta
    implements ITransformMeta<Injector, InjectorData> {

  private static final Class<?> PKG = InjectorMeta.class; // For Translator

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

  /** @param fieldname The name to set. */
  public void setFieldname(String[] fieldname) {
    this.fieldname = fieldname;
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

  public InjectorMeta() {
    super(); // allocate BaseTransformMeta
    allocate(0);
  }

  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode);
  }

  public Object clone() {
    Object retval = super.clone();
    return retval;
  }

  public void allocate(int nrFields) {
    fieldname = new String[nrFields];
    type = new int[nrFields];
    length = new int[nrFields];
    precision = new int[nrFields];
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder(300);
    retval.append("    <fields>");
    for (int i = 0; i < fieldname.length; i++) {
      retval.append("      <field>");
      retval.append("        ").append(XmlHandler.addTagValue("name", fieldname[i]));
      retval
          .append("        ")
          .append(XmlHandler.addTagValue("type", ValueMetaFactory.getValueMetaName(type[i])));
      retval.append("        ").append(XmlHandler.addTagValue("length", length[i]));
      retval.append("        ").append(XmlHandler.addTagValue("precision", precision[i]));
      retval.append("      </field>");
    }
    retval.append("    </fields>");

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
      IRowMeta inputRowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    for (int i = 0; i < this.fieldname.length; i++) {
      IValueMeta v;
      try {
        v = ValueMetaFactory.createValueMeta(this.fieldname[i], type[i], length[i], precision[i]);
        inputRowMeta.addValueMeta(v);
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
                  "InjectorMeta.CheckResult.TransformExpectingNoReadingInfoFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      CheckResult cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "InjectorMeta.CheckResult.NoInputReceivedError"),
              transformMeta);
      remarks.add(cr);
    }
  }

  public ITransform createTransform(
      TransformMeta transformMeta,
      InjectorData data,
      int cnr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new Injector(transformMeta, this, data, cnr, pipelineMeta, pipeline);
  }

  public InjectorData getTransformData() {
    return new InjectorData();
  }
}
