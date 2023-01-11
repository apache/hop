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

package org.apache.hop.pipeline.transforms.jsoninput;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.pipeline.transforms.file.BaseFileField;

/** Describes a JsonPath field. */
public class JsonInputField extends BaseFileField implements Cloneable {

  @HopMetadataProperty(
      key = "path",
      injectionKey = "FIELD_PATH",
      injectionKeyDescription = "JsonInput.Injection.FIELD_PATH")
  private String path;

  public JsonInputField(String fieldname) {
    super();
    setName(fieldname);
  }

  public JsonInputField() {
    this("");
  }

  public String getXml() {
    StringBuffer retval = new StringBuffer(400);

    retval.append("      <field>").append(Const.CR);
    retval.append("        ").append(XmlHandler.addTagValue("name", getName()));
    retval.append("        ").append(XmlHandler.addTagValue("path", getPath()));
    retval.append("        ").append(XmlHandler.addTagValue("type", getTypeDesc()));
    retval.append("        ").append(XmlHandler.addTagValue("format", getFormat()));
    retval.append("        ").append(XmlHandler.addTagValue("currency", getCurrencySymbol()));
    retval.append("        ").append(XmlHandler.addTagValue("decimal", getDecimalSymbol()));
    retval.append("        ").append(XmlHandler.addTagValue("group", getGroupSymbol()));
    retval.append("        ").append(XmlHandler.addTagValue("length", getLength()));
    retval.append("        ").append(XmlHandler.addTagValue("precision", getPrecision()));
    retval.append("        ").append(XmlHandler.addTagValue("trim_type", getTrimTypeCode()));
    retval.append("        ").append(XmlHandler.addTagValue("repeat", isRepeated()));

    retval.append("      </field>").append(Const.CR);

    return retval.toString();
  }

  public IValueMeta toValueMeta(String fieldOriginTransformName, IVariables vspace)
      throws HopPluginException {
    int type = getType();
    if (type == IValueMeta.TYPE_NONE) {
      type = IValueMeta.TYPE_STRING;
    }
    IValueMeta v =
        ValueMetaFactory.createValueMeta(
            vspace != null ? vspace.resolve(getName()) : getName(), type);
    v.setLength(getLength());
    v.setPrecision(getPrecision());
    v.setOrigin(fieldOriginTransformName);
    v.setConversionMask(getFormat());
    v.setDecimalSymbol(getDecimalSymbol());
    v.setGroupingSymbol(getGroupSymbol());
    v.setCurrencySymbol(getCurrencySymbol());
    v.setTrimType(getTrimType());
    return v;
  }

  @Override
  public JsonInputField clone() {
    JsonInputField retval = (JsonInputField) super.clone();
    return retval;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String value) {
    this.path = value;
  }
}
