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

package org.apache.hop.pipeline.transforms.stringcut;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
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

@Transform(
    id = "StringCut",
    image = "stringcut.svg",
    name = "i18n::BaseTransform.TypeLongDesc.StringCut",
    description = "i18n::BaseTransform.TypeTooltipDesc.StringCut",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/stringcut.html")
public class StringCutMeta extends BaseTransformMeta
    implements ITransformMeta<StringCut, StringCutData> {

  private static final Class<?> PKG = StringCutMeta.class; // For Translator

  private String[] fieldInStream;

  private String[] fieldOutStream;

  private String[] cutFrom;

  private String[] cutTo;

  public StringCutMeta() {
    super(); // allocate BaseTransformMeta
  }

  /** @return Returns the fieldInStream. */
  public String[] getFieldInStream() {
    return fieldInStream;
  }

  /** @param keyStream The fieldInStream to set. */
  public void setFieldInStream(String[] keyStream) {
    this.fieldInStream = keyStream;
  }

  /** @return Returns the fieldOutStream. */
  public String[] getFieldOutStream() {
    return fieldOutStream;
  }

  /** @param keyStream The fieldOutStream to set. */
  public void setFieldOutStream(String[] keyStream) {
    this.fieldOutStream = keyStream;
  }

  public String[] getCutFrom() {
    return cutFrom;
  }

  public void setCutFrom(String[] cutFrom) {
    this.cutFrom = cutFrom;
  }

  public String[] getCutTo() {
    return cutTo;
  }

  public void setCutTo(String[] cutTo) {
    this.cutTo = cutTo;
  }

  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode);
  }

  public void allocate(int nrkeys) {
    fieldInStream = new String[nrkeys];
    fieldOutStream = new String[nrkeys];
    cutTo = new String[nrkeys];
    cutFrom = new String[nrkeys];
  }

  public Object clone() {
    StringCutMeta retval = (StringCutMeta) super.clone();
    int nrkeys = fieldInStream.length;

    retval.allocate(nrkeys);
    System.arraycopy(fieldInStream, 0, retval.fieldInStream, 0, nrkeys);
    System.arraycopy(fieldOutStream, 0, retval.fieldOutStream, 0, nrkeys);
    System.arraycopy(cutTo, 0, retval.cutTo, 0, nrkeys);
    System.arraycopy(cutFrom, 0, retval.cutFrom, 0, nrkeys);

    return retval;
  }

  private void readData(Node transformNode) throws HopXmlException {
    try {
      int nrkeys;

      Node lookup = XmlHandler.getSubNode(transformNode, "fields");
      nrkeys = XmlHandler.countNodes(lookup, "field");

      allocate(nrkeys);

      for (int i = 0; i < nrkeys; i++) {
        Node fnode = XmlHandler.getSubNodeByNr(lookup, "field", i);
        fieldInStream[i] = Const.NVL(XmlHandler.getTagValue(fnode, "in_stream_name"), "");
        fieldOutStream[i] = Const.NVL(XmlHandler.getTagValue(fnode, "out_stream_name"), "");
        cutFrom[i] = Const.NVL(XmlHandler.getTagValue(fnode, "cut_from"), "");
        cutTo[i] = Const.NVL(XmlHandler.getTagValue(fnode, "cut_to"), "");
      }
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "StringCutMeta.Exception.UnableToReadTransformMetaFromXML"),
          e);
    }
  }

  public void setDefault() {
    fieldInStream = null;
    fieldOutStream = null;

    int nrkeys = 0;

    allocate(nrkeys);
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder(500);

    retval.append("    <fields>").append(Const.CR);

    for (int i = 0; i < fieldInStream.length; i++) {
      retval.append("      <field>").append(Const.CR);
      retval.append("        ").append(XmlHandler.addTagValue("in_stream_name", fieldInStream[i]));
      retval
          .append("        ")
          .append(XmlHandler.addTagValue("out_stream_name", fieldOutStream[i]));
      retval.append("        ").append(XmlHandler.addTagValue("cut_from", cutFrom[i]));
      retval.append("        ").append(XmlHandler.addTagValue("cut_to", cutTo[i]));
      retval.append("      </field>").append(Const.CR);
    }

    retval.append("    </fields>").append(Const.CR);

    return retval.toString();
  }

  public void getFields(
      IRowMeta inputRowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    for (int i = 0; i < fieldOutStream.length; i++) {
      IValueMeta v;
      if (!Utils.isEmpty(fieldOutStream[i])) {
        v = new ValueMetaString(variables.resolve(fieldOutStream[i]));
        v.setLength(100, -1);
        v.setOrigin(name);
        inputRowMeta.addValueMeta(v);
      } else {
        v = inputRowMeta.searchValueMeta(fieldInStream[i]);
        if (v == null) {
          continue;
        }
        v.setStorageType(IValueMeta.STORAGE_TYPE_NORMAL);
      }
    }
  }

  public void check(
      List<ICheckResult> remarks,
      PipelineMeta pipelineMeta,
      TransformMeta transforminfo,
      IRowMeta prev,
      String[] input,
      String[] output,
      IRowMeta info,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {

    CheckResult cr;
    String errorMessage = "";
    boolean first = true;
    boolean errorFound = false;

    if (prev == null) {
      errorMessage +=
          BaseMessages.getString(PKG, "StringCutMeta.CheckResult.NoInputReceived") + Const.CR;
      cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, errorMessage, transforminfo);
      remarks.add(cr);
    } else {

      for (int i = 0; i < fieldInStream.length; i++) {
        String field = fieldInStream[i];

        IValueMeta v = prev.searchValueMeta(field);
        if (v == null) {
          if (first) {
            first = false;
            errorMessage +=
                BaseMessages.getString(PKG, "StringCutMeta.CheckResult.MissingInStreamFields")
                    + Const.CR;
          }
          errorFound = true;
          errorMessage += "\t\t" + field + Const.CR;
        }
      }
      if (errorFound) {
        cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, errorMessage, transforminfo);
      } else {
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "StringCutMeta.CheckResult.FoundInStreamFields"),
                transforminfo);
      }
      remarks.add(cr);

      // Check whether all are strings
      first = true;
      errorFound = false;
      for (int i = 0; i < fieldInStream.length; i++) {
        String field = fieldInStream[i];

        IValueMeta v = prev.searchValueMeta(field);
        if (v != null) {
          if (v.getType() != IValueMeta.TYPE_STRING) {
            if (first) {
              first = false;
              errorMessage +=
                  BaseMessages.getString(
                          PKG, "StringCutMeta.CheckResult.OperationOnNonStringFields")
                      + Const.CR;
            }
            errorFound = true;
            errorMessage += "\t\t" + field + Const.CR;
          }
        }
      }
      if (errorFound) {
        cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, errorMessage, transforminfo);
      } else {
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(
                    PKG, "StringCutMeta.CheckResult.AllOperationsOnStringFields"),
                transforminfo);
      }
      remarks.add(cr);

      if (fieldInStream.length > 0) {
        for (int idx = 0; idx < fieldInStream.length; idx++) {
          if (Utils.isEmpty(fieldInStream[idx])) {
            cr =
                new CheckResult(
                    CheckResult.TYPE_RESULT_ERROR,
                    BaseMessages.getString(
                        PKG,
                        "StringCutMeta.CheckResult.InStreamFieldMissing",
                        new Integer(idx + 1).toString()),
                    transforminfo);
            remarks.add(cr);
          }
        }
      }
    }
  }

  public StringCut createTransform(
      TransformMeta transformMeta,
      StringCutData data,
      int cnr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new StringCut(transformMeta, this, data, cnr, pipelineMeta, pipeline);
  }

  public StringCutData getTransformData() {
    return new StringCutData();
  }

  public boolean supportsErrorHandling() {
    return true;
  }
}
