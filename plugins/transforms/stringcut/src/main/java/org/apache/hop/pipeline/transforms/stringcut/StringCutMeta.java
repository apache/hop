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
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;

@Transform(
    id = "StringCut",
    image = "stringcut.svg",
    name = "i18n::BaseTransform.TypeLongDesc.StringCut",
    description = "i18n::BaseTransform.TypeTooltipDesc.StringCut",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    documentationUrl = "https://hop.apache.org/manual/latest/pipeline/transforms/stringcut.html")
public class StringCutMeta extends BaseTransformMeta
    implements ITransformMeta<StringCut, StringCutData> {

  private static final Class<?> PKG = StringCutMeta.class; // For Translator

  @HopMetadataProperty(
          groupKey = "fields",
          key = "field",
          injectionGroupDescription = "StringCutMeta.Injection.Fields",
          injectionKeyDescription = "StringCutMeta.Injection.Field")
    private List<StringCutField> fields;

  public StringCutMeta() {
    super(); // allocate BaseTransformMeta
    fields = new ArrayList<>();
  }


  public StringCutMeta(StringCutMeta obj) {
    fields = new ArrayList<>();
    for (StringCutField field : obj.fields) {
      this.fields.add(new StringCutField(field));
    }
  }

  public Object clone() {
    return new StringCutMeta(this);
  }

  /**
   * Gets fields
   *
   * @return value of fields
   */
  public List<StringCutField> getFields() {
    return fields;
  }

  /** @param fields The fields to set */
  public void setFields(List<StringCutField> fields) {
    this.fields = fields;
  }

  public void getFields(
          IRowMeta inputRowMeta,
          String name,
          IRowMeta[] info,
          TransformMeta nextTransform,
          IVariables variables,
          IHopMetadataProvider metadataProvider)
          throws HopTransformException {
    for (int i = 0; i < fields.size(); i++) {
      IValueMeta v;
      String fieldOutStream = fields.get(i).getFieldOutStream();
      String fieldInStream = fields.get(i).getFieldInStream();
      if (!Utils.isEmpty(fieldOutStream)) {
        v = new ValueMetaString(variables.resolve(fieldOutStream));
        v.setLength(100, -1);
        v.setOrigin(name);
        inputRowMeta.addValueMeta(v);
      } else {
        v = inputRowMeta.searchValueMeta(fieldInStream);
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

      for (StringCutField scf: fields) {

        String field = scf.getFieldInStream();

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
      for (StringCutField scf: fields) {
        String field = scf.getFieldInStream();

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

      if (fields.size() > 0) {
        int idx = 0;
        for (StringCutField scf: fields) {
          if (Utils.isEmpty(scf.getFieldInStream())) {
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
          idx++;
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
