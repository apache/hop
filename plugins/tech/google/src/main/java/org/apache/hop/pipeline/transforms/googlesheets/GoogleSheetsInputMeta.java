/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hop.pipeline.transforms.googlesheets;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Getter
@Setter
@Transform(
    id = "GoogleSheetsInput",
    image = "google-sheets-input.svg",
    name = "i18n::GoogleSheetsInput.transform.Name",
    description = "i18n::GoogleSheetsInput.transform.Name",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    documentationUrl = "/pipeline/transforms/google-sheets-input.html")
public class GoogleSheetsInputMeta
    extends BaseTransformMeta<GoogleSheetsInput, GoogleSheetsInputData> {

  public GoogleSheetsInputMeta() {
    super();
    inputFields = new ArrayList<>();
  }

  @HopMetadataProperty(
      key = "jsonCrendentialPath",
      injectionGroupKey = "SHEET",
      isExcludedFromInjection = true)
  private String oldJsonCredentialPath;

  @HopMetadataProperty(key = "jsonCredentialPath", injectionGroupKey = "SHEET")
  private String jsonCredentialPath;

  @HopMetadataProperty(key = "spreadsheetKey", injectionGroupKey = "SHEET")
  private String spreadsheetKey;

  @HopMetadataProperty(key = "worksheetId", injectionGroupKey = "SHEET")
  private String worksheetId;

  @HopMetadataProperty(key = "sampleFields", injectionGroupKey = "INPUT_Fields")
  private Integer sampleFields;

  @HopMetadataProperty(key = "timeout", injectionGroupKey = "SHEET")
  private String timeout;

  @HopMetadataProperty(key = "impersonation", injectionGroupKey = "SHEET")
  private String impersonation;

  @HopMetadataProperty(key = "appName", injectionGroupKey = "SHEET")
  private String appName;

  @HopMetadataProperty(groupKey = "fields", key = "field", injectionGroupKey = "FIELDS")
  private List<GoogleSheetsInputField> inputFields;

  @HopMetadataProperty private String proxyHost;

  @HopMetadataProperty private String proxyPort;

  @Override
  public void setDefault() {
    this.spreadsheetKey = "";
    this.worksheetId = "";
    this.jsonCredentialPath = "client_secret.json";
    this.timeout = "5";
    this.impersonation = "";
    this.sampleFields = 100;
  }

  public String getJsonCredentialPath() {
    if (getOldJsonCredentialPath() != null && this.jsonCredentialPath == null) {
      this.jsonCredentialPath = getOldJsonCredentialPath();
      setOldJsonCredentialPath(null);
    }
    return this.jsonCredentialPath == null ? "" : this.jsonCredentialPath;
  }

  /*
    public void setJsonCredentialPath(String jsonCredentialPath) {
      this.jsonCredentialPath = jsonCredentialPath;
    }

    public List<GoogleSheetsInputField> getInputFields() {
      return inputFields;
    }

    public void setInputFields(List<GoogleSheetsInputField> inputFields) {
      this.inputFields = inputFields;
    }

    public String getSpreadsheetKey() {
      return this.spreadsheetKey == null ? "" : this.spreadsheetKey;
    }

    public void setSpreadsheetKey(String key) {
      this.spreadsheetKey = key;
    }

    public String getWorksheetId() {
      return this.worksheetId == null ? "" : this.worksheetId;
    }

    public void setWorksheetId(String id) {
      this.worksheetId = id;
    }

    public int getSampleFields() {
      return this.sampleFields == null ? 100 : this.sampleFields;
    }

    public void setSampleFields(Integer sampleFields) {
      this.sampleFields = sampleFields;
    }

    public String getTimeout() {
      return timeout;
    }

    public void setTimeout(String timeout) {
      this.timeout = timeout;
    }

    public String getImpersonation() {
      return impersonation;
    }

    public void setImpersonation(String impersonation) {
      this.impersonation = impersonation;
    }

    public String getAppName() {
      return appName;
    }

    public void setAppName(String appName) {
      this.appName = appName;
    }

    public String getOldJsonCredentialPath() {
      return oldJsonCredentialPath;
    }

    public void setOldJsonCredentialPath(String oldJsonCredentialPath) {
      this.oldJsonCredentialPath = oldJsonCredentialPath;
    }
  */

  @Override
  public void getFields(
      IRowMeta rowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    try {
      rowMeta.clear(); // Start with a clean slate, eats the input
      for (GoogleSheetsInputField field : inputFields) {
        int type = field.getType();
        if (type == IValueMeta.TYPE_NONE) {
          type = IValueMeta.TYPE_STRING;
        }

        try {
          IValueMeta v = ValueMetaFactory.createValueMeta(field.getName(), type);

          v.setLength(field.getLength());
          v.setPrecision(field.getPrecision());
          v.setOrigin(name);
          v.setConversionMask(field.getFormat());
          v.setDecimalSymbol(field.getDecimalSymbol());
          v.setGroupingSymbol(field.getGroupSymbol());
          v.setCurrencySymbol(field.getCurrencySymbol());
          v.setTrimType(field.getTrimType());

          rowMeta.addValueMeta(v);
        } catch (Exception e) {
          throw new HopTransformException(e);
        }
      }
    } catch (Exception e) {

    }
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
      IVariables space,
      IHopMetadataProvider metadataProvider) {
    if (prev == null || prev.isEmpty()) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              "Not receiving any fields from previous transforms.",
              transformMeta));
    } else {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              String.format(
                  "Transform is connected to previous one, receiving %1$d fields", prev.size()),
              transformMeta));
    }

    if (input.length > 0) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              "Transform is receiving info from other transforms!",
              transformMeta));
    } else {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              "No input received from other transforms.",
              transformMeta));
    }
  }
}
