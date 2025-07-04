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

import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Getter
@Setter
@Transform(
    id = "GoogleSheetsOutput",
    image = "google-sheets-output.svg",
    name = "i18n::GoogleSheetsOutput.transform.Name",
    description = "i18n::GoogleSheetsOutput.transform.Name",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Output",
    documentationUrl = "/pipeline/transforms/google-sheets-output.html")
public class GoogleSheetsOutputMeta
    extends BaseTransformMeta<GoogleSheetsOutput, GoogleSheetsOutputData> {

  public GoogleSheetsOutputMeta() {
    super();
    create = true;
  }

  @HopMetadataProperty(key = "jsonCredentialPath", injectionGroupKey = "SHEET")
  private String jsonCredentialPath;

  @HopMetadataProperty(key = "spreadsheetKey", injectionGroupKey = "SHEET")
  private String spreadsheetKey;

  @HopMetadataProperty(key = "worksheetId", injectionGroupKey = "SHEET")
  private String worksheetId;

  @HopMetadataProperty(key = "SHAREEMAIL", injectionGroupKey = "SHEET")
  private String shareEmail;

  @HopMetadataProperty(key = "SHAREDOMAIN", injectionGroupKey = "SHEET")
  private String shareDomain;

  @HopMetadataProperty(key = "CREATE", injectionGroupKey = "SHEET")
  private boolean create;

  @HopMetadataProperty(key = "APPEND", injectionGroupKey = "SHEET")
  private boolean append;

  @HopMetadataProperty(key = "timeout", injectionGroupKey = "SHEET")
  private String timeout;

  @HopMetadataProperty(key = "impersonation", injectionGroupKey = "SHEET")
  private String impersonation;

  @HopMetadataProperty(key = "appName", injectionGroupKey = "SHEET")
  private String appName;

  @HopMetadataProperty(key = "replace_sheet", injectionGroupKey = "SHEET")
  private boolean replaceSheet;

  @Override
  public void setDefault() {
    this.jsonCredentialPath = "" + "client_secret.json";
    this.spreadsheetKey = "";
    this.worksheetId = "";
    this.shareDomain = "";
    this.shareEmail = "";
    this.create = true;
    this.append = false;
    this.replaceSheet = false;
    this.impersonation = "";
    this.appName = "";
    this.timeout = "5";
  }

  @Override
  public Object clone() {
    GoogleSheetsOutputMeta retval = (GoogleSheetsOutputMeta) super.clone();
    retval.setJsonCredentialPath(this.jsonCredentialPath);
    retval.setSpreadsheetKey(this.spreadsheetKey);
    retval.setWorksheetId(this.worksheetId);
    retval.setCreate(this.create);
    retval.setAppend(this.append);
    retval.setShareEmail(this.shareEmail);
    retval.setShareDomain(this.shareDomain);
    return retval;
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
