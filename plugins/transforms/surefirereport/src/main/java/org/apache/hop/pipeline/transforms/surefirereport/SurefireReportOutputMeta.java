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

package org.apache.hop.pipeline.transforms.surefirereport;

import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "SurefireReportOutput",
    image = "surefirereport.svg",
    name = "i18n::SurefireReportOutput.Name",
    description = "i18n::SurefireReportOutput.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Output",
    keywords = "i18n::SurefireReportOutputMeta.keyword",
    documentationUrl = "/pipeline/transforms/surefirereport.html")
@Getter
@Setter
public class SurefireReportOutputMeta
    extends BaseTransformMeta<SurefireReportOutput, SurefireReportOutputData> {
  private static final Class<?> PKG = SurefireReportOutputMeta.class;

  @HopMetadataProperty(
      key = "filename",
      injectionKeyDescription = "SurefireReportOutput.Injection.Filename")
  private String filename;

  @HopMetadataProperty(
      key = "suite_name",
      injectionKeyDescription = "SurefireReportOutput.Injection.SuiteName")
  private String suiteName;

  @HopMetadataProperty(
      key = "create_parent_folder",
      injectionKeyDescription = "SurefireReportOutput.Injection.CreateParentFolder")
  private boolean createParentFolder;

  @HopMetadataProperty(
      key = "test_name_field",
      injectionKeyDescription = "SurefireReportOutput.Injection.TestNameField")
  private String testNameField;

  @HopMetadataProperty(
      key = "duration_field",
      injectionKeyDescription = "SurefireReportOutput.Injection.DurationField")
  private String durationField;

  @HopMetadataProperty(
      key = "duration_in_milliseconds",
      injectionKeyDescription = "SurefireReportOutput.Injection.DurationInMilliseconds")
  private boolean durationInMilliseconds;

  @HopMetadataProperty(
      key = "result_field",
      injectionKeyDescription = "SurefireReportOutput.Injection.ResultField")
  private String resultField;

  @HopMetadataProperty(
      key = "system_out_field",
      injectionKeyDescription = "SurefireReportOutput.Injection.SystemOutField")
  private String systemOutField;

  @HopMetadataProperty(
      key = "system_err_field",
      injectionKeyDescription = "SurefireReportOutput.Injection.SystemErrField")
  private String systemErrField;

  @HopMetadataProperty(
      key = "failure_message_field",
      injectionKeyDescription = "SurefireReportOutput.Injection.FailureMessageField")
  private String failureMessageField;

  @HopMetadataProperty(
      key = "failure_type_field",
      injectionKeyDescription = "SurefireReportOutput.Injection.FailureTypeField")
  private String failureTypeField;

  /** When true, the transform fails the pipeline if any test case failed or errored. */
  @HopMetadataProperty(
      key = "fail_on_test_failure",
      injectionKeyDescription = "SurefireReportOutput.Injection.FailOnTestFailure")
  private boolean failOnTestFailure;

  public SurefireReportOutputMeta() {
    super();
  }

  @Override
  public Object clone() {
    return super.clone();
  }

  @Override
  public void setDefault() {
    filename = "${IT_SUREFIRE_DIR}/surefile_${PROJECT_NAME}.xml";
    suiteName = "${PROJECT_NAME}";
    createParentFolder = true;
    testNameField = "testName";
    durationField = "ExecutionTime";
    durationInMilliseconds = true;
    resultField = "ExecutionResult";
    systemOutField = "ExecutionLogText";
    systemErrField = "";
    failureMessageField = "";
    failureTypeField = "";
    failOnTestFailure = true;
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
    if (Utils.isEmpty(filename)) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "SurefireReportOutputMeta.CheckResult.FilenameMissing"),
              transformMeta));
    } else {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "SurefireReportOutputMeta.CheckResult.FilenameOK"),
              transformMeta));
    }

    if (Utils.isEmpty(testNameField)) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "SurefireReportOutputMeta.CheckResult.TestNameFieldMissing"),
              transformMeta));
    } else {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "SurefireReportOutputMeta.CheckResult.TestNameFieldOK"),
              transformMeta));
    }

    if (input.length > 0) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "SurefireReportOutputMeta.CheckResult.ReceivingInfoFromOtherTransforms"),
              transformMeta));
    } else {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "SurefireReportOutputMeta.CheckResult.NoInputReceived"),
              transformMeta));
    }
  }
}
