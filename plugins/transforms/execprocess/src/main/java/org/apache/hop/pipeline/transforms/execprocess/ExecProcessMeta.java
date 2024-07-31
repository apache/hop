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

package org.apache.hop.pipeline.transforms.execprocess;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "ExecProcess",
    image = "execprocess.svg",
    name = "i18n::ExecProcess.Name",
    description = "i18n::ExecProcess.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Utility",
    keywords = "i18n::ExecProcessMeta.keyword",
    documentationUrl = "/pipeline/transforms/execprocess.html")
public class ExecProcessMeta extends BaseTransformMeta<ExecProcess, ExecProcessData> {
  private static final Class<?> PKG = ExecProcessMeta.class;

  /** dynamic process field name */
  @HopMetadataProperty(key = "processfield")
  private String processField;

  /** function result: new value name */
  @HopMetadataProperty(key = "resultfieldname")
  private String resultFieldName;

  /** function result: error fieldname */
  @HopMetadataProperty(key = "errorfieldname")
  private String errorFieldName;

  /** function result: exit value fieldname */
  @HopMetadataProperty(key = "exitvaluefieldname")
  private String exitValueFieldName;

  /** fail if the exit status is different from 0 */
  @HopMetadataProperty(key = "failwhennotsuccess")
  private boolean failWhenNotSuccess;

  /** Output Line Delimiter - defaults to empty string for backward compatibility */
  @HopMetadataProperty(key = "outputlinedelimiter")
  public String outputLineDelimiter = "";

  /** Whether arguments for the command are provided in input fields */
  @HopMetadataProperty(key = "argumentsInFields")
  private boolean argumentsInFields;

  /** The field names where arguments should be found */
  @HopMetadataProperty(groupKey = "argumentFields", key = "argumentField")
  private List<EPField> argumentFields;

  public ExecProcessMeta() {
    super();
    argumentFields = new ArrayList<>();
  }

  public ExecProcessMeta(ExecProcessMeta m) {
    this();
    this.processField = m.processField;
    this.resultFieldName = m.resultFieldName;
    this.errorFieldName = m.errorFieldName;
    this.exitValueFieldName = m.exitValueFieldName;
    this.failWhenNotSuccess = m.failWhenNotSuccess;
    this.outputLineDelimiter = m.outputLineDelimiter;
    this.argumentsInFields = m.argumentsInFields;
    m.argumentFields.forEach(f -> this.argumentFields.add(new EPField(f)));
  }

  @Override
  public ExecProcessMeta clone() {
    return new ExecProcessMeta(this);
  }

  @Override
  public void setDefault() {
    resultFieldName = "Result output";
    errorFieldName = "Error output";
    exitValueFieldName = "Exit value";
    failWhenNotSuccess = false;
  }

  @Override
  public void getFields(
      IRowMeta inputRowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    // Output fields (String)
    String realOutputFieldName = variables.resolve(resultFieldName);
    if (!Utils.isEmpty(realOutputFieldName)) {
      IValueMeta v = new ValueMetaString(realOutputFieldName);
      v.setLength(100, -1);
      v.setOrigin(name);
      inputRowMeta.addValueMeta(v);
    }
    String realErrorFieldName = variables.resolve(errorFieldName);
    if (!Utils.isEmpty(realErrorFieldName)) {
      IValueMeta v = new ValueMetaString(realErrorFieldName);
      v.setLength(100, -1);
      v.setOrigin(name);
      inputRowMeta.addValueMeta(v);
    }
    String realExitValueFieldName = variables.resolve(exitValueFieldName);
    if (!Utils.isEmpty(realExitValueFieldName)) {
      IValueMeta v = new ValueMetaInteger(realExitValueFieldName);
      v.setLength(IValueMeta.DEFAULT_INTEGER_LENGTH, 0);
      v.setOrigin(name);
      inputRowMeta.addValueMeta(v);
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
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    if (Utils.isEmpty(resultFieldName)) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "ExecProcessMeta.CheckResult.ResultFieldMissing"),
              transformMeta));
    } else {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "ExecProcessMeta.CheckResult.ResultFieldOK"),
              transformMeta));
    }

    if (Utils.isEmpty(processField)) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "ExecProcessMeta.CheckResult.ProcessFieldMissing"),
              transformMeta));
    } else {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "ExecProcessMeta.CheckResult.ProcessFieldOK"),
              transformMeta));
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "ExecProcessMeta.CheckResult.ReceivingInfoFromOtherTransforms"),
              transformMeta));
    } else {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "ExecProcessMeta.CheckResult.NoInputReceived"),
              transformMeta));
    }
  }

  public static final class EPField {
    @HopMetadataProperty(key = "argumentFieldName")
    private String name;

    public EPField() {}

    public EPField(EPField f) {
      this.name = f.name;
    }

    /**
     * Gets name
     *
     * @return value of name
     */
    public String getName() {
      return name;
    }

    /**
     * Sets name
     *
     * @param name value of name
     */
    public void setName(String name) {
      this.name = name;
    }
  }

  @Override
  public boolean supportsErrorHandling() {
    return failWhenNotSuccess;
  }

  public void setOutputLineDelimiter(String value) {
    this.outputLineDelimiter = value;
  }

  public String getOutputLineDelimiter() {
    return outputLineDelimiter;
  }

  public boolean isArgumentsInFields() {
    return argumentsInFields;
  }

  public void setArgumentsInFields(boolean argumentsInFields) {
    this.argumentsInFields = argumentsInFields;
  }

  public List<EPField> getArgumentFields() {
    return argumentFields;
  }

  public void setArgumentFields(List<EPField> argumentFields) {
    this.argumentFields = argumentFields;
  }

  /**
   * @return Returns the processField.
   */
  public String getProcessField() {
    return processField;
  }

  /**
   * @param processField The processField to set.
   */
  public void setProcessField(String processField) {
    this.processField = processField;
  }

  /**
   * @return Returns the resultName.
   */
  public String getResultFieldName() {
    return resultFieldName;
  }

  /**
   * @param errorFieldName The errorFieldName to set.
   */
  public void setResultFieldName(String errorFieldName) {
    this.resultFieldName = errorFieldName;
  }

  /**
   * @return Returns the errorFieldName.
   */
  public String getErrorFieldName() {
    return errorFieldName;
  }

  /**
   * @param errorFieldName The errorFieldName to set.
   */
  public void setErrorFieldName(String errorFieldName) {
    this.errorFieldName = errorFieldName;
  }

  /**
   * @return Returns the exitvaluefieldname.
   */
  public String getExitValueFieldName() {
    return exitValueFieldName;
  }

  /**
   * @param exitValueFieldName The exitValueFieldName to set.
   */
  public void setExitValueFieldName(String exitValueFieldName) {
    this.exitValueFieldName = exitValueFieldName;
  }

  /**
   * @return Returns the failWhenNotSuccess.
   */
  public boolean isFailWhenNotSuccess() {
    return failWhenNotSuccess;
  }

  /**
   * @param failWhenNotSuccess The failWhenNotSuccess to set.
   */
  public void setFailWhenNotSuccess(boolean failWhenNotSuccess) {
    this.failWhenNotSuccess = failWhenNotSuccess;
  }
}
