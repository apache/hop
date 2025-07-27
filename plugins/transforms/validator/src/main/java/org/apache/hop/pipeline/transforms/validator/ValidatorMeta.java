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
 *
 */

package org.apache.hop.pipeline.transforms.validator;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformIOMeta;
import org.apache.hop.pipeline.transform.TransformIOMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.stream.IStream;
import org.apache.hop.pipeline.transform.stream.Stream;
import org.apache.hop.pipeline.transform.stream.StreamIcon;

@Transform(
    id = "Validator",
    name = "i18n::ValidatorDialog.Transform.Name",
    description = "i18n::ValidatorDialog.Transform.Description",
    keywords = "i18n::ValidatorDialog.Transform.KeyWords",
    image = "validator.svg",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    documentationUrl = "/pipeline/transforms/validator.html")
public class ValidatorMeta extends BaseTransformMeta<Validator, ValidatorData> {
  private static final Class<?> PKG = ValidatorMeta.class;

  @HopMetadataProperty(
      key = "validator_field",
      injectionGroupKey = "VALIDATIONS",
      injectionGroupDescription = "Validator.Injection.VALIDATIONS",
      injectionKey = "VALIDATION",
      injectionKeyDescription = "Validator.Injection.VALIDATION")
  private List<Validation> validations;

  @HopMetadataProperty(
      key = "validate_all",
      injectionKey = "VALIDATE_ALL",
      injectionKeyDescription = "Validator.Injection.VALIDATE_ALL")
  private boolean validatingAll;

  @HopMetadataProperty(
      key = "concat_errors",
      injectionKey = "CONCATENATE_ERRORS",
      injectionKeyDescription = "Validator.Injection.CONCATENATE_ERRORS")
  private boolean concatenatingErrors;

  @HopMetadataProperty(
      key = "concat_separator",
      injectionKey = "CONCATENATION_SEPARATOR",
      injectionKeyDescription = "Validator.Injection.CONCATENATION_SEPARATOR")
  private String concatenationSeparator;

  public ValidatorMeta() {
    this.validations = new ArrayList<>();
  }

  public ValidatorMeta(ValidatorMeta m) {
    this();
    m.validations.forEach(v -> this.validations.add(new Validation(v)));
    this.validatingAll = m.validatingAll;
    this.concatenatingErrors = m.concatenatingErrors;
    this.concatenationSeparator = m.concatenationSeparator;
  }

  @Override
  public ValidatorMeta clone() {
    return new ValidatorMeta(this);
  }

  @Override
  public void check(
      List remarks,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      IRowMeta prev,
      String[] input,
      String[] output,
      IRowMeta info,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    CheckResult cr;
    if (prev == null || prev.isEmpty()) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_WARNING,
              BaseMessages.getString(PKG, "ValidatorMeta.CheckResult.ExpectedInputError"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "ValidatorMeta.CheckResult.FieldsReceived", "" + prev.size()),
              transformMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this step!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "ValidatorMeta.CheckResult.ExpectedInputOk"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "ValidatorMeta.CheckResult.ExpectedInputError"),
              transformMeta);
      remarks.add(cr);
    }
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

  @Override
  public ITransformIOMeta getTransformIOMeta() {
    ITransformIOMeta ioMeta = super.getTransformIOMeta(false);
    if (ioMeta != null) {
      return ioMeta;
    }

    ioMeta = new TransformIOMeta(true, true, false, false, true, false);
    for (Validation validation : validations) {
      IStream stream =
          new Stream(
              IStream.StreamType.INFO,
              validation.getSourcingTransform(),
              BaseMessages.getString(
                  PKG,
                  "ValidatorMeta.InfoStream.ValidationInput.Description",
                  Const.NVL(validation.getName(), "")),
              StreamIcon.INFO,
              validation.getSourcingTransformName());
      ioMeta.addStream(stream);
    }
    return ioMeta;
  }

  @Override
  public void searchInfoAndTargetTransforms(List transforms) {
    for (Validation validation : validations) {
      TransformMeta transformMeta =
          TransformMeta.findTransform(transforms, validation.getSourcingTransformName());
      validation.setSourcingTransform(transformMeta);
    }
    resetTransformIoMeta();
  }

  /** The standard new validation stream */
  private static IStream newValidation =
      new Stream(
          IStream.StreamType.INFO,
          null,
          BaseMessages.getString(PKG, "ValidatorMeta.NewValidation.Description"),
          StreamIcon.INFO,
          null);

  @Override
  public void handleStreamSelection(IStream stream) {
    // A hack to prevent us from losing information in the UI because
    // of the resetStepIoMeta() call at the end of this method.
    //
    List<IStream> streams = getTransformIOMeta().getInfoStreams();
    for (int i = 0; i < validations.size(); i++) {
      validations.get(i).setSourcingTransform(streams.get(i).getTransformMeta());
    }

    if (stream == newValidation) {

      // Add the info.
      //
      Validation validation = new Validation();
      validation.setName(stream.getTransformName());
      validation.setSourcingTransform(stream.getTransformMeta());
      validation.setSourcingValues(true);
      validations.add(validation);
    }

    // Force the IO to be recreated when it is next needed.
    resetTransformIoMeta();
  }

  /**
   * Gets validations
   *
   * @return value of validations
   */
  public List<Validation> getValidations() {
    return validations;
  }

  /**
   * Sets validations
   *
   * @param validations value of validations
   */
  public void setValidations(List<Validation> validations) {
    this.validations = validations;
  }

  /**
   * Gets validatingAll
   *
   * @return value of validatingAll
   */
  public boolean isValidatingAll() {
    return validatingAll;
  }

  /**
   * Sets validatingAll
   *
   * @param validatingAll value of validatingAll
   */
  public void setValidatingAll(boolean validatingAll) {
    this.validatingAll = validatingAll;
  }

  /**
   * Gets concatenatingErrors
   *
   * @return value of concatenatingErrors
   */
  public boolean isConcatenatingErrors() {
    return concatenatingErrors;
  }

  /**
   * Sets concatenatingErrors
   *
   * @param concatenatingErrors value of concatenatingErrors
   */
  public void setConcatenatingErrors(boolean concatenatingErrors) {
    this.concatenatingErrors = concatenatingErrors;
  }

  /**
   * Gets concatenationSeparator
   *
   * @return value of concatenationSeparator
   */
  public String getConcatenationSeparator() {
    return concatenationSeparator;
  }

  /**
   * Sets concatenationSeparator
   *
   * @param concatenationSeparator value of concatenationSeparator
   */
  public void setConcatenationSeparator(String concatenationSeparator) {
    this.concatenationSeparator = concatenationSeparator;
  }

  /**
   * Gets newValidation
   *
   * @return value of newValidation
   */
  public static IStream getNewValidation() {
    return newValidation;
  }

  /**
   * Sets newValidation
   *
   * @param newValidation value of newValidation
   */
  public static void setNewValidation(IStream newValidation) {
    ValidatorMeta.newValidation = newValidation;
  }
}
