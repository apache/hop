/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.standardizephonenumber;

import com.google.i18n.phonenumbers.PhoneNumberUtil;
import com.google.i18n.phonenumbers.PhoneNumberUtil.PhoneNumberFormat;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

@Transform(
    id = "StandardizePhoneNumber",
    image = "standardizephonenumber.svg",
    name = "i18n::StandardizePhoneNumber.Name",
    description = "i18n::StandardizePhoneNumber.Description",
    categoryDescription =
        "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.DataQuality",
    documentationUrl = "/pipeline/transforms/standardizephonenumber.html")
public class StandardizePhoneNumberMeta extends BaseTransformMeta<StandardizePhoneNumber, StandardizePhoneNumberData> implements Serializable {

  private static final Class<?> PKG = StandardizePhoneNumberMeta.class; // For Translator

  private static final Set<PhoneNumberFormat> SUPPORTED_FORMATS =
      EnumSet.of(
          PhoneNumberFormat.E164,
          PhoneNumberFormat.INTERNATIONAL,
          PhoneNumberFormat.NATIONAL,
          PhoneNumberFormat.RFC3966);

  /** The phone number to standardize */
  @HopMetadataProperty(
      key = "field",
      groupKey = "fields",
      injectionGroupDescription = "StandardizePhoneNumber.Injection.Fields",
      injectionKeyDescription = "StandardizePhoneNumber.Injection.Field")
  private List<StandardizePhoneField> fields = new ArrayList<>();

  public StandardizePhoneNumberMeta() {
    super();
  }

  public StandardizePhoneNumberMeta(StandardizePhoneNumberMeta meta) {
    super();

    for (StandardizePhoneField field : meta.getFields()) {
      fields.add(new StandardizePhoneField(field));
    }
  }

  @Override
  public Object clone() {
    return new StandardizePhoneNumberMeta(this);
  }

  @Override
  public void setDefault() {}

  @Override
  public void getFields(
      IRowMeta inputRowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    try {
      // add the extra fields if specified
      for (StandardizePhoneField standardize : this.getFields()) {

        // add the output fields if specified
        int index = inputRowMeta.indexOfValue(standardize.getInputField());
        IValueMeta valueMeta = inputRowMeta.getValueMeta(index);
        if (!Utils.isEmpty(standardize.getOutputField())
            && !standardize.getOutputField().equals(standardize.getInputField())) {
          // created output field only if name changed
          valueMeta =
              ValueMetaFactory.createValueMeta(
                  standardize.getOutputField(), IValueMeta.TYPE_STRING);

          inputRowMeta.addValueMeta(valueMeta);
        }
        valueMeta.setOrigin(name);

        // add result phone number type
        if (!Utils.isEmpty(standardize.getNumberTypeField())) {
          valueMeta =
              ValueMetaFactory.createValueMeta(
                  standardize.getNumberTypeField(), IValueMeta.TYPE_STRING);
          valueMeta.setOrigin(name);
          inputRowMeta.addValueMeta(valueMeta);
        }

        // add result is valid number
        if (!Utils.isEmpty(standardize.getIsValidNumberField())) {
          valueMeta =
              ValueMetaFactory.createValueMeta(
                  standardize.getIsValidNumberField(), IValueMeta.TYPE_BOOLEAN);
          valueMeta.setOrigin(name);
          inputRowMeta.addValueMeta(valueMeta);
        }
      }
    } catch (Exception e) {
      throw new HopTransformException(e);
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

    // See if we have fields from previous steps
    if (prev == null || prev.size() == 0) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_WARNING,
              BaseMessages.getString(
                  PKG,
                  "StandardizePhoneNumberMeta.CheckResult.NotReceivingFieldsFromPreviousTransforms"),
              transformMeta));
    } else {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG,
                  "StandardizePhoneNumberMeta.CheckResult.ReceivingFieldsFromPreviousTransforms",
                  prev.size()), // $NON-NLS-1$
              transformMeta));
    }

    // See if there are input streams leading to this step!
    if (input.length > 0) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "StandardizePhoneNumberMeta.CheckResult.ReceivingInfoFromOtherTransforms"),
              transformMeta));

      // Check only if input fields
      for (StandardizePhoneField standardize : fields) {

        // See if there are missing input streams
        IValueMeta valueMeta = null;
        if (prev != null) {
          valueMeta = prev.searchValueMeta(standardize.getInputField());
        }
        if (valueMeta == null) {
          String message =
              BaseMessages.getString(
                  PKG,
                  "StandardizePhoneNumberMeta.CheckResult.MissingInputField",
                  Const.NVL(standardize.getInputField(), standardize.getOutputField()));
          remarks.add(new CheckResult(ICheckResult.TYPE_RESULT_ERROR, message, transformMeta));
        }

        // See if there are missing input streams
        if (prev != null) {
          valueMeta = prev.searchValueMeta(standardize.getCountryField());
        }
        if (valueMeta == null) {
          String message =
              BaseMessages.getString(
                  PKG,
                  "StandardizePhoneNumberMeta.CheckResult.MissingCountryField",
                  standardize.getCountryField());
          remarks.add(new CheckResult(ICheckResult.TYPE_RESULT_ERROR, message, transformMeta));
        }
      }

    } else {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG,
                  "StandardizePhoneNumberMeta.CheckResult.NotReceivingInfoFromOtherTransforms"),
              transformMeta));
    }
  }

  public String[] getSupportedFormats() {

    ArrayList<String> result = new ArrayList<>();
    for (PhoneNumberFormat format : SUPPORTED_FORMATS) {
      result.add(format.name());
    }

    return result.toArray(new String[result.size()]);
  }

  public String[] getSupportedCountries() {

    PhoneNumberUtil phoneUtil = PhoneNumberUtil.getInstance();

    ArrayList<String> result = new ArrayList<>();
    for (String region : phoneUtil.getSupportedRegions()) {
      result.add(region);
    }

    return Const.sortStrings(result.toArray(new String[0]));
  }

  public List<StandardizePhoneField> getFields() {
    return this.fields;
  }

  public void setFields(final List<StandardizePhoneField> standardizes) {
    this.fields = standardizes;
  }
}
