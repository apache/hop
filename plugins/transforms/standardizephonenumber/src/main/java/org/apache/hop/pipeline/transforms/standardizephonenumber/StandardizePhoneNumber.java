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

package org.apache.hop.pipeline.transforms.standardizephonenumber;

import com.google.i18n.phonenumbers.NumberParseException;
import com.google.i18n.phonenumbers.PhoneNumberUtil;
import com.google.i18n.phonenumbers.PhoneNumberUtil.PhoneNumberFormat;
import com.google.i18n.phonenumbers.Phonenumber.PhoneNumber;
import java.util.Arrays;
import java.util.Locale;
import java.util.Set;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

public class StandardizePhoneNumber
    extends BaseTransform<StandardizePhoneNumberMeta, StandardizePhoneNumberData> {
  private static final Class<?> PKG = StandardizePhoneNumber.class;

  static final String NUMBER_TYPE_ERROR = "ERROR";

  private PhoneNumberUtil phoneNumberService;
  private Set<String> supportedRegions;

  public StandardizePhoneNumber(
      TransformMeta transformMeta,
      StandardizePhoneNumberMeta meta,
      StandardizePhoneNumberData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {

    // get incoming row, getRow() potentially blocks waiting for more rows,
    // returns null if no more rows expected
    Object[] row = getRow();

    // if no more rows are expected, indicate step is finished and
    // processRow() should not be called again
    if (row == null) {
      setOutputDone();
      return false;
    }

    // the "first" flag is inherited from the base step implementation
    // it is used to guard some processing tasks, like figuring out field
    // indexes
    // in the row structure that only need to be done once
    if (first) {
      if (isDebug()) {
        logDebug(BaseMessages.getString(PKG, "StandardizePhoneNumber.Log.StartedProcessing"));
      }

      first = false;
      // clone the input row structure and place it in our data object
      data.outputRowMeta = getInputRowMeta().clone();
      // use meta.getFields() to change it, so it reflects the output row
      // structure
      meta.getFields(data.outputRowMeta, this.getTransformName(), null, null, this, null);
    }

    IRowMeta inputRowMeta = getInputRowMeta();

    // copies row into outputRowValues and pads extra null-default slots for
    // the output values
    Object[] outputRow = Arrays.copyOf(row, data.outputRowMeta.size());

    for (StandardizePhoneField standardize : meta.getFields()) {
      String inputField = resolve(standardize.getInputField());
      String outputField = resolve(standardize.getOutputField());
      String countryField = resolve(standardize.getCountryField());
      String numberTypeField = resolve(standardize.getNumberTypeField());
      String isValidNumberField = resolve(standardize.getIsValidNumberField());
      String numberFormat = resolve(standardize.getNumberFormat());

      String region = resolveRegion(standardize, countryField, inputRowMeta, row);

      int inputIndex = inputRowMeta.indexOfValue(inputField);

      // if input field not found
      if (inputIndex < 0) {
        this.logError(
            BaseMessages.getString(
                PKG, "StandardizePhoneNumber.Log.InputFieldNotFound", inputField));
        this.setErrors(1);
        return false;
      }

      int outputIndex = inputIndex;
      if (!Utils.isEmpty(outputField)) {
        int resolvedOutputIndex = data.outputRowMeta.indexOfValue(outputField);
        if (resolvedOutputIndex >= 0) {
          outputIndex = resolvedOutputIndex;
        }
      }

      String originalValue = inputRowMeta.getString(row, inputIndex);
      if (!Utils.isEmpty(originalValue)) {
        PhoneNumber phoneNumber = null;
        try {
          // Replace unsupported character with blank
          String value = originalValue.replace(',', ' ');

          PhoneNumberFormat format = getPhoneNumberFormat(numberFormat);
          phoneNumber = phoneNumberService.parse(value, region);
          outputRow[outputIndex] = phoneNumberService.format(phoneNumber, format);
        } catch (NumberParseException e) {
          outputRow[outputIndex] = originalValue;
          if (isRowLevel()) {
            logRowlevel(
                BaseMessages.getString(
                    PKG,
                    "StandardizePhoneNumber.Log.ProcessPhoneNumberError",
                    inputField,
                    originalValue));
          }
        }

        setNumberType(outputRow, numberTypeField, phoneNumber);
        setIsValid(outputRow, isValidNumberField, phoneNumber);
      } else {
        setNumberType(outputRow, numberTypeField, null);
        setIsValid(outputRow, isValidNumberField, null);
      }
    }

    // put the row to the output row stream
    putRow(data.outputRowMeta, outputRow);

    if (isRowLevel()) {
      logRowlevel(
          BaseMessages.getString(
              PKG, "StandardizePhoneNumber.Log.WroteRowToNextTransform", outputRow));
    }

    // log progress if it is time to to so
    if (checkFeedback(getLinesRead()) && isBasic()) {
      logBasic("Line nr " + getLinesRead());
    }

    // indicate that processRow() should be called again
    return true;
  }

  /**
   * Normalize a country / region code: trim whitespace and upper-case using {@link Locale#ROOT}.
   *
   * @param country raw country code, may be null
   * @return ISO alpha-2 region or {@code null} if empty
   */
  static String normalizeRegion(String country) {
    if (country == null) {
      return null;
    }
    String normalized = country.trim().toUpperCase(Locale.ROOT);
    return normalized.isEmpty() ? null : normalized;
  }

  private String resolveRegion(
      StandardizePhoneField standardize, String countryField, IRowMeta inputRowMeta, Object[] row)
      throws HopException {
    String region = normalizeRegion(resolve(standardize.getDefaultCountry()));
    if (Utils.isEmpty(countryField)) {
      return region;
    }

    int index = inputRowMeta.indexOfValue(countryField);

    // if country field not found
    if (index < 0) {
      String message =
          BaseMessages.getString(
              PKG, "StandardizePhoneNumber.Log.CountryFieldNotFound", countryField);
      logError(message);
      this.setErrors(1);
      throw new HopException(message);
    }

    String country = inputRowMeta.getString(row, index);
    String normalized = normalizeRegion(country);
    if (normalized == null) {
      return region;
    }
    if (supportedRegions.contains(normalized)) {
      return normalized;
    }
    logError(BaseMessages.getString(PKG, "StandardizePhoneNumber.Log.RegionNotSupported", country));
    return region;
  }

  private void setNumberType(Object[] outputRow, String numberTypeField, PhoneNumber phoneNumber) {
    if (Utils.isEmpty(numberTypeField)) {
      return;
    }
    int i = data.outputRowMeta.indexOfValue(numberTypeField);
    if (i < 0) {
      return;
    }
    if (phoneNumber != null) {
      outputRow[i] = phoneNumberService.getNumberType(phoneNumber).toString();
    } else {
      outputRow[i] = NUMBER_TYPE_ERROR;
    }
  }

  private void setIsValid(Object[] outputRow, String isValidNumberField, PhoneNumber phoneNumber) {
    if (Utils.isEmpty(isValidNumberField)) {
      return;
    }
    int i = data.outputRowMeta.indexOfValue(isValidNumberField);
    if (i < 0) {
      return;
    }
    if (phoneNumber != null) {
      outputRow[i] = phoneNumberService.isValidNumber(phoneNumber);
    } else {
      outputRow[i] = false;
    }
  }

  @Override
  public boolean init() {

    if (super.init()) {
      this.phoneNumberService = PhoneNumberUtil.getInstance();
      this.supportedRegions = phoneNumberService.getSupportedRegions();
      return true;
    }

    return false;
  }

  public PhoneNumberFormat getPhoneNumberFormat(String value) {
    try {
      return PhoneNumberFormat.valueOf(value);
    } catch (Exception e) {
      this.logError("Error parsing phone number format", e);
      return PhoneNumberFormat.E164;
    }
  }
}
