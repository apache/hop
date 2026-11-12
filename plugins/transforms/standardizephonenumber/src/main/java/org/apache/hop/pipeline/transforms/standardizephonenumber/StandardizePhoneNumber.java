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
import java.util.ArrayList;
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

      // The settings of every field do not change from row to row: work them out once
      if (!prepareFields()) {
        return false;
      }
    }

    IRowMeta inputRowMeta = getInputRowMeta();

    // copies row into outputRowValues and pads extra null-default slots for
    // the output values
    Object[] outputRow = Arrays.copyOf(row, data.outputRowMeta.size());

    for (StandardizePhoneNumberData.Field field : data.fields) {
      String region = region(field, inputRowMeta, row);

      String originalValue = inputRowMeta.getString(row, field.inputIndex);
      if (!Utils.isEmpty(originalValue)) {
        PhoneNumber phoneNumber = null;
        try {
          // Replace unsupported character with blank
          String value = originalValue.replace(',', ' ');

          phoneNumber = phoneNumberService.parse(value, region);
          outputRow[field.outputIndex] = phoneNumberService.format(phoneNumber, format(field));
        } catch (NumberParseException e) {
          outputRow[field.outputIndex] = originalValue;
          if (isRowLevel()) {
            logRowlevel(
                BaseMessages.getString(
                    PKG,
                    "StandardizePhoneNumber.Log.ProcessPhoneNumberError",
                    field.inputField,
                    originalValue));
          }
        }

        setNumberType(outputRow, field.numberTypeIndex, phoneNumber);
        setIsValid(outputRow, field.isValidIndex, phoneNumber);
      } else {
        setNumberType(outputRow, field.numberTypeIndex, null);
        setIsValid(outputRow, field.isValidIndex, null);
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

  /**
   * Resolves the settings of every field: the names, where the fields are in the row, the default
   * region and the number format. Called on the first row.
   *
   * @return false when an input field is not in the row: the error is logged
   */
  private boolean prepareFields() throws HopException {
    IRowMeta inputRowMeta = getInputRowMeta();
    data.fields = new ArrayList<>();
    for (StandardizePhoneField standardize : meta.getFields()) {
      StandardizePhoneNumberData.Field field = new StandardizePhoneNumberData.Field();
      field.inputField = resolve(standardize.getInputField());
      String outputField = resolve(standardize.getOutputField());
      String countryField = resolve(standardize.getCountryField());
      String numberTypeField = resolve(standardize.getNumberTypeField());
      String isValidNumberField = resolve(standardize.getIsValidNumberField());
      field.numberFormat = resolve(standardize.getNumberFormat());
      field.defaultRegion = normalizeRegion(resolve(standardize.getDefaultCountry()));

      field.countryIndex = -1;
      if (!Utils.isEmpty(countryField)) {
        field.countryIndex = inputRowMeta.indexOfValue(countryField);

        // if country field not found
        if (field.countryIndex < 0) {
          String message =
              BaseMessages.getString(
                  PKG, "StandardizePhoneNumber.Log.CountryFieldNotFound", countryField);
          logError(message);
          this.setErrors(1);
          throw new HopException(message);
        }
      }

      field.inputIndex = inputRowMeta.indexOfValue(field.inputField);

      // if input field not found
      if (field.inputIndex < 0) {
        this.logError(
            BaseMessages.getString(
                PKG, "StandardizePhoneNumber.Log.InputFieldNotFound", field.inputField));
        this.setErrors(1);
        return false;
      }

      field.outputIndex = field.inputIndex;
      if (!Utils.isEmpty(outputField)) {
        int resolvedOutputIndex = data.outputRowMeta.indexOfValue(outputField);
        if (resolvedOutputIndex >= 0) {
          field.outputIndex = resolvedOutputIndex;
        }
      }

      field.numberTypeIndex =
          Utils.isEmpty(numberTypeField) ? -1 : data.outputRowMeta.indexOfValue(numberTypeField);
      field.isValidIndex =
          Utils.isEmpty(isValidNumberField)
              ? -1
              : data.outputRowMeta.indexOfValue(isValidNumberField);

      data.fields.add(field);
    }
    return true;
  }

  /**
   * The region to parse a number with: the country of the row when it is known, else the default.
   */
  private String region(StandardizePhoneNumberData.Field field, IRowMeta inputRowMeta, Object[] row)
      throws HopException {
    if (field.countryIndex < 0) {
      return field.defaultRegion;
    }

    String country = inputRowMeta.getString(row, field.countryIndex);
    String normalized = normalizeRegion(country);
    if (normalized == null) {
      return field.defaultRegion;
    }
    if (supportedRegions.contains(normalized)) {
      return normalized;
    }
    logError(BaseMessages.getString(PKG, "StandardizePhoneNumber.Log.RegionNotSupported", country));
    return field.defaultRegion;
  }

  /** The number format of a field, looked up the first time a number has to be formatted. */
  private PhoneNumberFormat format(StandardizePhoneNumberData.Field field) {
    if (field.format == null) {
      field.format = getPhoneNumberFormat(field.numberFormat);
    }
    return field.format;
  }

  private void setNumberType(Object[] outputRow, int index, PhoneNumber phoneNumber) {
    if (index < 0) {
      return;
    }
    if (phoneNumber != null) {
      outputRow[index] = phoneNumberService.getNumberType(phoneNumber).toString();
    } else {
      outputRow[index] = NUMBER_TYPE_ERROR;
    }
  }

  private void setIsValid(Object[] outputRow, int index, PhoneNumber phoneNumber) {
    if (index < 0) {
      return;
    }
    if (phoneNumber != null) {
      outputRow[index] = phoneNumberService.isValidNumber(phoneNumber);
    } else {
      outputRow[index] = false;
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
