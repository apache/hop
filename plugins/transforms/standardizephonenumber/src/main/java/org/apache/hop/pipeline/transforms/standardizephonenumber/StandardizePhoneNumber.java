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

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import java.util.Arrays;
import java.util.Set;
import com.google.i18n.phonenumbers.NumberParseException;
import com.google.i18n.phonenumbers.PhoneNumberUtil;
import com.google.i18n.phonenumbers.PhoneNumberUtil.PhoneNumberFormat;
import com.google.i18n.phonenumbers.Phonenumber.PhoneNumber;


public class StandardizePhoneNumber extends BaseTransform<StandardizePhoneNumberMeta, StandardizePhoneNumberData>
    implements ITransform<StandardizePhoneNumberMeta, StandardizePhoneNumberData> {
  private static final Class<?> PKG = StandardizePhoneNumber.class; // For Translator

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
        if (log.isDebug()) {
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

        // Default region
        String region = standardize.getDefaultCountry();
        if (!Utils.isEmpty(standardize.getCountryField())) {

            int index = inputRowMeta.indexOfValue(standardize.getCountryField());

            // if country field not found
            if (index < 0) {
                logError(BaseMessages.getString(PKG, "StandardizePhoneNumber.Log.CountryFieldNotFound",
                        standardize.getCountryField()));
                this.setErrors(1);
                return false;
            }

            String country = inputRowMeta.getString(row, index);
            if (country == null || Utils.isEmpty(country)) {
                region = standardize.getDefaultCountry();                   
            } else if (supportedRegions.contains(country.toUpperCase())) {
                region = country.toUpperCase();
            } else {
                logError(BaseMessages.getString(PKG, "StandardizePhoneNumber.Log.RegionNotSupported", country));
                region = standardize.getDefaultCountry();
            }

        }

        // Parse phone number
        String value = null;
        int index = inputRowMeta.indexOfValue(standardize.getInputField());

        // if input field not found
        if (index < 0) {
            this.logError(BaseMessages.getString(PKG, "StandardizePhoneNumber.Log.InputFieldNotFound",
                    standardize.getInputField()));
            this.setErrors(1);
            return false;
        }
        value = inputRowMeta.getString(row, index);

        if (value != null && !Utils.isEmpty(value)) {
            PhoneNumber phoneNumber = null;
            try {
                // Replace unsupported character wit blank
                value = value.replace(',', ' ');
                
                // Format 
                PhoneNumberFormat format = getPhoneNumberFormat(standardize.getNumberFormat());
                
                // Parse phone number
                phoneNumber = phoneNumberService.parse(value, region);
                if (!Utils.isEmpty(standardize.getOutputField())) {
                    index = data.outputRowMeta.indexOfValue(standardize.getOutputField());
                }
                outputRow[index] = phoneNumberService.format(phoneNumber, format);
            } catch (NumberParseException e) {
                if (log.isRowLevel()) {
                    logRowlevel(BaseMessages.getString(PKG, "StandardizePhoneNumber.Log.ProcessPhoneNumberError",
                            standardize.getInputField(), value));
                }
            }

            if (!Utils.isEmpty(standardize.getNumberTypeField())) {
                int i = data.outputRowMeta.indexOfValue(standardize.getNumberTypeField());
                if (phoneNumber != null)
                    outputRow[i] = phoneNumberService.getNumberType(phoneNumber);
                else
                    outputRow[i] = "ERROR";
            }

            if (!Utils.isEmpty(standardize.getIsValidNumberField())) {
                int i = data.outputRowMeta.indexOfValue(standardize.getIsValidNumberField());
                if (phoneNumber != null)
                    outputRow[i] = phoneNumberService.isValidNumber(phoneNumber);
                else
                    outputRow[i] = false;
            }
        } else {
            if (!Utils.isEmpty(standardize.getIsValidNumberField())) {
                int i = data.outputRowMeta.indexOfValue(standardize.getIsValidNumberField());
                outputRow[i] = false;
            }
        }
    }

    // put the row to the output row stream
    putRow(data.outputRowMeta, outputRow);

    if (log.isRowLevel()) {
        logRowlevel(
                BaseMessages.getString(PKG, "StandardizePhoneNumber.Log.WroteRowToNextTransform", outputRow));
    }

    // log progress if it is time to to so
    if (checkFeedback(getLinesRead())) {
        logBasic("Line nr " + getLinesRead());
    }

    // indicate that processRow() should be called again
    return true;
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
