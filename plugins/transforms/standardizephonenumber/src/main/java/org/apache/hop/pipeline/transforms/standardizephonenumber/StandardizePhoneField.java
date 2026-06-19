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

import com.google.i18n.phonenumbers.PhoneNumberUtil.PhoneNumberFormat;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.metadata.api.HopMetadataProperty;

/** Contains the properties of the fields to standardize. */
@Getter
public class StandardizePhoneField implements Cloneable {

  /** The input field name */
  @HopMetadataProperty(
      key = "input",
      injectionKeyDescription = "StandardizePhoneNumber.Injection.InputField")
  private String inputField = null;

  /** The target field name */
  @HopMetadataProperty(
      key = "output",
      injectionKeyDescription = "StandardizePhoneNumber.Injection.OutputField")
  private String outputField = null;

  /** The format */
  @Setter
  @HopMetadataProperty(
      key = "format",
      injectionKeyDescription = "StandardizePhoneNumber.Injection.NumberFormat")
  private String numberFormat = PhoneNumberFormat.E164.name();

  /** The country field name */
  @HopMetadataProperty(
      key = "country",
      injectionKeyDescription = "StandardizePhoneNumber.Injection.CountryField")
  private String countryField = null;

  /** The country codes (ISO 2) */
  @Setter
  @HopMetadataProperty(
      key = "defaultCountry",
      injectionKeyDescription = "StandardizePhoneNumber.Injection.DefaultCountry")
  private String defaultCountry;

  @HopMetadataProperty(
      key = "numbertype",
      injectionKeyDescription = "StandardizePhoneNumber.Injection.NumberTypeField")
  private String numberTypeField;

  @HopMetadataProperty(
      key = "isvalidnumber",
      injectionKeyDescription = "StandardizePhoneNumber.Injection.IsValidNumberField")
  private String isValidNumberField = null;

  public StandardizePhoneField() {
    super();
  }

  public StandardizePhoneField(StandardizePhoneField other) {
    super();

    this.inputField = other.inputField;
    this.outputField = other.outputField;
    this.numberFormat = other.numberFormat;
    this.countryField = other.countryField;
    this.defaultCountry = other.defaultCountry;
    this.numberTypeField = other.numberTypeField;
    this.defaultCountry = other.defaultCountry;
    this.isValidNumberField = other.isValidNumberField;
  }

  @Override
  public Object clone() {
    return new StandardizePhoneField(this);
  }

  public void setInputField(final String field) {
    this.inputField = StringUtils.stripToNull(field);
  }

  public void setOutputField(final String field) {
    this.outputField = StringUtils.stripToNull(field);
  }

  public void setCountryField(final String field) {
    this.countryField = StringUtils.stripToNull(field);
  }

  public void setNumberTypeField(final String phoneNumberTypeField) {
    this.numberTypeField = StringUtils.stripToNull(phoneNumberTypeField);
  }

  public void setIsValidNumberField(final String field) {
    this.isValidNumberField = StringUtils.stripToNull(field);
  }
}
