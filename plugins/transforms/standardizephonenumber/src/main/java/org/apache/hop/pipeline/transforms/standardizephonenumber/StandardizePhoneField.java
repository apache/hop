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

import org.apache.commons.lang.StringUtils;
import org.apache.hop.metadata.api.HopMetadataProperty;
import com.google.i18n.phonenumbers.PhoneNumberUtil.PhoneNumberFormat;

/**
 * Contains the properties of the fields to standardize.
 */
public class StandardizePhoneField implements Cloneable {

  /** The input field name */
  @HopMetadataProperty(key = "input",
      injectionKeyDescription = "StandardizePhoneNumber.Injection.InputField")
  private String inputField = null;

  /** The target field name */
  @HopMetadataProperty(key = "output",
      injectionKeyDescription = "StandardizePhoneNumber.Injection.OutputField")
  private String outputField = null;

  /** The format */
  @HopMetadataProperty(key = "format",
      injectionKeyDescription = "StandardizePhoneNumber.Injection.NumberFormat")
  private String numberFormat = PhoneNumberFormat.E164.name();
  
  /** The country field name */
  @HopMetadataProperty(key = "country",
      injectionKeyDescription = "StandardizePhoneNumber.Injection.CountryField")
  private String countryField = null;

  @HopMetadataProperty(key = "defaultCountry",
      injectionKeyDescription = "StandardizePhoneNumber.Injection.DefaultCountry")
  private String defaultCountry;

  @HopMetadataProperty(key = "numbertype",
      injectionKeyDescription = "StandardizePhoneNumber.Injection.NumberTypeField")
  private String numberTypeField;

  @HopMetadataProperty(key = "isvalidnumber",
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

  public String getInputField() {
    return inputField;
  }

  public void setInputField(final String field) {
    this.inputField = StringUtils.stripToNull(field);
  }

  public String getOutputField() {
    return outputField;
  }

  public void setOutputField(final String field) {
    this.outputField = StringUtils.stripToNull(field);
  }

  public String getCountryField() {
    return countryField;
  }

  public void setCountryField(final String field) {
    this.countryField = StringUtils.stripToNull(field);
  }


  /**
   * Get the country codes (ISO 2)
   * 
   * @return
   */
  public String getDefaultCountry() {
    return defaultCountry;
  }

  /**
   * Set the country codes (ISO 2)
   * 
   * @param country
   */
  public void setDefaultCountry(String country) {
    this.defaultCountry = country;
  }
  
  public String getNumberFormat() {
    return numberFormat;
  }

  public void setNumberFormat(final String param) {
      this.numberFormat = param;
  }

  public String getNumberTypeField() {
    return numberTypeField;
  }

  public void setNumberTypeField(final String phoneNumberTypeField) {
    this.numberTypeField = StringUtils.stripToNull(phoneNumberTypeField);
  }

  public String getIsValidNumberField() {
    return isValidNumberField;
  }

  public void setIsValidNumberField(final String field) {
    this.isValidNumberField = StringUtils.stripToNull(field);
  }
}
