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

package org.apache.hop.metadata.serializer.json.person;

import org.apache.hop.metadata.api.HopMetadataProperty;

import java.util.Objects;

public class City {

  @HopMetadataProperty
  private String zipCode;

  @HopMetadataProperty
  private String cityName;

  public City() {
  }

  public City( String zipCode, String cityName ) {
    this.zipCode = zipCode;
    this.cityName = cityName;
  }

  @Override public boolean equals( Object o ) {
    if ( this == o ) {
      return true;
    }
    if ( o == null || getClass() != o.getClass() ) {
      return false;
    }
    City city = (City) o;
    return Objects.equals( zipCode, city.zipCode ) &&
      Objects.equals( cityName, city.cityName );
  }

  @Override public int hashCode() {
    return Objects.hash( zipCode, cityName );
  }

  /**
   * Gets zipCode
   *
   * @return value of zipCode
   */
  public String getZipCode() {
    return zipCode;
  }

  /**
   * @param zipCode The zipCode to set
   */
  public void setZipCode( String zipCode ) {
    this.zipCode = zipCode;
  }

  /**
   * Gets cityName
   *
   * @return value of cityName
   */
  public String getCityName() {
    return cityName;
  }

  /**
   * @param cityName The cityName to set
   */
  public void setCityName( String cityName ) {
    this.cityName = cityName;
  }
}
