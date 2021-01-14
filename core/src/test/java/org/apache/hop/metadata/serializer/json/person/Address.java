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

public class Address {

  @HopMetadataProperty
  private String street;

  @HopMetadataProperty
  private String number;

  @HopMetadataProperty
  private City city;

  public Address() {
  }

  public Address( String street, String number, City city ) {
    this.street = street;
    this.number = number;
    this.city = city;
  }

  @Override public boolean equals( Object o ) {
    if ( this == o ) {
      return true;
    }
    if ( o == null || getClass() != o.getClass() ) {
      return false;
    }
    Address address = (Address) o;
    return Objects.equals( street, address.street ) &&
      Objects.equals( number, address.number ) &&
      Objects.equals( city, address.city );
  }

  @Override public int hashCode() {
    return Objects.hash( street, number, city );
  }

  /**
   * Gets street
   *
   * @return value of street
   */
  public String getStreet() {
    return street;
  }

  /**
   * @param street The street to set
   */
  public void setStreet( String street ) {
    this.street = street;
  }

  /**
   * Gets number
   *
   * @return value of number
   */
  public String getNumber() {
    return number;
  }

  /**
   * @param number The number to set
   */
  public void setNumber( String number ) {
    this.number = number;
  }

  /**
   * Gets city
   *
   * @return value of city
   */
  public City getCity() {
    return city;
  }

  /**
   * @param city The city to set
   */
  public void setCity( City city ) {
    this.city = city;
  }
}
