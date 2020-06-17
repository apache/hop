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
