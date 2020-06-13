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
