package org.apache.hop.metadata.serializer.json.occupation;

import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadata;

import java.util.Objects;

@HopMetadata(
  name = "Occupation",
  key = "occupation"
)
public class Occupation implements IHopMetadata {

  @HopMetadataProperty
  private String name;

  @HopMetadataProperty
  private String description;

  @HopMetadataProperty
  private int startYear;

  public Occupation() {
  }

  public Occupation( String name, String description, int startYear ) {
    this.name = name;
    this.description = description;
    this.startYear = startYear;
  }

  @Override public boolean equals( Object o ) {
    if ( this == o ) {
      return true;
    }
    if ( o == null || getClass() != o.getClass() ) {
      return false;
    }
    Occupation that = (Occupation) o;
    return startYear == that.startYear &&
      Objects.equals( name, that.name ) &&
      Objects.equals( description, that.description );
  }

  @Override public int hashCode() {
    return Objects.hash( name, description, startYear );
  }

  /**
   * Gets name
   *
   * @return value of name
   */
  public String getName() {
    return name;
  }

  /**
   * @param name The name to set
   */
  public void setName( String name ) {
    this.name = name;
  }

  /**
   * Gets description
   *
   * @return value of description
   */
  public String getDescription() {
    return description;
  }

  /**
   * @param description The description to set
   */
  public void setDescription( String description ) {
    this.description = description;
  }

  /**
   * Gets startYear
   *
   * @return value of startYear
   */
  public int getStartYear() {
    return startYear;
  }

  /**
   * @param startYear The startYear to set
   */
  public void setStartYear( int startYear ) {
    this.startYear = startYear;
  }
}
