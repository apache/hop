package org.apache.hop.pipeline.transforms.normaliser;

import org.apache.hop.metadata.api.HopMetadataProperty;
import java.util.Objects;

public class NormaliserField {

  @HopMetadataProperty(key="name", injectionKey = "NAME", injectionKeyDescription = "NormaliserMeta.Injection.NAME")
  private String name;

  @HopMetadataProperty(key="value", injectionKey = "VALUE", injectionKeyDescription = "NormaliserMeta.Injection.VALUE")
  private String value;

  @HopMetadataProperty(key="norm", injectionKey = "NORMALISED", injectionKeyDescription = "NormaliserMeta.Injection.NORMALISED")
  private String norm;

  public NormaliserField() {
    // Do nothing
  }

  public NormaliserField(NormaliserField field) {
    this.name = field.name;
    this.value = field.value;
    this.norm = field.norm;
  }
  
  /** @return the name */
  public String getName() {
    return name;
  }

  /** @param name the name to set */
  public void setName(String name) {
    this.name = name;
  }

  /** @return the value */
  public String getValue() {
    return value;
  }

  /** @param value the value to set */
  public void setValue(String value) {
    this.value = value;
  }

  /** @return the norm */
  public String getNorm() {
    return norm;
  }

  /** @param norm the norm to set */
  public void setNorm(String norm) {
    this.norm = norm;
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, norm, value);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    NormaliserField other = (NormaliserField) obj;
    if (name == null) {
      if (other.name != null) {
        return false;
      }
    } else if (!name.equals(other.name)) {
      return false;
    }
    if (norm == null) {
      if (other.norm != null) {
        return false;
      }
    } else if (!norm.equals(other.norm)) {
      return false;
    }
    if (value == null) {
      if (other.value != null) {
        return false;
      }
    } else if (!value.equals(other.value)) {
      return false;
    }
    return true;
  }
}