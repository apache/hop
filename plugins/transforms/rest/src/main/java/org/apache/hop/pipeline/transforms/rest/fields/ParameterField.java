package org.apache.hop.pipeline.transforms.rest.fields;

import org.apache.hop.metadata.api.HopMetadataProperty;

public class ParameterField {

  @HopMetadataProperty(key = "field", injectionKey = "PARAMETER_FIELD")
  private String headerField;

  @HopMetadataProperty(key = "name", injectionKey = "PARAMETER_NAME")
  private String name;

  public ParameterField() {}

  public ParameterField(String headerField, String name) {
    this.headerField = headerField;
    this.name = name;
  }

  public String getHeaderField() {
    return headerField;
  }

  public void setHeaderField(String headerField) {
    this.headerField = headerField;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }
}
