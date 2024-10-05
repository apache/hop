package org.apache.hop.pipeline.transforms.rest.fields;

import org.apache.hop.metadata.api.HopMetadataProperty;

public class HeaderField {

  @HopMetadataProperty(key = "field", injectionKey = "HEADER_FIELD_FIELD")
  private String headerField;

  @HopMetadataProperty(key = "name", injectionKey = "HEADER_FIELD_NAME")
  private String name;

  public HeaderField() {}

  public HeaderField(String headerField, String name) {
    this.name = name;
    this.headerField = headerField;
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
