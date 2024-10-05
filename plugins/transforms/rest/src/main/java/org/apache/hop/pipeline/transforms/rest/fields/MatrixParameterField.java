package org.apache.hop.pipeline.transforms.rest.fields;

import org.apache.hop.metadata.api.HopMetadataProperty;

public class MatrixParameterField {

  @HopMetadataProperty(key = "field", injectionKey = "MATRIX_PARAMETER_FIELD")
  private String headerField;

  @HopMetadataProperty(key = "name", injectionKey = "MATRIX_PARAMETER_NAME")
  private String name;

  public MatrixParameterField() {}

  public MatrixParameterField(String field, String name) {
    this.headerField = field;
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
