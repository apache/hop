package org.apache.hop.neo4j.transforms.loginfo;

import java.util.Objects;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.metadata.api.HopMetadataProperty;

@Getter
@Setter
public class GetLoggingInfoField {

  public GetLoggingInfoField() {}

  public GetLoggingInfoField(String fieldName, String fieldType, String fieldArgument) {
    this.fieldName = fieldName;
    this.fieldType = fieldType;
    this.fieldArgument = fieldArgument;
  }

  @HopMetadataProperty(key = "name", injectionKey = "FIELD_NAME")
  private String fieldName;

  @HopMetadataProperty(key = "type", injectionKey = "FIELD_TYPE")
  private String fieldType;

  @HopMetadataProperty(key = "argument", injectionKey = "FIELD_ARGUMENT")
  private String fieldArgument;

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || getClass() != obj.getClass()) return false;
    GetLoggingInfoField that = (GetLoggingInfoField) obj;
    return fieldType.equals(that.fieldType)
        && fieldName.equals(that.fieldName)
        && fieldArgument.equals(that.fieldArgument);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fieldName, fieldType, fieldArgument);
  }
}
