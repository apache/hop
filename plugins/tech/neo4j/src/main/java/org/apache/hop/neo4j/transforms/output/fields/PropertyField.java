package org.apache.hop.neo4j.transforms.output.fields;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.metadata.api.HopMetadataProperty;

@Getter
@Setter
public class PropertyField {

  @HopMetadataProperty(key = "name", injectionKey = "NODE_PROPERTY_NAME")
  private String propertyName;

  @HopMetadataProperty(key = "value", injectionKey = "NODE_PROPERTY_VALUE")
  private String propertyValue;

  @HopMetadataProperty(key = "type", injectionKey = "NODE_PROPERTY_TYPE")
  private String propertyType;

  @HopMetadataProperty(key = "primary", injectionKey = "NODE_PROPERTY_PRIMARY")
  private boolean propertyPrimary;
}
