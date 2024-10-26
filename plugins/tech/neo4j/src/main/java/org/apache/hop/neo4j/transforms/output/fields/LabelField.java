package org.apache.hop.neo4j.transforms.output.fields;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.metadata.api.HopMetadataProperty;

@Getter
@Setter
public class LabelField {

  @HopMetadataProperty(key = "value", injectionKey = "")
  private String label;

  @HopMetadataProperty(key = "label", injectionKey = "")
  private String labelField;
}
