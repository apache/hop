package org.apache.hop.neo4j.transforms.output.fields;

import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.metadata.api.HopMetadataProperty;

@Getter
@Setter
public class NodeField {

  @HopMetadataProperty(
      key = "property",
      injectionKey = "",
      groupKey = "properties",
      injectionGroupKey = "")
  private List<PropertyField> properties;

  @HopMetadataProperty(
      key = "labels",
      injectionKey = "",
      inlineListTags = {"label", "value"})
  private List<LabelField> labels;
}
