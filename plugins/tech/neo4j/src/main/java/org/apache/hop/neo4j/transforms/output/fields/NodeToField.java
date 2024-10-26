package org.apache.hop.neo4j.transforms.output.fields;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.metadata.api.HopMetadataProperty;

@Getter
@Setter
public class NodeToField extends NodeField {

  @HopMetadataProperty(key = "read_only_to_node", injectionKey = "READ_ONLY_TO_NODE")
  private boolean readOnly;
}
