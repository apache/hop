package org.apache.hop.neo4j.transforms.output.fields;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.metadata.api.HopMetadataProperty;

@Getter
@Setter
public class NodeFromField extends NodeField {

  @HopMetadataProperty(key = "read_only_from_node", injectionKey = "READ_ONLY_FROM_NODE")
  private boolean readOnly;
}
