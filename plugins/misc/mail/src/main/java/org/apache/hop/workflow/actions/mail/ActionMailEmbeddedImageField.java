package org.apache.hop.workflow.actions.mail;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.metadata.api.HopMetadataProperty;

@Getter
@Setter
public class ActionMailEmbeddedImageField {

  @HopMetadataProperty(key = "image_name")
  private String embeddedimage;

  @HopMetadataProperty(key = "content_id")
  private String contentId;
}
