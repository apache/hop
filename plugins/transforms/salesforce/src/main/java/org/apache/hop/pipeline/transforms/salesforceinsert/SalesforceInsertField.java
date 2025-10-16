package org.apache.hop.pipeline.transforms.salesforceinsert;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.metadata.api.HopMetadataProperty;

@Getter
@Setter
public class SalesforceInsertField implements Cloneable {
  //  private static final Class<?> PKG = SalesforceInsertMeta.class;

  @HopMetadataProperty(key = "name")
  private String updateLookup;

  @HopMetadataProperty(key = "updateStream")
  private String updateStream;

  @HopMetadataProperty(key = "useExternalId")
  private boolean useExternalId;

  //  public boolean isUseExternalId() {
  //    return useExternalId != null ? useExternalId : false;
  //  }

  //  public Boolean getUseExternalId() {
  //    return useExternalId;
  //  }
  //
  //  public void setUseExternalId(Boolean useExternalId) {
  //    this.useExternalId = useExternalId;
  //  }

  //  public Boolean isUseExternalId() {
  //    return useExternalId;
  //  }

  public SalesforceInsertField() {
    this.updateLookup = "";
    this.updateStream = "";
    this.useExternalId = false;
  }

  @Override
  public Object clone() {
    try {
      return (SalesforceInsertField) super.clone();
    } catch (CloneNotSupportedException e) {
      return null;
    }
  }
}
