package org.apache.hop.metadata.util;

import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;

public class HopMetadataInstance {
  private static HopMetadataInstance instance;

  private MultiMetadataProvider metadataProvider;

  private HopMetadataInstance() {
    // Nothing to do here.
  }

  public static HopMetadataInstance getInstance() {
    if (instance == null) {
      instance = new HopMetadataInstance();
    }
    return instance;
  }

  public static void setMetadataProvider(MultiMetadataProvider metadataProvider) {
    getInstance().metadataProvider = metadataProvider;
  }

  public static MultiMetadataProvider getMetadataProvider() {
    return getInstance().metadataProvider;
  }
}
