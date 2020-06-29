package org.apache.hop.metadata.api;

public interface IHasHopMetadataProvider {

  IHopMetadataProvider getMetadataProvider();

  void setMetadataProvider(IHopMetadataProvider metadataProvider);

}
