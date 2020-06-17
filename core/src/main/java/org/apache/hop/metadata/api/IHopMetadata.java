package org.apache.hop.metadata.api;

/**
 * Hop Metadata object classes should implement this interface so the plugin system can recognize them.
 * Other than that they just have a name really.  Maybe later we can add keywords and some ACLs.
 */
public interface IHopMetadata {
  String getName();
}
