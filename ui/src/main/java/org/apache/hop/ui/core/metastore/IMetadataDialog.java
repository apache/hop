package org.apache.hop.ui.core.metastore;

/**
 * This interface indicates that the class can be used to edit metadata.
 */
public interface IMetadataDialog {
  /**
   * Open the dialog and return the name of the metadata object.
   * @return The name of the metadata object or null if canceled/closed.
   */
  public String open();
}
