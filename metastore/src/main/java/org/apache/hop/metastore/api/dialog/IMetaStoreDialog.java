package org.apache.hop.metastore.api.dialog;

public interface IMetaStoreDialog {
  /**
   * Open a dialog which edits the metadata of a metastore element.
   *
   * @return null in case the dialog was cancelled or the name of the element if OK was pressed.
   */
  String open();
}
