package org.apache.hop.ui.core.dialog;

public interface IDirectoryDialog {
  void setText( String text );

  String open();

  void setMessage( String message );

  void setFilterPath( String filterPath );

  String getFilterPath();
}
