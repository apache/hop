package org.apache.hop.ui.core.dialog;

public interface IFileDialog {
  void setText( String text );

  void setFilterExtensions( String[] filterExtensions );

  void setFilterNames( String[] filterNames );

  void setFileName(String fileName);

  String getFilterPath();
  String getFileName();

  String open();

  void setFilterPath( String filterPath );
}