package org.apache.hop.ui.core.dialog;


import org.eclipse.swt.widgets.FileDialog;

public class NativeFileDialog implements IFileDialog {

  private org.eclipse.swt.widgets.FileDialog fileDialog;

  public NativeFileDialog( FileDialog fileDialog ) {
    this.fileDialog = fileDialog;
  }

  @Override public void setText( String text ) {
    fileDialog.setText(text);
  }

  @Override public void setFilterExtensions( String[] filterExtensions ) {
    fileDialog.setFilterExtensions( filterExtensions );
  }

  @Override public void setFilterNames( String[] filterNames ) {
    fileDialog.setFilterNames( filterNames );
  }

  @Override public void setFileName( String fileName ) {
    fileDialog.setFileName( fileName );
  }

  @Override public String getFilterPath() {
    return fileDialog.getFilterPath();
  }

  @Override public String getFileName() {
    return fileDialog.getFileName();
  }

  @Override public String open() {
    return fileDialog.open();
  }

  @Override public void setFilterPath( String filterPath ) {
    fileDialog.setFilterPath( filterPath );
  }

  /**
   * Gets fileDialog
   *
   * @return value of fileDialog
   */
  public FileDialog getFileDialog() {
    return fileDialog;
  }

  /**
   * @param fileDialog The fileDialog to set
   */
  public void setFileDialog( FileDialog fileDialog ) {
    this.fileDialog = fileDialog;
  }


}
