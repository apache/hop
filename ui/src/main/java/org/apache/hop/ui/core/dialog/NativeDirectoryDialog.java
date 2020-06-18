package org.apache.hop.ui.core.dialog;

import org.eclipse.swt.widgets.DirectoryDialog;

public class NativeDirectoryDialog implements IDirectoryDialog {

  private DirectoryDialog dialog;

  public NativeDirectoryDialog( DirectoryDialog dialog ) {
    this.dialog = dialog;
  }

  @Override public void setText( String text ) {
    dialog.setText( text );
  }

  @Override public String open() {
    return dialog.open();
  }

  @Override public void setMessage( String message ) {
    dialog.setMessage( message );
  }

  @Override public void setFilterPath( String filterPath ) {
    dialog.setFilterPath( filterPath );
  }

  @Override public String getFilterPath() {
    return dialog.getFilterPath();
  }
}
