package org.apache.hop.env.config;

import org.apache.hop.core.Const;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;


public class EnvironmentConfigDialog extends Dialog {
  private static Class<?> PKG = EnvironmentConfigDialog.class; // for i18n purposes, needed by Translator2!!

  private final EnvironmentConfig config;

  private boolean ok;

  private Shell shell;
  private final PropsUi props;

  private Button wEnabled;
  private Button wAutoOpen;

  private int margin;
  private int middle;

  public EnvironmentConfigDialog( Shell parent, EnvironmentConfig config ) {
    super( parent, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.RESIZE );

    this.config = config;

    props = PropsUi.getInstance();
  }

  public boolean open() {

    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.RESIZE );
    shell.setImage( GuiResource.getInstance().getImageHopUi() );
    props.setLook( shell );

    margin = Const.MARGIN + 2;
    middle = Const.MIDDLE_PCT;

    FormLayout formLayout = new FormLayout();

    shell.setLayout( formLayout );
    shell.setText( "Environment Configuration" );

    Label wlEnabled = new Label( shell, SWT.RIGHT );
    props.setLook( wlEnabled );
    wlEnabled.setText( "Enable Hop Environment? " );
    FormData fdlEnabled = new FormData();
    fdlEnabled.left = new FormAttachment( 0, 0 );
    fdlEnabled.right = new FormAttachment( middle, 0 );
    fdlEnabled.top = new FormAttachment( 0, margin );
    wlEnabled.setLayoutData( fdlEnabled );
    wEnabled = new Button( shell, SWT.CHECK | SWT.LEFT );
    props.setLook( wEnabled );
    FormData fdEnabled = new FormData();
    fdEnabled.left = new FormAttachment( middle, margin );
    fdEnabled.right = new FormAttachment( 100, 0 );
    fdEnabled.top = new FormAttachment( wlEnabled, 0, SWT.CENTER );
    wEnabled.setLayoutData( fdEnabled );
    Control lastControl = wEnabled;

    Label wlAutoOpen = new Label( shell, SWT.RIGHT );
    props.setLook( wlAutoOpen );
    wlAutoOpen.setText( "Open last environment at Hop GUI startup? " );
    FormData fdlAutoOpen = new FormData();
    fdlAutoOpen.left = new FormAttachment( 0, 0 );
    fdlAutoOpen.right = new FormAttachment( middle, 0 );
    fdlAutoOpen.top = new FormAttachment( lastControl, margin );
    wlAutoOpen.setLayoutData( fdlAutoOpen );
    wAutoOpen = new Button( shell, SWT.CHECK | SWT.LEFT );
    props.setLook( wAutoOpen );
    FormData fdAutoOpen = new FormData();
    fdAutoOpen.left = new FormAttachment( middle, margin );
    fdAutoOpen.right = new FormAttachment( 100, 0 );
    fdAutoOpen.top = new FormAttachment( wlAutoOpen, 0, SWT.CENTER );
    wAutoOpen.setLayoutData( fdAutoOpen );
    lastControl = wAutoOpen;


    Button wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOK.addListener( SWT.Selection, event -> ok() );
    Button wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection, event -> cancel() );

    // Buttons go at the bottom of the dialog
    //
    BaseTransformDialog.positionBottomButtons( shell, new Button[] { wOK, wCancel }, margin * 3, lastControl );

    // Set the shell size, based upon previous time...
    BaseTransformDialog.setSize( shell );

    getData();

    shell.open();

    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }

    return ok;
  }

  private void ok() {
    getInfo( config );
    ok = true;

    dispose();
  }

  private void cancel() {
    ok = false;

    dispose();
  }

  public void dispose() {
    props.setScreen( new WindowProperty( shell ) );
    shell.dispose();
  }

  private void getData() {
    wEnabled.setSelection( config.isEnabled() );
    wAutoOpen.setSelection( config.isOpeningLastEnvironmentAtStartup() );
  }

  private void getInfo( EnvironmentConfig conf ) {
    conf.setEnabled( wEnabled.getSelection() );
    conf.setOpeningLastEnvironmentAtStartup( wAutoOpen.getSelection() );
  }
}
