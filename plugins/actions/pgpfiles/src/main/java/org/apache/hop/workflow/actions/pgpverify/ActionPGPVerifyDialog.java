/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.workflow.actions.pgpverify;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.IActionDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

/**
 * This defines a PGP verify action.
 *
 * @author Samatar
 * @since 25-02-2011
 */
public class ActionPGPVerifyDialog extends ActionDialog implements IActionDialog {
  private static Class<?> PKG = ActionPGPVerify.class; // for i18n purposes, needed by Translator!!

  private static final String[] EXTENSIONS = new String[] { "*" };

  private static final String[] FILETYPES = new String[] { BaseMessages.getString(
    PKG, "JobPGPVerify.Filetype.All" ) };

  private Label wlName;

  private Text wName;

  private FormData fdlName, fdName;

  private Label wlGPGLocation;

  private Button wbGPGLocation;

  private TextVar wGPGLocation;

  private FormData fdlGPGLocation, fdbGPGLocation, fdGPGLocation;

  private Label wlFilename;

  private Button wbFilename;

  private TextVar wFilename;

  private FormData fdlFilename, fdbFilename, fdFilename;

  private Label wluseDetachedSignature;

  private Button wuseDetachedSignature;

  private FormData fdluseDetachedSignature, fduseDetachedSignature;

  private Label wlDetachedFilename;

  private Button wbDetachedFilename;

  private TextVar wDetachedFilename;

  private FormData fdlDetachedFilename, fdbDetachedFilename, fdDetachedFilename;

  private Button wOk, wCancel;

  private Listener lsOk, lsCancel;

  private ActionPGPVerify action;

  private Shell shell;

  private SelectionAdapter lsDef;

  private boolean changed;

  private Group wSettings;
  private FormData fdSettings;

  public ActionPGPVerifyDialog( Shell parent, IAction action, WorkflowMeta workflowMeta ) {
    super( parent, action, workflowMeta );
    this.action = (ActionPGPVerify) action;
    if ( this.action.getName() == null ) {
      this.action.setName( BaseMessages.getString( PKG, "JobPGPVerify.Name.Default" ) );
    }
  }

  public IAction open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.MIN | SWT.MAX | SWT.RESIZE );
    props.setLook( shell );
    WorkflowDialog.setShellImage( shell, action );

    ModifyListener lsMod = new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        action.setChanged();
      }
    };
    changed = action.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "JobPGPVerify.Title" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // GPGLocation line
    wlName = new Label( shell, SWT.RIGHT );
    wlName.setText( BaseMessages.getString( PKG, "JobPGPVerify.Name.Label" ) );
    props.setLook( wlName );
    fdlName = new FormData();
    fdlName.left = new FormAttachment( 0, 0 );
    fdlName.right = new FormAttachment( middle, -margin );
    fdlName.top = new FormAttachment( 0, margin );
    wlName.setLayoutData( fdlName );
    wName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wName );
    wName.addModifyListener( lsMod );
    fdName = new FormData();
    fdName.left = new FormAttachment( middle, 0 );
    fdName.top = new FormAttachment( 0, margin );
    fdName.right = new FormAttachment( 100, 0 );
    wName.setLayoutData( fdName );

    // ////////////////////////
    // START OF SERVER SETTINGS GROUP///
    // /
    wSettings = new Group( shell, SWT.SHADOW_NONE );
    props.setLook( wSettings );
    wSettings.setText( BaseMessages.getString( PKG, "JobPGPVerify.Settings.Group.Label" ) );

    FormLayout SettingsgroupLayout = new FormLayout();
    SettingsgroupLayout.marginWidth = 10;
    SettingsgroupLayout.marginHeight = 10;

    wSettings.setLayout( SettingsgroupLayout );

    // GPGLocation line
    wlGPGLocation = new Label( wSettings, SWT.RIGHT );
    wlGPGLocation.setText( BaseMessages.getString( PKG, "JobPGPVerify.GPGLocation.Label" ) );
    props.setLook( wlGPGLocation );
    fdlGPGLocation = new FormData();
    fdlGPGLocation.left = new FormAttachment( 0, 0 );
    fdlGPGLocation.top = new FormAttachment( wName, margin );
    fdlGPGLocation.right = new FormAttachment( middle, -margin );
    wlGPGLocation.setLayoutData( fdlGPGLocation );

    wbGPGLocation = new Button( wSettings, SWT.PUSH | SWT.CENTER );
    props.setLook( wbGPGLocation );
    wbGPGLocation.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    fdbGPGLocation = new FormData();
    fdbGPGLocation.right = new FormAttachment( 100, 0 );
    fdbGPGLocation.top = new FormAttachment( wName, 0 );
    wbGPGLocation.setLayoutData( fdbGPGLocation );

    wGPGLocation = new TextVar( workflowMeta, wSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wGPGLocation );
    wGPGLocation.addModifyListener( lsMod );
    fdGPGLocation = new FormData();
    fdGPGLocation.left = new FormAttachment( middle, 0 );
    fdGPGLocation.top = new FormAttachment( wName, margin );
    fdGPGLocation.right = new FormAttachment( wbGPGLocation, -margin );
    wGPGLocation.setLayoutData( fdGPGLocation );

    // Filename line
    wlFilename = new Label( wSettings, SWT.RIGHT );
    wlFilename.setText( BaseMessages.getString( PKG, "JobPGPVerify.Filename.Label" ) );
    props.setLook( wlFilename );
    fdlFilename = new FormData();
    fdlFilename.left = new FormAttachment( 0, 0 );
    fdlFilename.top = new FormAttachment( wGPGLocation, margin );
    fdlFilename.right = new FormAttachment( middle, -margin );
    wlFilename.setLayoutData( fdlFilename );

    wbFilename = new Button( wSettings, SWT.PUSH | SWT.CENTER );
    props.setLook( wbFilename );
    wbFilename.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    fdbFilename = new FormData();
    fdbFilename.right = new FormAttachment( 100, 0 );
    fdbFilename.top = new FormAttachment( wGPGLocation, 0 );
    wbFilename.setLayoutData( fdbFilename );

    wFilename = new TextVar( workflowMeta, wSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFilename );
    wFilename.addModifyListener( lsMod );
    fdFilename = new FormData();
    fdFilename.left = new FormAttachment( middle, 0 );
    fdFilename.top = new FormAttachment( wGPGLocation, margin );
    fdFilename.right = new FormAttachment( wbFilename, -margin );
    wFilename.setLayoutData( fdFilename );

    wluseDetachedSignature = new Label( wSettings, SWT.RIGHT );
    wluseDetachedSignature.setText( BaseMessages.getString( PKG, "JobPGPVerify.useDetachedSignature.Label" ) );
    props.setLook( wluseDetachedSignature );
    fdluseDetachedSignature = new FormData();
    fdluseDetachedSignature.left = new FormAttachment( 0, 0 );
    fdluseDetachedSignature.top = new FormAttachment( wFilename, margin );
    fdluseDetachedSignature.right = new FormAttachment( middle, -margin );
    wluseDetachedSignature.setLayoutData( fdluseDetachedSignature );
    wuseDetachedSignature = new Button( wSettings, SWT.CHECK );
    props.setLook( wuseDetachedSignature );
    wuseDetachedSignature.setToolTipText( BaseMessages
      .getString( PKG, "JobPGPVerify.useDetachedSignature.Tooltip" ) );
    fduseDetachedSignature = new FormData();
    fduseDetachedSignature.left = new FormAttachment( middle, 0 );
    fduseDetachedSignature.top = new FormAttachment( wFilename, margin );
    fduseDetachedSignature.right = new FormAttachment( 100, -margin );
    wuseDetachedSignature.setLayoutData( fduseDetachedSignature );
    wuseDetachedSignature.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {

        enableDetachedSignature();

      }
    } );

    // DetachedFilename line
    wlDetachedFilename = new Label( wSettings, SWT.RIGHT );
    wlDetachedFilename.setText( BaseMessages.getString( PKG, "JobPGPVerify.DetachedFilename.Label" ) );
    props.setLook( wlDetachedFilename );
    fdlDetachedFilename = new FormData();
    fdlDetachedFilename.left = new FormAttachment( 0, 0 );
    fdlDetachedFilename.top = new FormAttachment( wuseDetachedSignature, margin );
    fdlDetachedFilename.right = new FormAttachment( middle, -margin );
    wlDetachedFilename.setLayoutData( fdlDetachedFilename );

    wbDetachedFilename = new Button( wSettings, SWT.PUSH | SWT.CENTER );
    props.setLook( wbDetachedFilename );
    wbDetachedFilename.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    fdbDetachedFilename = new FormData();
    fdbDetachedFilename.right = new FormAttachment( 100, 0 );
    fdbDetachedFilename.top = new FormAttachment( wuseDetachedSignature, 0 );
    wbDetachedFilename.setLayoutData( fdbDetachedFilename );

    wDetachedFilename = new TextVar( workflowMeta, wSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wDetachedFilename );
    wDetachedFilename.addModifyListener( lsMod );
    fdDetachedFilename = new FormData();
    fdDetachedFilename.left = new FormAttachment( middle, 0 );
    fdDetachedFilename.top = new FormAttachment( wuseDetachedSignature, margin );
    fdDetachedFilename.right = new FormAttachment( wbDetachedFilename, -margin );
    wDetachedFilename.setLayoutData( fdDetachedFilename );

    // Whenever something changes, set the tooltip to the expanded version:
    wDetachedFilename.addModifyListener( new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        wDetachedFilename.setToolTipText( workflowMeta.environmentSubstitute( wDetachedFilename.getText() ) );
      }
    } );

    wbDetachedFilename.addListener( SWT.Selection, e->  BaseDialog.presentFileDialog( shell, wDetachedFilename, workflowMeta, EXTENSIONS, FILETYPES, false ) );

    // Whenever something changes, set the tooltip to the expanded version:
    wFilename.addModifyListener( e -> wFilename.setToolTipText( workflowMeta.environmentSubstitute( wFilename.getText() ) ) );
    wbFilename.addListener( SWT.Selection, e-> BaseDialog.presentFileDialog( shell, wFilename, workflowMeta, EXTENSIONS, FILETYPES, false ) );

    // Whenever something changes, set the tooltip to the expanded version:
    wGPGLocation.addModifyListener( e -> wGPGLocation.setToolTipText( workflowMeta.environmentSubstitute( wGPGLocation.getText() ) ) );
    wbGPGLocation.addListener( SWT.Selection, e-> BaseDialog.presentFileDialog( shell, wGPGLocation, workflowMeta, EXTENSIONS, FILETYPES, false ) );
    
    fdSettings = new FormData();
    fdSettings.left = new FormAttachment( 0, margin );
    fdSettings.top = new FormAttachment( wName, margin );
    fdSettings.right = new FormAttachment( 100, -margin );
    wSettings.setLayoutData( fdSettings );
    // ///////////////////////////////////////////////////////////
    // / END OF Advanced SETTINGS GROUP
    // ///////////////////////////////////////////////////////////

    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    FormData fd = new FormData();
    fd.right = new FormAttachment( 50, -10 );
    fd.bottom = new FormAttachment( 100, 0 );
    fd.width = 100;
    wOk.setLayoutData( fd );

    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    fd = new FormData();
    fd.left = new FormAttachment( 50, 10 );
    fd.bottom = new FormAttachment( 100, 0 );
    fd.width = 100;
    wCancel.setLayoutData( fd );

    BaseTransformDialog.positionBottomButtons( shell, new Button[] { wOk, wCancel }, margin, wSettings );
    // Add listeners
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };
    lsOk = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };

    wCancel.addListener( SWT.Selection, lsCancel );
    wOk.addListener( SWT.Selection, lsOk );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wName.addSelectionListener( lsDef );
    wGPGLocation.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();
    enableDetachedSignature();
    BaseTransformDialog.setSize( shell );

    shell.open();
    props.setDialogSize( shell, "JobPGPVerifyDialogSize" );
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return action;
  }

  public void dispose() {
    WindowProperty winprop = new WindowProperty( shell );
    props.setScreen( winprop );
    shell.dispose();
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    wName.setText( Const.nullToEmpty( action.getName() ) );
    if ( action.getGPGLocation() != null ) {
      wGPGLocation.setText( action.getGPGLocation() );
    }
    if ( action.getFilename() != null ) {
      wFilename.setText( action.getFilename() );
    }
    if ( action.getDetachedfilename() != null ) {
      wDetachedFilename.setText( action.getDetachedfilename() );
    }
    wuseDetachedSignature.setSelection( action.useDetachedfilename() );

    wName.selectAll();
    wName.setFocus();
  }

  private void cancel() {
    action.setChanged( changed );
    action = null;
    dispose();
  }

  private void ok() {
    if ( Utils.isEmpty( wName.getText() ) ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setText( BaseMessages.getString( PKG, "System.TransformActionNameMissing.Title" ) );
      mb.setMessage( BaseMessages.getString( PKG, "System.ActionNameMissing.Msg" ) );
      mb.open();
      return;
    }
    action.setName( wName.getText() );
    action.setGPGLocation( wGPGLocation.getText() );
    action.setFilename( wFilename.getText() );
    action.setDetachedfilename( wDetachedFilename.getText() );
    action.setUseDetachedfilename( wuseDetachedSignature.getSelection() );
    dispose();
  }

  private void enableDetachedSignature() {
    wlDetachedFilename.setEnabled( wuseDetachedSignature.getSelection() );
    wDetachedFilename.setEnabled( wuseDetachedSignature.getSelection() );
    wbDetachedFilename.setEnabled( wuseDetachedSignature.getSelection() );
  }

  public boolean evaluates() {
    return true;
  }

  public boolean isUnconditional() {
    return false;
  }

}
