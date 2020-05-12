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

package org.apache.hop.workflow.actions.waitforfile;

import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.PluginDialog;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.IActionDialog;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
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
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

/**
 * This dialog allows you to edit the Wait For File action settings.
 *
 * @author Sven Boden
 * @since 28-01-2007
 */
@PluginDialog( 
		  id = "WAIT_FOR_FILE", 
		  image = "WaitForFile.svg", 
		  pluginType = PluginDialog.PluginType.ACTION,
		  documentationUrl = "https://www.project-hop.org/manual/latest/plugins/actions/"
)
public class ActionWaitForFileDialog extends ActionDialog implements IActionDialog {
  private static Class<?> PKG = ActionWaitForFile.class; // for i18n purposes, needed by Translator!!

  private static final String[] FILETYPES = new String[] { BaseMessages.getString(
    PKG, "JobWaitForFile.Filetype.All" ) };

  private Label wlName;
  private Text wName;
  private FormData fdlName, fdName;

  private Label wlFilename;
  private Button wbFilename;
  private TextVar wFilename;
  private FormData fdlFilename, fdbFilename, fdFilename;

  private Label wlMaximumTimeout;
  private TextVar wMaximumTimeout;
  private FormData fdlMaximumTimeout, fdMaximumTimeout;

  private Label wlCheckCycleTime;
  private TextVar wCheckCycleTime;
  private FormData fdlCheckCycleTime, fdCheckCycleTime;

  private Label wlSuccesOnTimeout;
  private Button wSuccesOnTimeout;
  private FormData fdlSuccesOnTimeout, fdSuccesOnTimeout;

  private Label wlFileSizeCheck;
  private Button wFileSizeCheck;
  private FormData fdlFileSizeCheck, fdFileSizeCheck;

  private Label wlAddFilenameResult;
  private Button wAddFilenameResult;
  private FormData fdlAddFilenameResult, fdAddFilenameResult;

  private Button wOk, wCancel;
  private Listener lsOk, lsCancel;

  private ActionWaitForFile action;
  private Shell shell;
  private SelectionAdapter lsDef;

  private boolean changed;

  public ActionWaitForFileDialog( Shell parent, IAction action, WorkflowMeta workflowMeta ) {
    super( parent, action, workflowMeta );
    this.action = (ActionWaitForFile) action;
    if ( this.action.getName() == null ) {
      this.action.setName( BaseMessages.getString( PKG, "JobWaitForFile.Name.Default" ) );
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
    shell.setText( BaseMessages.getString( PKG, "JobWaitForFile.Title" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Filename line
    wlName = new Label( shell, SWT.RIGHT );
    wlName.setText( BaseMessages.getString( PKG, "JobWaitForFile.Name.Label" ) );
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

    // Filename line
    wlFilename = new Label( shell, SWT.RIGHT );
    wlFilename.setText( BaseMessages.getString( PKG, "JobWaitForFile.Filename.Label" ) );
    props.setLook( wlFilename );
    fdlFilename = new FormData();
    fdlFilename.left = new FormAttachment( 0, 0 );
    fdlFilename.top = new FormAttachment( wName, margin );
    fdlFilename.right = new FormAttachment( middle, -margin );
    wlFilename.setLayoutData( fdlFilename );

    wbFilename = new Button( shell, SWT.PUSH | SWT.CENTER );
    props.setLook( wbFilename );
    wbFilename.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    fdbFilename = new FormData();
    fdbFilename.right = new FormAttachment( 100, 0 );
    fdbFilename.top = new FormAttachment( wName, 0 );
    wbFilename.setLayoutData( fdbFilename );

    wFilename = new TextVar( workflowMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFilename );
    wFilename.addModifyListener( lsMod );
    fdFilename = new FormData();
    fdFilename.left = new FormAttachment( middle, 0 );
    fdFilename.top = new FormAttachment( wName, margin );
    fdFilename.right = new FormAttachment( wbFilename, -margin );
    wFilename.setLayoutData( fdFilename );

    // Whenever something changes, set the tooltip to the expanded version:
    wFilename.addModifyListener( e -> wFilename.setToolTipText( workflowMeta.environmentSubstitute( wFilename.getText() ) ) );

    wbFilename.addListener( SWT.Selection, e-> BaseDialog.presentFileDialog( shell, wFilename, workflowMeta,
      new String[] { "*" }, FILETYPES, true )
    );

    // Maximum timeout
    wlMaximumTimeout = new Label( shell, SWT.RIGHT );
    wlMaximumTimeout.setText( BaseMessages.getString( PKG, "JobWaitForFile.MaximumTimeout.Label" ) );
    props.setLook( wlMaximumTimeout );
    fdlMaximumTimeout = new FormData();
    fdlMaximumTimeout.left = new FormAttachment( 0, 0 );
    fdlMaximumTimeout.top = new FormAttachment( wFilename, margin );
    fdlMaximumTimeout.right = new FormAttachment( middle, -margin );
    wlMaximumTimeout.setLayoutData( fdlMaximumTimeout );
    wMaximumTimeout = new TextVar( workflowMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wMaximumTimeout );
    wMaximumTimeout.setToolTipText( BaseMessages.getString( PKG, "JobWaitForFile.MaximumTimeout.Tooltip" ) );
    wMaximumTimeout.addModifyListener( lsMod );
    fdMaximumTimeout = new FormData();
    fdMaximumTimeout.left = new FormAttachment( middle, 0 );
    fdMaximumTimeout.top = new FormAttachment( wFilename, margin );
    fdMaximumTimeout.right = new FormAttachment( 100, 0 );
    wMaximumTimeout.setLayoutData( fdMaximumTimeout );

    // Cycle time
    wlCheckCycleTime = new Label( shell, SWT.RIGHT );
    wlCheckCycleTime.setText( BaseMessages.getString( PKG, "JobWaitForFile.CheckCycleTime.Label" ) );
    props.setLook( wlCheckCycleTime );
    fdlCheckCycleTime = new FormData();
    fdlCheckCycleTime.left = new FormAttachment( 0, 0 );
    fdlCheckCycleTime.top = new FormAttachment( wMaximumTimeout, margin );
    fdlCheckCycleTime.right = new FormAttachment( middle, -margin );
    wlCheckCycleTime.setLayoutData( fdlCheckCycleTime );
    wCheckCycleTime = new TextVar( workflowMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wCheckCycleTime );
    wCheckCycleTime.setToolTipText( BaseMessages.getString( PKG, "JobWaitForFile.CheckCycleTime.Tooltip" ) );
    wCheckCycleTime.addModifyListener( lsMod );
    fdCheckCycleTime = new FormData();
    fdCheckCycleTime.left = new FormAttachment( middle, 0 );
    fdCheckCycleTime.top = new FormAttachment( wMaximumTimeout, margin );
    fdCheckCycleTime.right = new FormAttachment( 100, 0 );
    wCheckCycleTime.setLayoutData( fdCheckCycleTime );

    // Success on timeout
    wlSuccesOnTimeout = new Label( shell, SWT.RIGHT );
    wlSuccesOnTimeout.setText( BaseMessages.getString( PKG, "JobWaitForFile.SuccessOnTimeout.Label" ) );
    props.setLook( wlSuccesOnTimeout );
    fdlSuccesOnTimeout = new FormData();
    fdlSuccesOnTimeout.left = new FormAttachment( 0, 0 );
    fdlSuccesOnTimeout.top = new FormAttachment( wCheckCycleTime, margin );
    fdlSuccesOnTimeout.right = new FormAttachment( middle, -margin );
    wlSuccesOnTimeout.setLayoutData( fdlSuccesOnTimeout );
    wSuccesOnTimeout = new Button( shell, SWT.CHECK );
    props.setLook( wSuccesOnTimeout );
    wSuccesOnTimeout.setToolTipText( BaseMessages.getString( PKG, "JobWaitForFile.SuccessOnTimeout.Tooltip" ) );
    fdSuccesOnTimeout = new FormData();
    fdSuccesOnTimeout.left = new FormAttachment( middle, 0 );
    fdSuccesOnTimeout.top = new FormAttachment( wCheckCycleTime, margin );
    fdSuccesOnTimeout.right = new FormAttachment( 100, 0 );
    wSuccesOnTimeout.setLayoutData( fdSuccesOnTimeout );
    wSuccesOnTimeout.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
      }
    } );

    // Check file size
    wlFileSizeCheck = new Label( shell, SWT.RIGHT );
    wlFileSizeCheck.setText( BaseMessages.getString( PKG, "JobWaitForFile.FileSizeCheck.Label" ) );
    props.setLook( wlFileSizeCheck );
    fdlFileSizeCheck = new FormData();
    fdlFileSizeCheck.left = new FormAttachment( 0, 0 );
    fdlFileSizeCheck.top = new FormAttachment( wSuccesOnTimeout, margin );
    fdlFileSizeCheck.right = new FormAttachment( middle, -margin );
    wlFileSizeCheck.setLayoutData( fdlFileSizeCheck );
    wFileSizeCheck = new Button( shell, SWT.CHECK );
    props.setLook( wFileSizeCheck );
    wFileSizeCheck.setToolTipText( BaseMessages.getString( PKG, "JobWaitForFile.FileSizeCheck.Tooltip" ) );
    fdFileSizeCheck = new FormData();
    fdFileSizeCheck.left = new FormAttachment( middle, 0 );
    fdFileSizeCheck.top = new FormAttachment( wSuccesOnTimeout, margin );
    fdFileSizeCheck.right = new FormAttachment( 100, 0 );
    wFileSizeCheck.setLayoutData( fdFileSizeCheck );
    wFileSizeCheck.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
      }
    } );
    // Add filename to result filenames
    wlAddFilenameResult = new Label( shell, SWT.RIGHT );
    wlAddFilenameResult.setText( BaseMessages.getString( PKG, "JobWaitForFile.AddFilenameResult.Label" ) );
    props.setLook( wlAddFilenameResult );
    fdlAddFilenameResult = new FormData();
    fdlAddFilenameResult.left = new FormAttachment( 0, 0 );
    fdlAddFilenameResult.top = new FormAttachment( wFileSizeCheck, margin );
    fdlAddFilenameResult.right = new FormAttachment( middle, -margin );
    wlAddFilenameResult.setLayoutData( fdlAddFilenameResult );
    wAddFilenameResult = new Button( shell, SWT.CHECK );
    props.setLook( wAddFilenameResult );
    wAddFilenameResult.setToolTipText( BaseMessages.getString( PKG, "JobWaitForFile.AddFilenameResult.Tooltip" ) );
    fdAddFilenameResult = new FormData();
    fdAddFilenameResult.left = new FormAttachment( middle, 0 );
    fdAddFilenameResult.top = new FormAttachment( wFileSizeCheck, margin );
    fdAddFilenameResult.right = new FormAttachment( 100, 0 );
    wAddFilenameResult.setLayoutData( fdAddFilenameResult );
    wAddFilenameResult.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
      }
    } );
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    BaseTransformDialog.positionBottomButtons( shell, new Button[] { wOk, wCancel }, margin, wAddFilenameResult );

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
    wFilename.addSelectionListener( lsDef );
    wMaximumTimeout.addSelectionListener( lsDef );
    wCheckCycleTime.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();

    BaseTransformDialog.setSize( shell );

    shell.open();
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
    wFilename.setText( Const.NVL( action.getFilename(), "" ) );
    wMaximumTimeout.setText( Const.NVL( action.getMaximumTimeout(), "" ) );
    wCheckCycleTime.setText( Const.NVL( action.getCheckCycleTime(), "" ) );
    wSuccesOnTimeout.setSelection( action.isSuccessOnTimeout() );
    wFileSizeCheck.setSelection( action.isFileSizeCheck() );
    wAddFilenameResult.setSelection( action.isAddFilenameToResult() );

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
    action.setFilename( wFilename.getText() );
    action.setMaximumTimeout( wMaximumTimeout.getText() );
    action.setCheckCycleTime( wCheckCycleTime.getText() );
    action.setSuccessOnTimeout( wSuccesOnTimeout.getSelection() );
    action.setFileSizeCheck( wFileSizeCheck.getSelection() );
    action.setAddFilenameToResult( wAddFilenameResult.getSelection() );
    dispose();
  }

  public boolean evaluates() {
    return true;
  }

  public boolean isUnconditional() {
    return false;
  }
}
