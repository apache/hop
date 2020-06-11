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

package org.apache.hop.workflow.actions.createfolder;

import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.PluginDialog;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.hopgui.HopGuiExtensionPoint;
import org.apache.hop.ui.hopgui.delegates.HopGuiDirectoryDialogExtension;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.IActionDialog;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
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
import org.eclipse.swt.widgets.DirectoryDialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This dialog allows you to edit the Create Folder action settings.
 *
 * @author Sven/Samatar
 * @since 17-10-2007
 */
@PluginDialog( 
		  id = "CREATE_FOLDER", 
		  image = "CreateFolder.svg", 
		  pluginType = PluginDialog.PluginType.ACTION,
		  documentationUrl = "https://www.project-hop.org/manual/latest/plugins/actions/createfolder.html"
)
public class ActionCreateFolderDialog extends ActionDialog implements IActionDialog {
  private static Class<?> PKG = ActionCreateFolder.class; // for i18n purposes, needed by Translator!!

  private Label wlName;
  private Text wName;
  private FormData fdlName, fdName;

  private Label wlFoldername;
  private Button wbFoldername;
  private TextVar wFoldername;
  private FormData fdlFoldername, fdbFoldername, fdFoldername;

  private Label wlAbortExists;
  private Button wAbortExists;
  private FormData fdlAbortExists, fdAbortExists;

  private Button wOk, wCancel;
  private Listener lsOk, lsCancel;

  private ActionCreateFolder action;
  private Shell shell;

  private SelectionAdapter lsDef;

  private boolean changed;

  public ActionCreateFolderDialog( Shell parent, IAction action, WorkflowMeta workflowMeta ) {
    super( parent, action, workflowMeta );
    this.action = (ActionCreateFolder) action;
    if ( this.action.getName() == null ) {
      this.action.setName( BaseMessages.getString( PKG, "JobCreateFolder.Name.Default" ) );
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
    shell.setText( BaseMessages.getString( PKG, "JobCreateFolder.Title" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Foldername line
    wlName = new Label( shell, SWT.RIGHT );
    wlName.setText( BaseMessages.getString( PKG, "JobCreateFolder.Name.Label" ) );
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

    // Foldername line
    wlFoldername = new Label( shell, SWT.RIGHT );
    wlFoldername.setText( BaseMessages.getString( PKG, "JobCreateFolder.Foldername.Label" ) );
    props.setLook( wlFoldername );
    fdlFoldername = new FormData();
    fdlFoldername.left = new FormAttachment( 0, 0 );
    fdlFoldername.top = new FormAttachment( wName, margin );
    fdlFoldername.right = new FormAttachment( middle, -margin );
    wlFoldername.setLayoutData( fdlFoldername );

    wbFoldername = new Button( shell, SWT.PUSH | SWT.CENTER );
    props.setLook( wbFoldername );
    wbFoldername.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    fdbFoldername = new FormData();
    fdbFoldername.right = new FormAttachment( 100, 0 );
    fdbFoldername.top = new FormAttachment( wName, 0 );
    wbFoldername.setLayoutData( fdbFoldername );

    wFoldername = new TextVar( workflowMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFoldername );
    wFoldername.addModifyListener( lsMod );
    fdFoldername = new FormData();
    fdFoldername.left = new FormAttachment( middle, 0 );
    fdFoldername.top = new FormAttachment( wName, margin );
    fdFoldername.right = new FormAttachment( wbFoldername, -margin );
    wFoldername.setLayoutData( fdFoldername );

    // Whenever something changes, set the tooltip to the expanded version:
    wFoldername.addModifyListener( e -> wFoldername.setToolTipText( workflowMeta.environmentSubstitute( wFoldername.getText() ) ) );

    wbFoldername.addListener( SWT.Selection, e -> BaseDialog.presentDirectoryDialog( shell, wFoldername, workflowMeta ) );

    wlAbortExists = new Label( shell, SWT.RIGHT );
    wlAbortExists.setText( BaseMessages.getString( PKG, "JobCreateFolder.FailIfExists.Label" ) );
    props.setLook( wlAbortExists );
    fdlAbortExists = new FormData();
    fdlAbortExists.left = new FormAttachment( 0, 0 );
    fdlAbortExists.top = new FormAttachment( wFoldername, margin );
    fdlAbortExists.right = new FormAttachment( middle, -margin );
    wlAbortExists.setLayoutData( fdlAbortExists );
    wAbortExists = new Button( shell, SWT.CHECK );
    props.setLook( wAbortExists );
    wAbortExists.setToolTipText( BaseMessages.getString( PKG, "JobCreateFolder.FailIfExists.Tooltip" ) );
    fdAbortExists = new FormData();
    fdAbortExists.left = new FormAttachment( middle, 0 );
    fdAbortExists.top = new FormAttachment( wFoldername, margin );
    fdAbortExists.right = new FormAttachment( 100, 0 );
    wAbortExists.setLayoutData( fdAbortExists );
    wAbortExists.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
      }
    } );

    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    BaseTransformDialog.positionBottomButtons( shell, new Button[] { wOk, wCancel }, margin, wAbortExists );

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
    wFoldername.addSelectionListener( lsDef );

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
    if ( action.getName() != null ) {
      wName.setText( action.getName() );
    }
    if ( action.getFoldername() != null ) {
      wFoldername.setText( action.getFoldername() );
    }
    wAbortExists.setSelection( action.isFailOfFolderExists() );

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
    action.setFoldername( wFoldername.getText() );
    action.setFailOfFolderExists( wAbortExists.getSelection() );
    dispose();
  }

  public boolean evaluates() {
    return true;
  }

  public boolean isUnconditional() {
    return false;
  }
}
