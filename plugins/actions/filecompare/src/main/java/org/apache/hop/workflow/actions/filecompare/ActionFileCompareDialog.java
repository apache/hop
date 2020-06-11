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

package org.apache.hop.workflow.actions.filecompare;

import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.PluginDialog;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.dialog.BaseDialog;
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
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

/**
 * This dialog allows you to edit the File compare action settings.
 *
 * @author Sven Boden
 * @since 01-02-2007
 */
@PluginDialog( 
		  id = "FILE_COMPARE", 
		  image = "FileCompare.svg", 
		  pluginType = PluginDialog.PluginType.ACTION,
		  documentationUrl = "https://www.project-hop.org/manual/latest/plugins/actions/filecompare.html"
)
public class ActionFileCompareDialog extends ActionDialog implements IActionDialog {
  private static Class<?> PKG = ActionFileCompare.class; // for i18n purposes, needed by Translator!!

  private static final String[] FILETYPES = new String[] { BaseMessages.getString(
    PKG, "JobFileCompare.Filetype.All" ) };

  private Label wlName;
  private Text wName;
  private FormData fdlName, fdName;

  private Label wlFilename1;
  private Button wbFilename1;
  private TextVar wFilename1;
  private FormData fdlFilename1, fdbFilename1, fdFilename1;

  private Label wlFilename2;
  private Button wbFilename2;
  private TextVar wFilename2;
  private FormData fdlFilename2, fdbFilename2, fdFilename2;

  private Button wOk, wCancel;
  private Listener lsOk, lsCancel;

  private ActionFileCompare action;
  private Shell shell;
  private Label wlAddFilenameResult;
  private Button wAddFilenameResult;
  private FormData fdlAddFilenameResult, fdAddFilenameResult;

  private SelectionAdapter lsDef;

  private boolean changed;

  public ActionFileCompareDialog( Shell parent, IAction action, WorkflowMeta workflowMeta ) {
    super( parent, action, workflowMeta );
    this.action = (ActionFileCompare) action;
    if ( this.action.getName() == null ) {
      this.action.setName( BaseMessages.getString( PKG, "JobFileCompare.Name.Default" ) );
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
    shell.setText( BaseMessages.getString( PKG, "JobFileCompare.Title" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Name line
    wlName = new Label( shell, SWT.RIGHT );
    wlName.setText( BaseMessages.getString( PKG, "JobFileCompare.Name.Label" ) );
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

    // Filename 1 line
    wlFilename1 = new Label( shell, SWT.RIGHT );
    wlFilename1.setText( BaseMessages.getString( PKG, "JobFileCompare.Filename1.Label" ) );
    props.setLook( wlFilename1 );
    fdlFilename1 = new FormData();
    fdlFilename1.left = new FormAttachment( 0, 0 );
    fdlFilename1.top = new FormAttachment( wName, margin );
    fdlFilename1.right = new FormAttachment( middle, -margin );
    wlFilename1.setLayoutData( fdlFilename1 );
    wbFilename1 = new Button( shell, SWT.PUSH | SWT.CENTER );
    props.setLook( wbFilename1 );
    wbFilename1.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    fdbFilename1 = new FormData();
    fdbFilename1.right = new FormAttachment( 100, 0 );
    fdbFilename1.top = new FormAttachment( wName, 0 );
    wbFilename1.setLayoutData( fdbFilename1 );
    wFilename1 = new TextVar( workflowMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFilename1 );
    wFilename1.addModifyListener( lsMod );
    fdFilename1 = new FormData();
    fdFilename1.left = new FormAttachment( middle, 0 );
    fdFilename1.top = new FormAttachment( wName, margin );
    fdFilename1.right = new FormAttachment( wbFilename1, -margin );
    wFilename1.setLayoutData( fdFilename1 );

    // Whenever something changes, set the tooltip to the expanded version:
    wFilename1.addModifyListener( e -> wFilename1.setToolTipText( workflowMeta.environmentSubstitute( wFilename1.getText() ) ) );

    wbFilename1.addListener( SWT.Selection, e-> BaseDialog.presentFileDialog( shell, wFilename1, workflowMeta,
      new String[] { "*" }, FILETYPES, true )
    );

    // Filename 2 line
    wlFilename2 = new Label( shell, SWT.RIGHT );
    wlFilename2.setText( BaseMessages.getString( PKG, "JobFileCompare.Filename2.Label" ) );
    props.setLook( wlFilename2 );
    fdlFilename2 = new FormData();
    fdlFilename2.left = new FormAttachment( 0, 0 );
    fdlFilename2.top = new FormAttachment( wFilename1, margin );
    fdlFilename2.right = new FormAttachment( middle, -margin );
    wlFilename2.setLayoutData( fdlFilename2 );
    wbFilename2 = new Button( shell, SWT.PUSH | SWT.CENTER );
    props.setLook( wbFilename2 );
    wbFilename2.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    fdbFilename2 = new FormData();
    fdbFilename2.right = new FormAttachment( 100, 0 );
    fdbFilename2.top = new FormAttachment( wFilename1, 0 );
    wbFilename2.setLayoutData( fdbFilename2 );
    wFilename2 = new TextVar( workflowMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFilename2 );
    wFilename2.addModifyListener( lsMod );
    fdFilename2 = new FormData();
    fdFilename2.left = new FormAttachment( middle, 0 );
    fdFilename2.top = new FormAttachment( wFilename1, margin );
    fdFilename2.right = new FormAttachment( wbFilename2, -margin );
    wFilename2.setLayoutData( fdFilename2 );

    // Whenever something changes, set the tooltip to the expanded version:
    wFilename2.addModifyListener( e -> wFilename2.setToolTipText( workflowMeta.environmentSubstitute( wFilename2.getText() ) ) );

    wbFilename2.addListener( SWT.Selection, e-> BaseDialog.presentFileDialog( shell, wFilename2, workflowMeta,
      new String[] { "*" }, FILETYPES, true )
    );

    // Add filename to result filenames
    wlAddFilenameResult = new Label( shell, SWT.RIGHT );
    wlAddFilenameResult.setText( BaseMessages.getString( PKG, "JobFileCompare.AddFilenameResult.Label" ) );
    props.setLook( wlAddFilenameResult );
    fdlAddFilenameResult = new FormData();
    fdlAddFilenameResult.left = new FormAttachment( 0, 0 );
    fdlAddFilenameResult.top = new FormAttachment( wbFilename2, margin );
    fdlAddFilenameResult.right = new FormAttachment( middle, -margin );
    wlAddFilenameResult.setLayoutData( fdlAddFilenameResult );
    wAddFilenameResult = new Button( shell, SWT.CHECK );
    props.setLook( wAddFilenameResult );
    wAddFilenameResult.setToolTipText( BaseMessages.getString( PKG, "JobFileCompare.AddFilenameResult.Tooltip" ) );
    fdAddFilenameResult = new FormData();
    fdAddFilenameResult.left = new FormAttachment( middle, 0 );
    fdAddFilenameResult.top = new FormAttachment( wbFilename2, margin );
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
    wFilename1.addSelectionListener( lsDef );
    wFilename2.addSelectionListener( lsDef );

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
    if ( action.getFilename1() != null ) {
      wFilename1.setText( action.getFilename1() );
    }
    if ( action.getFilename2() != null ) {
      wFilename2.setText( action.getFilename2() );
    }
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
    action.setFilename1( wFilename1.getText() );
    action.setFilename2( wFilename2.getText() );
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
