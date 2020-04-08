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

package org.apache.hop.workflow.actions.msgboxinfo;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.annotations.PluginDialog;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IActionDialog;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ControlSpaceKeyAdapter;
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
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

/**
 * This dialog allows you to edit a JobEntryEval object.
 *
 * @author Matt
 * @since 19-06-2003
 */
@PluginDialog( 
		  id = "MSGBOX_INFO", 
		  image = "MsgBoxInfo.svg", 
		  pluginType = PluginDialog.PluginType.ACTION,
		  documentationUrl = "https://www.project-hop.org/manual/latest/plugins/actions/"
)
public class ActionMsgBoxInfoDialog extends ActionDialog implements IActionDialog {
  private static Class<?> PKG = ActionMsgBoxInfo.class; // for i18n purposes, needed by Translator!!

  private Label wlName;

  private Text wName;

  private FormData fdlName, fdName;

  private Label wlBodyMessage;

  private TextVar wBodyMessage;

  private FormData fdlBodyMessage, fdBodyMessage;

  private Button wOk, wCancel;

  private Listener lsOk, lsCancel;

  private ActionMsgBoxInfo jobEntry;

  private Shell shell;

  private SelectionAdapter lsDef;

  private boolean changed;

  // TitleMessage
  private Label wlTitleMessage;

  private TextVar wTitleMessage;

  private FormData fdlTitleMessage, fdTitleMessage;

  public ActionMsgBoxInfoDialog( Shell parent, IAction jobEntryInt, WorkflowMeta workflowMeta ) {
    super( parent, jobEntryInt, workflowMeta );
    jobEntry = (ActionMsgBoxInfo) jobEntryInt;
    if ( this.jobEntry.getName() == null ) {
      this.jobEntry.setName( BaseMessages.getString( PKG, "MsgBoxInfo.Name.Default" ) );
    }
  }

  public IAction open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, props.getWorkflowsDialogStyle() );
    props.setLook( shell );
    WorkflowDialog.setShellImage( shell, jobEntry );

    ModifyListener lsMod = new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        jobEntry.setChanged();
      }
    };
    changed = jobEntry.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "MsgBoxInfo.Title" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    // at the bottom
    BaseTransformDialog.positionBottomButtons( shell, new Button[] { wOk, wCancel }, margin, null );

    // Filename line
    wlName = new Label( shell, SWT.RIGHT );
    wlName.setText( BaseMessages.getString( PKG, "MsgBoxInfo.Label" ) );
    props.setLook( wlName );
    fdlName = new FormData();
    fdlName.left = new FormAttachment( 0, 0 );
    fdlName.right = new FormAttachment( middle, 0 );
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

    // Title Msgbox
    wlTitleMessage = new Label( shell, SWT.RIGHT );
    wlTitleMessage.setText( BaseMessages.getString( PKG, "MsgBoxInfo.TitleMessage.Label" ) );
    props.setLook( wlTitleMessage );
    fdlTitleMessage = new FormData();
    fdlTitleMessage.left = new FormAttachment( 0, 0 );
    fdlTitleMessage.top = new FormAttachment( wName, margin );
    fdlTitleMessage.right = new FormAttachment( middle, -margin );
    wlTitleMessage.setLayoutData( fdlTitleMessage );

    wTitleMessage = new TextVar( workflowMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wTitleMessage );
    wTitleMessage.addModifyListener( lsMod );
    fdTitleMessage = new FormData();
    fdTitleMessage.left = new FormAttachment( middle, 0 );
    fdTitleMessage.top = new FormAttachment( wName, margin );
    fdTitleMessage.right = new FormAttachment( 100, 0 );
    wTitleMessage.setLayoutData( fdTitleMessage );

    // Body Msgbox
    wlBodyMessage = new Label( shell, SWT.RIGHT );
    wlBodyMessage.setText( BaseMessages.getString( PKG, "MsgBoxInfo.BodyMessage.Label" ) );
    props.setLook( wlBodyMessage );
    fdlBodyMessage = new FormData();
    fdlBodyMessage.left = new FormAttachment( 0, 0 );
    fdlBodyMessage.top = new FormAttachment( wTitleMessage, margin );
    fdlBodyMessage.right = new FormAttachment( middle, -margin );
    wlBodyMessage.setLayoutData( fdlBodyMessage );

    wBodyMessage = new TextVar( workflowMeta, shell, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL );
    wBodyMessage.setText( BaseMessages.getString( PKG, "MsgBoxInfo.Name.Default" ) );
    props.setLook( wBodyMessage, Props.WIDGET_STYLE_FIXED );
    wBodyMessage.addModifyListener( lsMod );
    fdBodyMessage = new FormData();
    fdBodyMessage.left = new FormAttachment( middle, 0 );
    fdBodyMessage.top = new FormAttachment( wTitleMessage, margin );
    fdBodyMessage.right = new FormAttachment( 100, 0 );
    fdBodyMessage.bottom = new FormAttachment( wOk, -margin );
    wBodyMessage.setLayoutData( fdBodyMessage );

    // SelectionAdapter lsVar = VariableButtonListenerFactory.getSelectionAdapter(shell, wBodyMessage, workflowMeta);
    wBodyMessage.addKeyListener( new ControlSpaceKeyAdapter( workflowMeta, wBodyMessage ) );

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

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();

    BaseTransformDialog.setSize( shell, 250, 250, false );

    shell.open();
    props.setDialogSize( shell, "JobEvalDialogSize" );
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return jobEntry;
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
    wName.setText( Const.NVL( jobEntry.getName(), "" ) );
    wBodyMessage.setText( Const.NVL( jobEntry.getBodyMessage(), "" ) );
    wTitleMessage.setText( Const.NVL( jobEntry.getTitleMessage(), "" ) );

    wName.selectAll();
    wName.setFocus();
  }

  private void cancel() {
    jobEntry.setChanged( changed );
    jobEntry = null;
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
    jobEntry.setName( wName.getText() );
    jobEntry.setTitleMessage( wTitleMessage.getText() );
    jobEntry.setBodyMessage( wBodyMessage.getText() );
    dispose();
  }
}
