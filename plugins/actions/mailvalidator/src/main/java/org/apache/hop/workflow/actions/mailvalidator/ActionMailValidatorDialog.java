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

package org.apache.hop.workflow.actions.mailvalidator;

import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.PluginDialog;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IActionDialog;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.LabelTextVar;
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
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

/**
 * This dialog allows you to edit a ActionMailValidator
 *
 * @author Samatar
 * @since 23-06-2008
 */
@PluginDialog( 
		  id = "MAIL_VALIDATOR", 
		  image = "MailValidator.svg", 
		  pluginType = PluginDialog.PluginType.ACTION,
		  documentationUrl = "https://www.project-hop.org/manual/latest/plugins/actions/"
)
public class ActionMailValidatorDialog extends ActionDialog implements IActionDialog {
  private static Class<?> PKG = ActionMailValidator.class; // for i18n purposes, needed by Translator!!

  private Label wlName;

  private Text wName;

  private FormData fdlName, fdName;

  private Button wOk, wCancel;

  private Listener lsOk, lsCancel;

  private ActionMailValidator action;

  private Shell shell;

  private SelectionAdapter lsDef;

  private boolean changed;

  private LabelTextVar wMailAddress;

  private FormData fdMailAddress;

  private Display display;

  private Group wSettingsGroup;
  private FormData fdSettingsGroup;

  private Label wleMailSender;
  private TextVar weMailSender;
  private FormData fdleMailSender, fdeMailSender;

  private Label wlTimeOut;
  private TextVar wTimeOut;
  private FormData fdlTimeOut, fdTimeOut;

  private Label wlDefaultSMTP;
  private TextVar wDefaultSMTP;
  private FormData fdlDefaultSMTP, fdDefaultSMTP;

  private Label wlSMTPCheck;
  private FormData fdlSMTPCheck;
  private Button wSMTPCheck;
  private FormData fdSMTPCheck;

  public ActionMailValidatorDialog( Shell parent, IAction action, WorkflowMeta workflowMeta ) {
    super( parent, action, workflowMeta );
    this.action = (ActionMailValidator) action;

    if ( this.action.getName() == null ) {
      this.action.setName( BaseMessages.getString( PKG, "ActionMailValidatorDialog.Name.Default" ) );
    }
  }

  public IAction open() {
    Shell parent = getParent();
    display = parent.getDisplay();

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
    shell.setText( BaseMessages.getString( PKG, "ActionMailValidatorDialog.Title" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Filename line
    wlName = new Label( shell, SWT.RIGHT );
    wlName.setText( BaseMessages.getString( PKG, "ActionMailValidatorDialog.Label" ) );
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
    fdName.left = new FormAttachment( middle, margin );
    fdName.top = new FormAttachment( 0, margin );
    fdName.right = new FormAttachment( 100, 0 );
    wName.setLayoutData( fdName );

    // eMail address
    wMailAddress = new LabelTextVar( workflowMeta, shell,
      BaseMessages.getString( PKG, "ActionMailValidatorDialog.MailAddress.Label" ),
      BaseMessages.getString( PKG, "ActionMailValidatorDialog.MailAddress.Tooltip" ) );
    wMailAddress.addModifyListener( lsMod );
    fdMailAddress = new FormData();
    fdMailAddress.left = new FormAttachment( 0, 0 );
    fdMailAddress.top = new FormAttachment( wName, margin );
    fdMailAddress.right = new FormAttachment( 100, 0 );
    wMailAddress.setLayoutData( fdMailAddress );

    // ////////////////////////
    // START OF Settings GROUP
    // ////////////////////////

    wSettingsGroup = new Group( shell, SWT.SHADOW_NONE );
    props.setLook( wSettingsGroup );
    wSettingsGroup.setText( BaseMessages
      .getString( PKG, "ActionMailValidatorDialog.Group.SettingsAddress.Label" ) );

    FormLayout SettingsgroupLayout = new FormLayout();
    SettingsgroupLayout.marginWidth = 10;
    SettingsgroupLayout.marginHeight = 10;
    wSettingsGroup.setLayout( SettingsgroupLayout );

    // perform SMTP check?
    wlSMTPCheck = new Label( wSettingsGroup, SWT.RIGHT );
    wlSMTPCheck.setText( BaseMessages.getString( PKG, "ActionMailValidatorDialog.SMTPCheck.Label" ) );
    props.setLook( wlSMTPCheck );
    fdlSMTPCheck = new FormData();
    fdlSMTPCheck.left = new FormAttachment( 0, 0 );
    fdlSMTPCheck.top = new FormAttachment( wMailAddress, margin );
    fdlSMTPCheck.right = new FormAttachment( middle, -2 * margin );
    wlSMTPCheck.setLayoutData( fdlSMTPCheck );
    wSMTPCheck = new Button( wSettingsGroup, SWT.CHECK );
    props.setLook( wSMTPCheck );
    wSMTPCheck.setToolTipText( BaseMessages.getString( PKG, "ActionMailValidatorDialog.SMTPCheck.Tooltip" ) );
    fdSMTPCheck = new FormData();
    fdSMTPCheck.left = new FormAttachment( middle, -margin );
    fdSMTPCheck.top = new FormAttachment( wMailAddress, margin );
    wSMTPCheck.setLayoutData( fdSMTPCheck );
    wSMTPCheck.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        activeSMTPCheck();
      }
    } );

    // TimeOut fieldname ...
    wlTimeOut = new Label( wSettingsGroup, SWT.RIGHT );
    wlTimeOut.setText( BaseMessages.getString( PKG, "ActionMailValidatorDialog.TimeOutField.Label" ) );
    props.setLook( wlTimeOut );
    fdlTimeOut = new FormData();
    fdlTimeOut.left = new FormAttachment( 0, 0 );
    fdlTimeOut.right = new FormAttachment( middle, -2 * margin );
    fdlTimeOut.top = new FormAttachment( wSMTPCheck, margin );
    wlTimeOut.setLayoutData( fdlTimeOut );

    wTimeOut = new TextVar( workflowMeta, wSettingsGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wTimeOut.setToolTipText( BaseMessages.getString( PKG, "ActionMailValidatorDialog.TimeOutField.Tooltip" ) );
    props.setLook( wTimeOut );
    wTimeOut.addModifyListener( lsMod );
    fdTimeOut = new FormData();
    fdTimeOut.left = new FormAttachment( middle, -margin );
    fdTimeOut.top = new FormAttachment( wSMTPCheck, margin );
    fdTimeOut.right = new FormAttachment( 100, 0 );
    wTimeOut.setLayoutData( fdTimeOut );

    // eMailSender fieldname ...
    wleMailSender = new Label( wSettingsGroup, SWT.RIGHT );
    wleMailSender.setText( BaseMessages.getString( PKG, "ActionMailValidatorDialog.eMailSenderField.Label" ) );
    props.setLook( wleMailSender );
    fdleMailSender = new FormData();
    fdleMailSender.left = new FormAttachment( 0, 0 );
    fdleMailSender.right = new FormAttachment( middle, -2 * margin );
    fdleMailSender.top = new FormAttachment( wTimeOut, margin );
    wleMailSender.setLayoutData( fdleMailSender );

    weMailSender = new TextVar( workflowMeta, wSettingsGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    weMailSender.setToolTipText( BaseMessages.getString(
      PKG, "ActionMailValidatorDialog.eMailSenderField.Tooltip" ) );
    props.setLook( weMailSender );
    weMailSender.addModifyListener( lsMod );
    fdeMailSender = new FormData();
    fdeMailSender.left = new FormAttachment( middle, -margin );
    fdeMailSender.top = new FormAttachment( wTimeOut, margin );
    fdeMailSender.right = new FormAttachment( 100, 0 );
    weMailSender.setLayoutData( fdeMailSender );

    // DefaultSMTP fieldname ...
    wlDefaultSMTP = new Label( wSettingsGroup, SWT.RIGHT );
    wlDefaultSMTP.setText( BaseMessages.getString( PKG, "ActionMailValidatorDialog.DefaultSMTPField.Label" ) );
    props.setLook( wlDefaultSMTP );
    fdlDefaultSMTP = new FormData();
    fdlDefaultSMTP.left = new FormAttachment( 0, 0 );
    fdlDefaultSMTP.right = new FormAttachment( middle, -2 * margin );
    fdlDefaultSMTP.top = new FormAttachment( weMailSender, margin );
    wlDefaultSMTP.setLayoutData( fdlDefaultSMTP );

    wDefaultSMTP = new TextVar( workflowMeta, wSettingsGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wDefaultSMTP.setToolTipText( BaseMessages.getString(
      PKG, "ActionMailValidatorDialog.DefaultSMTPField.Tooltip" ) );
    props.setLook( wDefaultSMTP );
    wDefaultSMTP.addModifyListener( lsMod );
    fdDefaultSMTP = new FormData();
    fdDefaultSMTP.left = new FormAttachment( middle, -margin );
    fdDefaultSMTP.top = new FormAttachment( weMailSender, margin );
    fdDefaultSMTP.right = new FormAttachment( 100, 0 );
    wDefaultSMTP.setLayoutData( fdDefaultSMTP );

    fdSettingsGroup = new FormData();
    fdSettingsGroup.left = new FormAttachment( 0, margin );
    fdSettingsGroup.top = new FormAttachment( wMailAddress, margin );
    fdSettingsGroup.right = new FormAttachment( 100, -margin );
    wSettingsGroup.setLayoutData( fdSettingsGroup );

    // ///////////////////////////////////////////////////////////
    // / END OF Settings GROUP
    // ///////////////////////////////////////////////////////////

    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    // at the bottom
    BaseTransformDialog.positionBottomButtons( shell, new Button[] { wOk, wCancel }, margin, wSettingsGroup );

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
    activeSMTPCheck();
    BaseTransformDialog.setSize( shell );

    shell.open();
    props.setDialogSize( shell, "JobSuccessDialogSize" );
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

  private void activeSMTPCheck() {
    wlTimeOut.setEnabled( wSMTPCheck.getSelection() );
    wTimeOut.setEnabled( wSMTPCheck.getSelection() );
    wlDefaultSMTP.setEnabled( wSMTPCheck.getSelection() );
    wDefaultSMTP.setEnabled( wSMTPCheck.getSelection() );
    wleMailSender.setEnabled( wSMTPCheck.getSelection() );
    weMailSender.setEnabled( wSMTPCheck.getSelection() );
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    wName.setText( Const.NVL( action.getName(), "" ) );
    wMailAddress.setText( Const.NVL( action.getEmailAddress(), "" ) );
    wTimeOut.setText( Const.NVL( action.getTimeOut(), "0" ) );
    wSMTPCheck.setSelection( action.isSMTPCheck() );
    wDefaultSMTP.setText( Const.NVL( action.getDefaultSMTP(), "" ) );
    weMailSender.setText( Const.NVL( action.geteMailSender(), "" ) );

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
    action.setEmailAddress( wMailAddress.getText() );
    action.setTimeOut( wTimeOut.getText() );
    action.setDefaultSMTP( wDefaultSMTP.getText() );
    action.seteMailSender( weMailSender.getText() );
    action.setSMTPCheck( wSMTPCheck.getSelection() );
    dispose();
  }
}
