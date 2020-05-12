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

package org.apache.hop.workflow.actions.webserviceavailable;

import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.PluginDialog;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
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
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

/**
 * This dialog allows you to edit the webservice available action.
 *
 * @author Samatar
 * @since 05-11-2009
 */
@PluginDialog( 
		  id = "WEBSERVICE_AVAILABLE", 
		  image = "WebServiceAvailable.svg", 
		  pluginType = PluginDialog.PluginType.ACTION,
		  documentationUrl = "https://www.project-hop.org/manual/latest/plugins/actions/"
)
public class ActionWebServiceAvailableDialog extends ActionDialog implements IActionDialog {
  private static Class<?> PKG = ActionWebServiceAvailable.class; // for i18n purposes, needed by Translator!!

  private Label wlName;

  private Text wName;

  private FormData fdlName, fdName;

  private Label wlURL;

  private TextVar wURL;

  private FormData fdlURL, fdURL;

  private Label wlConnectTimeOut;

  private TextVar wConnectTimeOut;

  private FormData fdlConnectTimeOut, fdConnectTimeOut;

  private Label wlReadTimeOut;

  private TextVar wReadTimeOut;

  private FormData fdlReadTimeOut, fdReadTimeOut;

  private Button wOk, wCancel;

  private Listener lsOk, lsCancel;

  private ActionWebServiceAvailable action;

  private Shell shell;

  private SelectionAdapter lsDef;

  private boolean changed;

  public ActionWebServiceAvailableDialog( Shell parent, IAction action,
                                          WorkflowMeta workflowMeta ) {
    super( parent, action, workflowMeta );
    this.action = (ActionWebServiceAvailable) action;
    if ( this.action.getName() == null ) {
      this.action.setName( BaseMessages.getString( PKG, "ActionWebServiceAvailable.Name.Default" ) );
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
    shell.setText( BaseMessages.getString( PKG, "ActionWebServiceAvailable.Title" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Filename line
    wlName = new Label( shell, SWT.RIGHT );
    wlName.setText( BaseMessages.getString( PKG, "ActionWebServiceAvailable.Name.Label" ) );
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

    // URL line
    wlURL = new Label( shell, SWT.RIGHT );
    wlURL.setText( BaseMessages.getString( PKG, "ActionWebServiceAvailable.URL.Label" ) );
    props.setLook( wlURL );
    fdlURL = new FormData();
    fdlURL.left = new FormAttachment( 0, 0 );
    fdlURL.top = new FormAttachment( wName, margin );
    fdlURL.right = new FormAttachment( middle, -margin );
    wlURL.setLayoutData( fdlURL );

    wURL = new TextVar( workflowMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wURL );
    wURL.addModifyListener( lsMod );
    fdURL = new FormData();
    fdURL.left = new FormAttachment( middle, 0 );
    fdURL.top = new FormAttachment( wName, margin );
    fdURL.right = new FormAttachment( 100, -margin );
    wURL.setLayoutData( fdURL );

    // Whenever something changes, set the tooltip to the expanded version:
    wURL.addModifyListener( new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        wURL.setToolTipText( workflowMeta.environmentSubstitute( wURL.getText() ) );
      }
    } );

    // connect timeout line
    wlConnectTimeOut = new Label( shell, SWT.RIGHT );
    wlConnectTimeOut.setText( BaseMessages.getString( PKG, "ActionWebServiceAvailable.ConnectTimeOut.Label" ) );
    props.setLook( wlConnectTimeOut );
    fdlConnectTimeOut = new FormData();
    fdlConnectTimeOut.left = new FormAttachment( 0, 0 );
    fdlConnectTimeOut.top = new FormAttachment( wURL, margin );
    fdlConnectTimeOut.right = new FormAttachment( middle, -margin );
    wlConnectTimeOut.setLayoutData( fdlConnectTimeOut );

    wConnectTimeOut = new TextVar( workflowMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wConnectTimeOut.setToolTipText( BaseMessages.getString(
      PKG, "ActionWebServiceAvailable.ConnectTimeOut.Tooltip" ) );
    props.setLook( wConnectTimeOut );
    wConnectTimeOut.addModifyListener( lsMod );
    fdConnectTimeOut = new FormData();
    fdConnectTimeOut.left = new FormAttachment( middle, 0 );
    fdConnectTimeOut.top = new FormAttachment( wURL, margin );
    fdConnectTimeOut.right = new FormAttachment( 100, -margin );
    wConnectTimeOut.setLayoutData( fdConnectTimeOut );

    // Whenever something changes, set the tooltip to the expanded version:
    wConnectTimeOut.addModifyListener( new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        wConnectTimeOut.setToolTipText( workflowMeta.environmentSubstitute( wConnectTimeOut.getText() ) );
      }
    } );

    // Read timeout line
    wlReadTimeOut = new Label( shell, SWT.RIGHT );
    wlReadTimeOut.setText( BaseMessages.getString( PKG, "ActionWebServiceAvailable.ReadTimeOut.Label" ) );
    props.setLook( wlReadTimeOut );
    fdlReadTimeOut = new FormData();
    fdlReadTimeOut.left = new FormAttachment( 0, 0 );
    fdlReadTimeOut.top = new FormAttachment( wConnectTimeOut, margin );
    fdlReadTimeOut.right = new FormAttachment( middle, -margin );
    wlReadTimeOut.setLayoutData( fdlReadTimeOut );

    wReadTimeOut = new TextVar( workflowMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wReadTimeOut.setToolTipText( BaseMessages.getString( PKG, "ActionWebServiceAvailable.ReadTimeOut.Tooltip" ) );
    props.setLook( wReadTimeOut );
    wReadTimeOut.addModifyListener( lsMod );
    fdReadTimeOut = new FormData();
    fdReadTimeOut.left = new FormAttachment( middle, 0 );
    fdReadTimeOut.top = new FormAttachment( wConnectTimeOut, margin );
    fdReadTimeOut.right = new FormAttachment( 100, -margin );
    wReadTimeOut.setLayoutData( fdReadTimeOut );

    // Whenever something changes, set the tooltip to the expanded version:
    wReadTimeOut.addModifyListener( new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        wReadTimeOut.setToolTipText( workflowMeta.environmentSubstitute( wReadTimeOut.getText() ) );
      }
    } );

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

    BaseTransformDialog.positionBottomButtons( shell, new Button[] { wOk, wCancel }, margin, wReadTimeOut );

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
    wURL.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();

    BaseTransformDialog.setSize( shell );

    shell.open();
    props.setDialogSize( shell, "ActionWebServiceAvailableDialogSize" );
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
    wURL.setText( Const.nullToEmpty( action.getURL() ) );
    wConnectTimeOut.setText( Const.NVL( action.getConnectTimeOut(), "0" ) );
    wReadTimeOut.setText( Const.NVL( action.getReadTimeOut(), "0" ) );

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
    action.setURL( wURL.getText() );
    action.setConnectTimeOut( wConnectTimeOut.getText() );
    action.setReadTimeOut( wReadTimeOut.getText() );
    dispose();
  }

  public boolean evaluates() {
    return true;
  }

  public boolean isUnconditional() {
    return false;
  }

}
