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

package org.apache.hop.workflow.actions.ping;

import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.PluginDialog;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.IActionDialog;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
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
 * This dialog allows you to edit the ping action settings.
 *
 * @author Samatar Hassan
 * @since Mar-2007
 */
@PluginDialog( 
		  id = "PING", 
		  image = "Ping.svg", 
		  pluginType = PluginDialog.PluginType.ACTION,
		  documentationUrl = "https://www.project-hop.org/manual/latest/plugins/actions/ping.html"
)
public class ActionPingDialog extends ActionDialog implements IActionDialog {
  private static Class<?> PKG = ActionPing.class; // for i18n purposes, needed by Translator!!

  private Label wlName;
  private Text wName;
  private FormData fdlName, fdName;

  private Label wlHostname;
  private TextVar wHostname;
  private FormData fdlHostname, fdHostname;

  private Label wlTimeOut;
  private TextVar wTimeOut;
  private FormData fdlTimeOut, fdTimeOut;

  private Button wOk, wCancel;

  private Listener lsOk, lsCancel;

  private ActionPing action;

  private Shell shell;

  private SelectionAdapter lsDef;

  private Label wlPingType;
  private CCombo wPingType;
  private FormData fdlPingType, fdPingType;

  private Label wlNbrPackets;
  private TextVar wNbrPackets;
  private FormData fdNbrPackets, fdlNbrPackets;

  private boolean changed;

  public ActionPingDialog( Shell parent, IAction action, WorkflowMeta workflowMeta ) {
    super( parent, action, workflowMeta );
    this.action = (ActionPing) action;
    if ( this.action.getName() == null ) {
      this.action.setName( BaseMessages.getString( PKG, "JobPing.Name.Default" ) );
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
    shell.setText( BaseMessages.getString( PKG, "JobPing.Title" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Filename line
    wlName = new Label( shell, SWT.RIGHT );
    wlName.setText( BaseMessages.getString( PKG, "JobPing.Name.Label" ) );
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

    // hostname line
    wlHostname = new Label( shell, SWT.RIGHT );
    wlHostname.setText( BaseMessages.getString( PKG, "JobPing.Hostname.Label" ) );
    props.setLook( wlHostname );
    fdlHostname = new FormData();
    fdlHostname.left = new FormAttachment( 0, 0 );
    fdlHostname.top = new FormAttachment( wName, margin );
    fdlHostname.right = new FormAttachment( middle, 0 );
    wlHostname.setLayoutData( fdlHostname );

    wHostname = new TextVar( workflowMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wHostname );
    wHostname.addModifyListener( lsMod );
    fdHostname = new FormData();
    fdHostname.left = new FormAttachment( middle, 0 );
    fdHostname.top = new FormAttachment( wName, margin );
    fdHostname.right = new FormAttachment( 100, 0 );
    wHostname.setLayoutData( fdHostname );

    // Whenever something changes, set the tooltip to the expanded version:
    wHostname.addModifyListener( new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        wHostname.setToolTipText( workflowMeta.environmentSubstitute( wHostname.getText() ) );
      }
    } );

    wlPingType = new Label( shell, SWT.RIGHT );
    wlPingType.setText( BaseMessages.getString( PKG, "JobPing.PingType.Label" ) );
    props.setLook( wlPingType );
    fdlPingType = new FormData();
    fdlPingType.left = new FormAttachment( 0, 0 );
    fdlPingType.right = new FormAttachment( middle, 0 );
    fdlPingType.top = new FormAttachment( wHostname, margin );
    wlPingType.setLayoutData( fdlPingType );
    wPingType = new CCombo( shell, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wPingType.add( BaseMessages.getString( PKG, "JobPing.ClassicPing.Label" ) );
    wPingType.add( BaseMessages.getString( PKG, "JobPing.SystemPing.Label" ) );
    wPingType.add( BaseMessages.getString( PKG, "JobPing.BothPings.Label" ) );
    wPingType.select( 1 ); // +1: starts at -1
    props.setLook( wPingType );
    fdPingType = new FormData();
    fdPingType.left = new FormAttachment( middle, 0 );
    fdPingType.top = new FormAttachment( wHostname, margin );
    fdPingType.right = new FormAttachment( 100, 0 );
    wPingType.setLayoutData( fdPingType );
    wPingType.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        setPingType();
        action.setChanged();
      }
    } );

    // Timeout
    wlTimeOut = new Label( shell, SWT.RIGHT );
    wlTimeOut.setText( BaseMessages.getString( PKG, "JobPing.TimeOut.Label" ) );
    props.setLook( wlTimeOut );
    fdlTimeOut = new FormData();
    fdlTimeOut.left = new FormAttachment( 0, 0 );
    fdlTimeOut.right = new FormAttachment( middle, 0 );
    fdlTimeOut.top = new FormAttachment( wPingType, margin );
    wlTimeOut.setLayoutData( fdlTimeOut );

    wTimeOut = new TextVar( workflowMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wlTimeOut.setToolTipText( BaseMessages.getString( PKG, "JobPing.TimeOut.Tooltip" ) );
    props.setLook( wTimeOut );
    wTimeOut.addModifyListener( lsMod );
    fdTimeOut = new FormData();
    fdTimeOut.left = new FormAttachment( middle, 0 );
    fdTimeOut.top = new FormAttachment( wPingType, margin );
    fdTimeOut.right = new FormAttachment( 100, 0 );
    wTimeOut.setLayoutData( fdTimeOut );

    // Nbr packets to send
    wlNbrPackets = new Label( shell, SWT.RIGHT );
    wlNbrPackets.setText( BaseMessages.getString( PKG, "JobPing.NrPackets.Label" ) );
    props.setLook( wlNbrPackets );
    fdlNbrPackets = new FormData();
    fdlNbrPackets.left = new FormAttachment( 0, 0 );
    fdlNbrPackets.right = new FormAttachment( middle, 0 );
    fdlNbrPackets.top = new FormAttachment( wTimeOut, margin );
    wlNbrPackets.setLayoutData( fdlNbrPackets );

    wNbrPackets = new TextVar( workflowMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wNbrPackets );
    wNbrPackets.addModifyListener( lsMod );
    fdNbrPackets = new FormData();
    fdNbrPackets.left = new FormAttachment( middle, 0 );
    fdNbrPackets.top = new FormAttachment( wTimeOut, margin );
    fdNbrPackets.right = new FormAttachment( 100, 0 );
    wNbrPackets.setLayoutData( fdNbrPackets );

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
    wHostname.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();
    setPingType();
    BaseTransformDialog.setSize( shell );

    shell.open();
    props.setDialogSize( shell, "JobPingDialogSize" );
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return action;
  }

  private void setPingType() {
    wlTimeOut.setEnabled( wPingType.getSelectionIndex() == action.isystemPing
      || wPingType.getSelectionIndex() == action.ibothPings );
    wTimeOut.setEnabled( wPingType.getSelectionIndex() == action.isystemPing
      || wPingType.getSelectionIndex() == action.ibothPings );
    wlNbrPackets.setEnabled( wPingType.getSelectionIndex() == action.iclassicPing
      || wPingType.getSelectionIndex() == action.ibothPings );
    wNbrPackets.setEnabled( wPingType.getSelectionIndex() == action.iclassicPing
      || wPingType.getSelectionIndex() == action.ibothPings );
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
    if ( action.getHostname() != null ) {
      wHostname.setText( action.getHostname() );
    }
    if ( action.getNbrPackets() != null ) {
      wNbrPackets.setText( action.getNbrPackets() );
    } else {
      wNbrPackets.setText( "2" );
    }

    if ( action.getTimeOut() != null ) {
      wTimeOut.setText( action.getTimeOut() );
    } else {
      wTimeOut.setText( "3000" );
    }

    wPingType.select( action.ipingtype );

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
    action.setHostname( wHostname.getText() );
    action.setNbrPackets( wNbrPackets.getText() );
    action.setTimeOut( wTimeOut.getText() );
    action.ipingtype = wPingType.getSelectionIndex();
    if ( wPingType.getSelectionIndex() == action.isystemPing ) {
      action.pingtype = action.systemPing;
    } else if ( wPingType.getSelectionIndex() == action.ibothPings ) {
      action.pingtype = action.bothPings;
    } else {
      action.pingtype = action.classicPing;
    }

    dispose();
  }

  public boolean evaluates() {
    return true;
  }

  public boolean isUnconditional() {
    return false;
  }
}
