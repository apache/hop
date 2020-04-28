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

package org.apache.hop.pipeline.transforms.syslog;

import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.PluginDialog;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.workflow.actions.syslog.SyslogDefs;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.LabelTextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.FocusListener;
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
import org.snmp4j.UserTarget;
import org.snmp4j.smi.UdpAddress;

import java.net.InetAddress;

@PluginDialog(
        id = "SyslogMessage",
        image = "syslogmessage.svg",
        pluginType = PluginDialog.PluginType.TRANSFORM,
        documentationUrl = ""
)
public class SyslogMessageDialog extends BaseTransformDialog implements ITransformDialog {
  private static Class<?> PKG = SyslogMessageMeta.class; // for i18n purposes, needed by Translator!!

  private Label wlMessageField;
  private CCombo wMessageField;
  private FormData fdlMessageField, fdMessageField;

  private Group wSettingsGroup;
  private FormData fdSettingsGroup;
  private SyslogMessageMeta input;

  private Group wLogSettings;
  private FormData fdLogSettings;

  private FormData fdPort;

  private LabelTextVar wPort;

  private FormData fdFacility;
  private CCombo wFacility;

  private Label wlPriority;
  private FormData fdlPriority;
  private FormData fdPriority;
  private CCombo wPriority;

  private Button wTest;

  private FormData fdTest;

  private Listener lsTest;

  private LabelTextVar wServerName;
  private FormData fdServerName;

  private Label wlFacility;

  private FormData fdlFacility;

  private Label wlAddTimestamp;
  private FormData fdlAddTimestamp;
  private Button wAddTimestamp;
  private FormData fdAddTimestamp;

  private Label wlAddHostName;
  private FormData fdlAddHostName;
  private Button wAddHostName;
  private FormData fdAddHostName;

  private Label wlDatePattern;
  private FormData fdlDatePattern;
  private FormData fdDatePattern;
  private ComboVar wDatePattern;

  private boolean gotPreviousFields = false;

  public SyslogMessageDialog( Shell parent, Object in, PipelineMeta pipelineMeta, String sname ) {
    super( parent, (BaseTransformMeta) in, pipelineMeta, sname );
    input = (SyslogMessageMeta) in;
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, input );

    ModifyListener lsMod = new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        input.setChanged();
      }
    };

    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "SyslogMessageDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // TransformName line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "SyslogMessageDialog.TransformName.Label" ) );
    props.setLook( wlTransformName );
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment( 0, 0 );
    fdlTransformName.right = new FormAttachment( middle, -margin );
    fdlTransformName.top = new FormAttachment( 0, margin );
    wlTransformName.setLayoutData( fdlTransformName );
    wTransformName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wTransformName.setText( transformName );
    props.setLook( wTransformName );
    wTransformName.addModifyListener( lsMod );
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment( middle, 0 );
    fdTransformName.top = new FormAttachment( 0, margin );
    fdTransformName.right = new FormAttachment( 100, 0 );
    wTransformName.setLayoutData( fdTransformName );

    // ///////////////////////////////
    // START OF Settings GROUP //
    // ///////////////////////////////

    wSettingsGroup = new Group( shell, SWT.SHADOW_NONE );
    props.setLook( wSettingsGroup );
    wSettingsGroup.setText( BaseMessages.getString( PKG, "SyslogMessageDialog.wSettingsGroup.Label" ) );

    FormLayout settingGroupLayout = new FormLayout();
    settingGroupLayout.marginWidth = 10;
    settingGroupLayout.marginHeight = 10;
    wSettingsGroup.setLayout( settingGroupLayout );

    // Server port line
    wServerName = new LabelTextVar( pipelineMeta, wSettingsGroup,
      BaseMessages.getString( PKG, "SyslogMessageDialog.Server.Label" ),
      BaseMessages.getString( PKG, "SyslogMessageDialog.Server.Tooltip" ) );
    props.setLook( wServerName );
    wServerName.addModifyListener( lsMod );
    fdServerName = new FormData();
    fdServerName.left = new FormAttachment( 0, 0 );
    fdServerName.top = new FormAttachment( wTransformName, margin );
    fdServerName.right = new FormAttachment( 100, 0 );
    wServerName.setLayoutData( fdServerName );

    // Server port line
    wPort = new LabelTextVar( pipelineMeta, wSettingsGroup,
      BaseMessages.getString( PKG, "SyslogMessageDialog.Port.Label" ),
      BaseMessages.getString( PKG, "SyslogMessageDialog.Port.Tooltip" ) );
    props.setLook( wPort );
    wPort.addModifyListener( lsMod );
    fdPort = new FormData();
    fdPort.left = new FormAttachment( 0, 0 );
    fdPort.top = new FormAttachment( wServerName, margin );
    fdPort.right = new FormAttachment( 100, 0 );
    wPort.setLayoutData( fdPort );

    // Test connection button
    wTest = new Button( wSettingsGroup, SWT.PUSH );
    wTest.setText( BaseMessages.getString( PKG, "SyslogMessageDialog.TestConnection.Label" ) );
    props.setLook( wTest );
    fdTest = new FormData();
    wTest.setToolTipText( BaseMessages.getString( PKG, "SyslogMessageDialog.TestConnection.Tooltip" ) );
    fdTest.top = new FormAttachment( wPort, 2 * margin );
    fdTest.right = new FormAttachment( 100, 0 );
    wTest.setLayoutData( fdTest );

    fdSettingsGroup = new FormData();
    fdSettingsGroup.left = new FormAttachment( 0, margin );
    fdSettingsGroup.top = new FormAttachment( wTransformName, margin );
    fdSettingsGroup.right = new FormAttachment( 100, -margin );
    wSettingsGroup.setLayoutData( fdSettingsGroup );

    // ///////////////////////////////
    // END OF Settings Fields GROUP //

    // ////////////////////////
    // START OF Log SETTINGS GROUP///
    // /
    wLogSettings = new Group( shell, SWT.SHADOW_NONE );
    props.setLook( wLogSettings );
    wLogSettings.setText( BaseMessages.getString( PKG, "SyslogMessageDialog.LogSettings.Group.Label" ) );

    FormLayout LogSettingsgroupLayout = new FormLayout();
    LogSettingsgroupLayout.marginWidth = 10;
    LogSettingsgroupLayout.marginHeight = 10;

    wLogSettings.setLayout( LogSettingsgroupLayout );

    // Facility type
    wlFacility = new Label( wLogSettings, SWT.RIGHT );
    wlFacility.setText( BaseMessages.getString( PKG, "SyslogMessageDialog.Facility.Label" ) );
    props.setLook( wlFacility );
    fdlFacility = new FormData();
    fdlFacility.left = new FormAttachment( 0, margin );
    fdlFacility.right = new FormAttachment( middle, -margin );
    fdlFacility.top = new FormAttachment( wSettingsGroup, margin );
    wlFacility.setLayoutData( fdlFacility );
    wFacility = new CCombo( wLogSettings, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wFacility.setItems( SyslogDefs.FACILITYS );

    props.setLook( wFacility );
    fdFacility = new FormData();
    fdFacility.left = new FormAttachment( middle, margin );
    fdFacility.top = new FormAttachment( wSettingsGroup, margin );
    fdFacility.right = new FormAttachment( 100, 0 );
    wFacility.setLayoutData( fdFacility );
    wFacility.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {

      }
    } );

    // Priority type
    wlPriority = new Label( wLogSettings, SWT.RIGHT );
    wlPriority.setText( BaseMessages.getString( PKG, "SyslogMessageDialog.Priority.Label" ) );
    props.setLook( wlPriority );
    fdlPriority = new FormData();
    fdlPriority.left = new FormAttachment( 0, margin );
    fdlPriority.right = new FormAttachment( middle, -margin );
    fdlPriority.top = new FormAttachment( wFacility, margin );
    wlPriority.setLayoutData( fdlPriority );
    wPriority = new CCombo( wLogSettings, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wPriority.setItems( SyslogDefs.PRIORITYS );

    props.setLook( wPriority );
    fdPriority = new FormData();
    fdPriority.left = new FormAttachment( middle, margin );
    fdPriority.top = new FormAttachment( wFacility, margin );
    fdPriority.right = new FormAttachment( 100, 0 );
    wPriority.setLayoutData( fdPriority );
    wPriority.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {

      }
    } );

    // Add HostName?
    wlAddHostName = new Label( wLogSettings, SWT.RIGHT );
    wlAddHostName.setText( BaseMessages.getString( PKG, "SyslogMessageDialog.AddHostName.Label" ) );
    props.setLook( wlAddHostName );
    fdlAddHostName = new FormData();
    fdlAddHostName.left = new FormAttachment( 0, 0 );
    fdlAddHostName.top = new FormAttachment( wPriority, margin );
    fdlAddHostName.right = new FormAttachment( middle, -margin );
    wlAddHostName.setLayoutData( fdlAddHostName );
    wAddHostName = new Button( wLogSettings, SWT.CHECK );
    props.setLook( wAddHostName );
    wAddHostName.setToolTipText( BaseMessages.getString( PKG, "SyslogMessageDialog.AddHostName.Tooltip" ) );
    fdAddHostName = new FormData();
    fdAddHostName.left = new FormAttachment( middle, margin );
    fdAddHostName.top = new FormAttachment( wPriority, margin );
    fdAddHostName.right = new FormAttachment( 100, 0 );
    wAddHostName.setLayoutData( fdAddHostName );
    wAddHostName.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    // Add timestamp?
    wlAddTimestamp = new Label( wLogSettings, SWT.RIGHT );
    wlAddTimestamp.setText( BaseMessages.getString( PKG, "SyslogMessageDialog.AddTimestamp.Label" ) );
    props.setLook( wlAddTimestamp );
    fdlAddTimestamp = new FormData();
    fdlAddTimestamp.left = new FormAttachment( 0, 0 );
    fdlAddTimestamp.top = new FormAttachment( wAddHostName, margin );
    fdlAddTimestamp.right = new FormAttachment( middle, -margin );
    wlAddTimestamp.setLayoutData( fdlAddTimestamp );
    wAddTimestamp = new Button( wLogSettings, SWT.CHECK );
    props.setLook( wAddTimestamp );
    wAddTimestamp.setToolTipText( BaseMessages.getString( PKG, "SyslogMessageDialog.AddTimestamp.Tooltip" ) );
    fdAddTimestamp = new FormData();
    fdAddTimestamp.left = new FormAttachment( middle, margin );
    fdAddTimestamp.top = new FormAttachment( wAddHostName, margin );
    fdAddTimestamp.right = new FormAttachment( 100, 0 );
    wAddTimestamp.setLayoutData( fdAddTimestamp );
    wAddTimestamp.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        activeAddTimestamp();
        input.setChanged();
      }
    } );

    // DatePattern type
    wlDatePattern = new Label( wLogSettings, SWT.RIGHT );
    wlDatePattern.setText( BaseMessages.getString( PKG, "SyslogMessageDialog.DatePattern.Label" ) );
    props.setLook( wlDatePattern );
    fdlDatePattern = new FormData();
    fdlDatePattern.left = new FormAttachment( 0, margin );
    fdlDatePattern.right = new FormAttachment( middle, -margin );
    fdlDatePattern.top = new FormAttachment( wAddTimestamp, margin );
    wlDatePattern.setLayoutData( fdlDatePattern );
    wDatePattern = new ComboVar( pipelineMeta, wLogSettings, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wDatePattern.setItems( Const.getDateFormats() );
    props.setLook( wDatePattern );
    fdDatePattern = new FormData();
    fdDatePattern.left = new FormAttachment( middle, margin );
    fdDatePattern.top = new FormAttachment( wAddTimestamp, margin );
    fdDatePattern.right = new FormAttachment( 100, 0 );
    wDatePattern.setLayoutData( fdDatePattern );
    wDatePattern.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {

      }
    } );

    // MessageField field
    wlMessageField = new Label( wLogSettings, SWT.RIGHT );
    wlMessageField.setText( BaseMessages.getString( PKG, "SyslogMessageDialog.MessageNameField.Label" ) );
    props.setLook( wlMessageField );
    fdlMessageField = new FormData();
    fdlMessageField.left = new FormAttachment( 0, margin );
    fdlMessageField.right = new FormAttachment( middle, -margin );
    fdlMessageField.top = new FormAttachment( wDatePattern, margin );
    wlMessageField.setLayoutData( fdlMessageField );

    wMessageField = new CCombo( wLogSettings, SWT.BORDER | SWT.READ_ONLY );
    props.setLook( wMessageField );
    wMessageField.setEditable( true );
    wMessageField.addModifyListener( lsMod );
    fdMessageField = new FormData();
    fdMessageField.left = new FormAttachment( middle, margin );
    fdMessageField.top = new FormAttachment( wDatePattern, margin );
    fdMessageField.right = new FormAttachment( 100, 0 );
    wMessageField.setLayoutData( fdMessageField );
    wMessageField.addFocusListener( new FocusListener() {
      public void focusLost( org.eclipse.swt.events.FocusEvent e ) {
      }

      public void focusGained( org.eclipse.swt.events.FocusEvent e ) {
        get();
      }
    } );

    fdLogSettings = new FormData();
    fdLogSettings.left = new FormAttachment( 0, margin );
    fdLogSettings.top = new FormAttachment( wSettingsGroup, margin );
    fdLogSettings.right = new FormAttachment( 100, -margin );
    wLogSettings.setLayoutData( fdLogSettings );
    // ///////////////////////////////////////////////////////////
    // / END OF Log SETTINGS GROUP
    // ///////////////////////////////////////////////////////////

    // THE BUTTONS
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOk, wCancel }, margin, wLogSettings );

    // Add listeners
    lsOk = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };

    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };

    lsTest = new Listener() {
      public void handleEvent( Event e ) {
        test();
      }
    };

    wOk.addListener( SWT.Selection, lsOk );
    wCancel.addListener( SWT.Selection, lsCancel );
    wTest.addListener( SWT.Selection, lsTest );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wTransformName.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    // Set the shell size, based upon previous time...
    setSize();
    getData();
    activeAddTimestamp();
    input.setChanged( changed );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return transformName;
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    if ( input.getMessageFieldName() != null ) {
      wMessageField.setText( input.getMessageFieldName() );
    }
    if ( input.getServerName() != null ) {
      wServerName.setText( input.getServerName() );
    }
    if ( input.getPort() != null ) {
      wPort.setText( input.getPort() );
    }
    if ( input.getFacility() != null ) {
      wFacility.setText( input.getFacility() );
    }
    if ( input.getPriority() != null ) {
      wPriority.setText( input.getPriority() );
    }
    if ( input.getDatePattern() != null ) {
      wDatePattern.setText( input.getDatePattern() );
    }
    wAddTimestamp.setSelection( input.isAddTimestamp() );
    wAddHostName.setSelection( input.isAddHostName() );

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void activeAddTimestamp() {
    wlDatePattern.setEnabled( wAddTimestamp.getSelection() );
    wDatePattern.setEnabled( wAddTimestamp.getSelection() );
  }

  private void cancel() {
    transformName = null;
    input.setChanged( changed );
    dispose();
  }

  private void ok() {
    if ( Utils.isEmpty( wTransformName.getText() ) ) {
      return;
    }

    input.setServerName( wServerName.getText() );
    input.setPort( wPort.getText() );
    input.setFacility( wFacility.getText() );
    input.setPriority( wPriority.getText() );
    input.setMessageFieldName( wMessageField.getText() );
    input.addTimestamp( wAddTimestamp.getSelection() );
    input.addHostName( wAddHostName.getSelection() );

    input.setDatePattern( wDatePattern.getText() );

    transformName = wTransformName.getText(); // return value

    dispose();
  }

  private void get() {
    if ( !gotPreviousFields ) {
      gotPreviousFields = true;
      try {
        String source = wMessageField.getText();

        wMessageField.removeAll();
        IRowMeta r = pipelineMeta.getPrevTransformFields( transformName );
        if ( r != null ) {
          wMessageField.setItems( r.getFieldNames() );
          if ( source != null ) {
            wMessageField.setText( source );
          }
        }
      } catch ( HopException ke ) {
        new ErrorDialog(
          shell, BaseMessages.getString( PKG, "SyslogMessageDialog.FailedToGetFields.DialogTitle" ),
          BaseMessages.getString( PKG, "SyslogMessageDialog.FailedToGetFields.DialogMessage" ), ke );
      }
    }
  }

  private void test() {
    boolean testOK = false;
    String errMsg = null;
    String hostname = pipelineMeta.environmentSubstitute( wServerName.getText() );
    int nrPort = Const.toInt( pipelineMeta.environmentSubstitute( "" + wPort.getText() ), SyslogDefs.DEFAULT_PORT );

    try {
      UdpAddress udpAddress = new UdpAddress( InetAddress.getByName( hostname ), nrPort );
      UserTarget usertarget = new UserTarget();
      usertarget.setAddress( udpAddress );

      testOK = usertarget.getAddress().isValid();

      if ( !testOK ) {
        errMsg = BaseMessages.getString( PKG, "SyslogMessageDialog.CanNotGetAddress", hostname );
      }

    } catch ( Exception e ) {
      errMsg = e.getMessage();
    }
    if ( testOK ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_INFORMATION );
      mb.setMessage( BaseMessages.getString( PKG, "SyslogMessageDialog.Connected.OK", hostname ) + Const.CR );
      mb.setText( BaseMessages.getString( PKG, "SyslogMessageDialog.Connected.Title.Ok" ) );
      mb.open();
    } else {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString( PKG, "SyslogMessageDialog.Connected.NOK.ConnectionBad", hostname )
        + Const.CR + errMsg + Const.CR );
      mb.setText( BaseMessages.getString( PKG, "SyslogMessageDialog.Connected.Title.Bad" ) );
      mb.open();
    }

  }

}
