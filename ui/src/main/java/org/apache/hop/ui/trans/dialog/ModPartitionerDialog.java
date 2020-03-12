/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.ui.trans.dialog;

import org.apache.hop.core.Const;
import org.apache.hop.core.plugins.PartitionerPluginType;
import org.apache.hop.core.plugins.PluginInterface;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.trans.ModPartitioner;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.BaseStepMeta;
import org.apache.hop.trans.step.StepDialogInterface;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.trans.step.StepPartitioningMeta;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GUIResource;
import org.apache.hop.ui.trans.step.BaseStepDialog;
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
import org.eclipse.swt.widgets.Shell;

import java.util.Arrays;

public class ModPartitionerDialog extends BaseStepDialog implements StepDialogInterface {
  private static Class<?> PKG = TransDialog.class; // for i18n purposes, needed by Translator2!!

  private StepPartitioningMeta partitioningMeta;
  private ModPartitioner partitioner;
  private String fieldName;

  private Label wlFieldname;
  private CCombo wFieldname;
  private FormData fdlFieldname, fdFieldname;

  public ModPartitionerDialog( Shell parent, StepMeta stepMeta, StepPartitioningMeta partitioningMeta,
                               TransMeta transMeta ) {
    super( parent, (BaseStepMeta) stepMeta.getStepMetaInterface(), transMeta, partitioningMeta
      .getPartitioner().getDescription() );
    this.stepMeta = stepMeta;
    this.partitioningMeta = partitioningMeta;
    partitioner = (ModPartitioner) partitioningMeta.getPartitioner();
    fieldName = partitioner.getFieldName();
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX );
    props.setLook( shell );
    setShellImage( shell );

    ModifyListener lsMod = new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        partitioningMeta.hasChanged( true );
      }
    };
    changed = partitioningMeta.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( partitioner.getDescription() );

    int margin = props.getMargin();

    int middle = props.getMiddlePct();

    wlFieldname = new Label( shell, SWT.RIGHT );
    wlFieldname.setText( "Fieldname" );
    props.setLook( wlFieldname );
    fdlFieldname = new FormData();
    fdlFieldname.left = new FormAttachment( 0, 0 );
    fdlFieldname.right = new FormAttachment( middle, -margin );
    fdlFieldname.top = new FormAttachment( 0, margin );
    wlFieldname.setLayoutData( fdlFieldname );
    wFieldname = new CCombo( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wFieldname.setText( fieldName == null ? "" : fieldName );
    props.setLook( wFieldname );
    wFieldname.addModifyListener( lsMod );
    fdFieldname = new FormData();
    fdFieldname.left = new FormAttachment( middle, 0 );
    fdFieldname.top = new FormAttachment( 0, margin );
    fdFieldname.right = new FormAttachment( 100, 0 );
    wFieldname.setLayoutData( fdFieldname );
    try {
      RowMetaInterface inputFields = transMeta.getPrevStepFields( stepMeta );
      if ( inputFields != null ) {
        String[] fieldNames = inputFields.getFieldNames();
        Arrays.sort( fieldNames );
        wFieldname.setItems( fieldNames );

      }
    } catch ( Exception e ) {
      new ErrorDialog( shell, "Error", "Error obtaining list of input fields:", e );
    }

    // Some buttons
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    fdOK = new FormData();

    setButtonPositions( new Button[] { wOK, wCancel }, margin, null );

    // Add listeners
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };
    lsOK = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };

    wCancel.addListener( SWT.Selection, lsCancel );
    wOK.addListener( SWT.Selection, lsOK );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    // Set the shell size, based upon previous time...
    setSize();
    getData();
    partitioningMeta.hasChanged( changed );

    setSize();
    wOK.setEnabled( !StringUtil.isEmpty( wFieldname.getText() ) );
    ModifyListener modifyListener = new ModifyListener() {
      @Override public void modifyText( ModifyEvent modifyEvent ) {
        wOK.setEnabled( !StringUtil.isEmpty( wFieldname.getText() ) );
      }
    };
    wFieldname.addModifyListener( modifyListener );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return stepname;
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    wFieldname.setText( fieldName == null ? "" : fieldName );
  }

  private void cancel() {
    stepname = null;
    partitioningMeta.hasChanged( changed );
    dispose();
  }

  private void ok() {
    fieldName = wFieldname.getText();
    partitioner.setFieldName( fieldName );
    dispose();
  }

  private void setShellImage( Shell shell ) {
    PluginInterface plugin = PluginRegistry.getInstance().getPlugin( PartitionerPluginType.class, partitioner.getId() );
    if ( !Utils.isEmpty( plugin.getDocumentationUrl() ) ) {
      createHelpButton( shell, stepMeta, plugin );
    }

    shell.setImage( GUIResource.getInstance().getImageHopUi() );
  }
}
