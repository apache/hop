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

package org.apache.hop.ui.partition.dialog;

import org.apache.hop.core.Const;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.metastore.persist.MetaStoreFactory;
import org.apache.hop.partition.PartitionSchema;
import org.apache.hop.ui.core.PropsUI;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GUIResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.trans.step.BaseStepDialog;
import org.eclipse.jface.dialogs.MessageDialog;
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
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

import java.util.ArrayList;
import java.util.List;

/**
 * Dialog that allows you to edit the settings of the partition schema
 *
 * @author Matt
 * @see PartitionSchema
 * @since 17-11-2006
 */

public class PartitionSchemaDialog extends Dialog {
  private static Class<?> PKG = PartitionSchemaDialog.class; // for i18n purposes, needed by Translator2!!

  private final IMetaStore metaStore;
  private PartitionSchema partitionSchema;

  private Shell shell;

  // Name
  private Text wName;

  // Dynamic definition?
  private Button wDynamic;
  private TextVar wNumber;

  // Partitions
  private TableView wPartitions;

  private Button wOK, wGet, wCancel;

  private ModifyListener lsMod;

  private PropsUI props;

  private int middle;
  private int margin;

  private PartitionSchema originalSchema;
  private boolean ok;

  private List<DatabaseMeta> databases;

  private VariableSpace variableSpace;

  public PartitionSchemaDialog( Shell par, IMetaStore metaStore, PartitionSchema partitionSchema, VariableSpace variableSpace ) {
    super( par, SWT.NONE );
    this.metaStore = metaStore;
    this.partitionSchema = (PartitionSchema) partitionSchema.clone();
    this.originalSchema = partitionSchema;
    this.databases = databases;
    this.variableSpace = variableSpace;

    props = PropsUI.getInstance();
    ok = false;
  }

  public boolean open() {
    Shell parent = getParent();
    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    shell.setImage( GUIResource.getInstance().getImageHopUi() );

    lsMod = new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        partitionSchema.setChanged();
      }
    };

    middle = props.getMiddlePct();
    margin = props.getMargin();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setText( BaseMessages.getString( PKG, "PartitionSchemaDialog.Shell.Title" ) );
    shell.setLayout( formLayout );

    // First, add the buttons...

    // Buttons
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( " &OK " );

    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( " &Cancel " );

    Button[] buttons = new Button[] { wOK, wCancel };
    BaseStepDialog.positionBottomButtons( shell, buttons, margin, null );

    // The rest stays above the buttons, so we added those first...

    // What's the schema name??
    //
    Label wlName = new Label( shell, SWT.RIGHT );
    props.setLook( wlName );
    wlName.setText( BaseMessages.getString( PKG, "PartitionSchemaDialog.PartitionName.Label" ) );
    FormData fdlName = new FormData();
    fdlName.top = new FormAttachment( 0, 0 );
    fdlName.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fdlName.right = new FormAttachment( middle, 0 );
    wlName.setLayoutData( fdlName );

    wName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wName );
    wName.addModifyListener( lsMod );
    FormData fdName = new FormData();
    fdName.top = new FormAttachment( 0, 0 );
    fdName.left = new FormAttachment( middle, margin ); // To the right of the label
    fdName.right = new FormAttachment( 95, 0 );
    wName.setLayoutData( fdName );

    // is the schema defined dynamically using the number of slave servers in the used cluster.
    //
    Label wlDynamic = new Label( shell, SWT.RIGHT );
    props.setLook( wlDynamic );
    wlDynamic.setText( BaseMessages.getString( PKG, "PartitionSchemaDialog.Dynamic.Label" ) );
    FormData fdlDynamic = new FormData();
    fdlDynamic.top = new FormAttachment( wName, margin );
    fdlDynamic.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fdlDynamic.right = new FormAttachment( middle, 0 );
    wlDynamic.setLayoutData( fdlDynamic );

    wDynamic = new Button( shell, SWT.CHECK );
    props.setLook( wDynamic );
    wDynamic.setToolTipText( BaseMessages.getString( PKG, "PartitionSchemaDialog.Dynamic.Tooltip" ) );
    FormData fdDynamic = new FormData();
    fdDynamic.top = new FormAttachment( wName, margin );
    fdDynamic.left = new FormAttachment( middle, margin ); // To the right of the label
    fdDynamic.right = new FormAttachment( 95, 0 );
    wDynamic.setLayoutData( fdDynamic );

    // The number of partitions per cluster schema
    //
    Label wlNumber = new Label( shell, SWT.RIGHT );
    props.setLook( wlNumber );
    wlNumber.setText( BaseMessages.getString( PKG, "PartitionSchemaDialog.Number.Label" ) );
    FormData fdlNumber = new FormData();
    fdlNumber.top = new FormAttachment( wDynamic, margin );
    fdlNumber.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fdlNumber.right = new FormAttachment( middle, 0 );
    wlNumber.setLayoutData( fdlNumber );

    wNumber =
      new TextVar( variableSpace, shell, SWT.LEFT | SWT.BORDER | SWT.SINGLE, BaseMessages.getString(
        PKG, "PartitionSchemaDialog.Number.Tooltip" ) );
    props.setLook( wNumber );
    FormData fdNumber = new FormData();
    fdNumber.top = new FormAttachment( wDynamic, margin );
    fdNumber.left = new FormAttachment( middle, margin ); // To the right of the label
    fdNumber.right = new FormAttachment( 95, 0 );
    wNumber.setLayoutData( fdNumber );

    // Schema list:
    Label wlPartitions = new Label( shell, SWT.RIGHT );
    wlPartitions.setText( BaseMessages.getString( PKG, "PartitionSchemaDialog.Partitions.Label" ) );
    props.setLook( wlPartitions );
    FormData fdlPartitions = new FormData();
    fdlPartitions.left = new FormAttachment( 0, 0 );
    fdlPartitions.right = new FormAttachment( middle, 0 );
    fdlPartitions.top = new FormAttachment( wNumber, margin );
    wlPartitions.setLayoutData( fdlPartitions );

    ColumnInfo[] partitionColumns =
      new ColumnInfo[] { new ColumnInfo(
        BaseMessages.getString( PKG, "PartitionSchemaDialog.PartitionID.Label" ), ColumnInfo.COLUMN_TYPE_TEXT,
        false, false ), };
    wPartitions = new TableView( Variables.getADefaultVariableSpace(), // probably better push this up. TODO
      shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, partitionColumns, 1, lsMod, props );
    props.setLook( wPartitions );
    FormData fdPartitions = new FormData();
    fdPartitions.left = new FormAttachment( middle, margin );
    fdPartitions.right = new FormAttachment( 100, 0 );
    fdPartitions.top = new FormAttachment( wNumber, margin );
    fdPartitions.bottom = new FormAttachment( wOK, -margin * 2 );
    wPartitions.setLayoutData( fdPartitions );

    // Add listeners
    wOK.addListener( SWT.Selection, new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    } );
    wCancel.addListener( SWT.Selection, new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    } );

    SelectionAdapter selAdapter = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };
    wName.addSelectionListener( selAdapter );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();

    BaseStepDialog.setSize( shell );

    shell.open();
    Display display = parent.getDisplay();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return ok;
  }

  public void dispose() {
    props.setScreen( new WindowProperty( shell ) );
    shell.dispose();
  }

  public void getData() {
    wName.setText( Const.NVL( partitionSchema.getName(), "" ) );

    refreshPartitions();

    wDynamic.setSelection( partitionSchema.isDynamicallyDefined() );
    wNumber.setText( Const.NVL( partitionSchema.getNumberOfPartitionsPerSlave(), "" ) );

    wName.setFocus();
  }

  private void refreshPartitions() {
    wPartitions.clearAll( false );
    List<String> partitionIDs = partitionSchema.getPartitionIDs();
    for ( int i = 0; i < partitionIDs.size(); i++ ) {
      TableItem item = new TableItem( wPartitions.table, SWT.NONE );
      item.setText( 1, partitionIDs.get( i ) );
    }
    wPartitions.removeEmptyRows();
    wPartitions.setRowNums();
    wPartitions.optWidth( true );
  }

  private void cancel() {
    originalSchema = null;
    dispose();
  }

  public void ok() {
    try {
      MetaStoreFactory<PartitionSchema> partitionFactory = PartitionSchema.createFactory( metaStore );
      getInfo();

      if ( !partitionSchema.getName().equals( originalSchema.getName() ) ) {
        if ( partitionFactory.elementExists( partitionSchema.getName() ) ) {
          String title = BaseMessages.getString( PKG, "PartitionSchemaDialog.PartitionSchemaNameExists.Title" );
          String message =
            BaseMessages.getString( PKG, "PartitionSchemaDialog.PartitionSchemaNameExists", partitionSchema.getName() );
          String okButton = BaseMessages.getString( PKG, "System.Button.OK" );
          MessageDialog dialog =
            new MessageDialog( shell, title, null, message, MessageDialog.ERROR, new String[] { okButton }, 0 );

          dialog.open();
          return;
        }
      }

      originalSchema.setName( partitionSchema.getName() );
      originalSchema.setPartitionIDs( partitionSchema.getPartitionIDs() );
      originalSchema.setDynamicallyDefined( wDynamic.getSelection() );
      originalSchema.setNumberOfPartitionsPerSlave( wNumber.getText() );
      originalSchema.setChanged();

      ok = true;

      dispose();
    } catch ( Exception e ) {
      new ErrorDialog( shell, "Error", "Error getting dialog information for the partition schema", e );
    }
  }

  // Get dialog info in partition schema meta-data
  //
  private void getInfo() {
    partitionSchema.setName( wName.getText() );

    List<String> parts = new ArrayList<>();

    int nrNonEmptyPartitions = wPartitions.nrNonEmpty();
    for ( int i = 0; i < nrNonEmptyPartitions; i++ ) {
      parts.add( wPartitions.getNonEmpty( i ).getText( 1 ) );
    }
    partitionSchema.setPartitionIDs( parts );
  }
}
