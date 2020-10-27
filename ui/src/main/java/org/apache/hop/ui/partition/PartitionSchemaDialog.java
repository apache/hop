/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
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

package org.apache.hop.ui.partition;

import org.apache.hop.core.Const;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.partition.PartitionSchema;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.metadata.IMetadataDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
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
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
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

public class PartitionSchemaDialog extends Dialog implements IMetadataDialog {
  private static final Class<?> PKG = PartitionSchemaDialog.class; // for i18n purposes, needed by Translator!!

  private final IHopMetadataProvider metadataProvider;
  private PartitionSchema partitionSchema;

  private Shell shell;

  // Name
  private Text wName;

  // Dynamic definition?
  private Button wDynamic;
  private TextVar wNumber;

  // Partitions
  private Label wlPartitions;
  private TableView wPartitions;

  private Button wOk, wGet, wCancel;

  private ModifyListener lsMod;

  private PropsUi props;

  private int middle;
  private int margin;

  private PartitionSchema originalSchema;
  private String result;

  private IVariables variables;

  public PartitionSchemaDialog( Shell par, IHopMetadataProvider metadataProvider, PartitionSchema partitionSchema, IVariables variables ) {
    super( par, SWT.NONE );
    this.metadataProvider = metadataProvider;
    this.partitionSchema = (PartitionSchema) partitionSchema.clone();
    this.originalSchema = partitionSchema;
    this.variables = variables;

    props = PropsUi.getInstance();
    result = null;
  }

  public String open() {
    Shell parent = getParent();
    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    shell.setImage( GuiResource.getInstance().getImageHopUi() );

    lsMod = e -> partitionSchema.setChanged();

    middle = props.getMiddlePct();
    margin = props.getMargin();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setText( BaseMessages.getString( PKG, "PartitionSchemaDialog.Shell.Title" ) );
    shell.setLayout( formLayout );

    // First, add the buttons...

    // Buttons
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( " &OK " );

    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( " &Cancel " );

    Button[] buttons = new Button[] { wOk, wCancel };
    BaseTransformDialog.positionBottomButtons( shell, buttons, margin, null );

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
    fdName.top = new FormAttachment( wlName, 0, SWT.CENTER );
    fdName.left = new FormAttachment( middle, margin ); // To the right of the label
    fdName.right = new FormAttachment( 100, 0 );
    wName.setLayoutData( fdName );
    Control lastControl = wlName;

    // is the schema defined dynamically using the number of hop servers in the used cluster.
    //
    Label wlDynamic = new Label( shell, SWT.RIGHT );
    props.setLook( wlDynamic );
    wlDynamic.setText( BaseMessages.getString( PKG, "PartitionSchemaDialog.Dynamic.Label" ) );
    FormData fdlDynamic = new FormData();
    fdlDynamic.top = new FormAttachment( lastControl, margin );
    fdlDynamic.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fdlDynamic.right = new FormAttachment( middle, 0 );
    wlDynamic.setLayoutData( fdlDynamic );

    wDynamic = new Button( shell, SWT.CHECK );
    props.setLook( wDynamic );
    wDynamic.setToolTipText( BaseMessages.getString( PKG, "PartitionSchemaDialog.Dynamic.Tooltip" ) );
    FormData fdDynamic = new FormData();
    fdDynamic.top = new FormAttachment( wlDynamic, 0, SWT.CENTER );
    fdDynamic.left = new FormAttachment( middle, margin ); // To the right of the label
    fdDynamic.right = new FormAttachment( 100, 0 );
    wDynamic.setLayoutData( fdDynamic );
    lastControl = wlDynamic;

    // If dynamic is chosen, disable to list of partition names
    //
    wDynamic.addListener( SWT.Selection, e-> enableFields());

    // The number of partitions per cluster schema
    //
    Label wlNumber = new Label( shell, SWT.RIGHT );
    props.setLook( wlNumber );
    wlNumber.setText( BaseMessages.getString( PKG, "PartitionSchemaDialog.Number.Label" ) );
    FormData fdlNumber = new FormData();
    fdlNumber.top = new FormAttachment( lastControl, margin );
    fdlNumber.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fdlNumber.right = new FormAttachment( middle, 0 );
    wlNumber.setLayoutData( fdlNumber );

    wNumber = new TextVar( variables, shell, SWT.LEFT | SWT.BORDER | SWT.SINGLE, BaseMessages.getString( PKG, "PartitionSchemaDialog.Number.Tooltip" ) );
    props.setLook( wNumber );
    FormData fdNumber = new FormData();
    fdNumber.top = new FormAttachment( wlNumber, 0, SWT.CENTER );
    fdNumber.left = new FormAttachment( middle, margin ); // To the right of the label
    fdNumber.right = new FormAttachment( 95, 0 );
    wNumber.setLayoutData( fdNumber );
    lastControl = wlNumber;

    // Schema list:
    wlPartitions = new Label( shell, SWT.RIGHT );
    wlPartitions.setText( BaseMessages.getString( PKG, "PartitionSchemaDialog.Partitions.Label" ) );
    props.setLook( wlPartitions );
    FormData fdlPartitions = new FormData();
    fdlPartitions.left = new FormAttachment( 0, 0 );
    fdlPartitions.right = new FormAttachment( middle, 0 );
    fdlPartitions.top = new FormAttachment( lastControl, margin );
    wlPartitions.setLayoutData( fdlPartitions );

    ColumnInfo[] partitionColumns = new ColumnInfo[] {
      new ColumnInfo( BaseMessages.getString( PKG, "PartitionSchemaDialog.PartitionID.Label" ), ColumnInfo.COLUMN_TYPE_TEXT, false, false ),
    };
    wPartitions = new TableView( partitionSchema, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, partitionColumns, 1, lsMod, props );
    props.setLook( wPartitions );
    FormData fdPartitions = new FormData();
    fdPartitions.left = new FormAttachment( middle, margin );
    fdPartitions.right = new FormAttachment( 100, 0 );
    fdPartitions.top = new FormAttachment( lastControl, margin );
    fdPartitions.bottom = new FormAttachment( wOk, -margin * 2 );
    wPartitions.setLayoutData( fdPartitions );

    // Add listeners
    wOk.addListener( SWT.Selection, e -> ok() );
    wCancel.addListener( SWT.Selection, e -> cancel() );

    SelectionAdapter selAdapter = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };
    wName.addSelectionListener( selAdapter );
    wNumber.addSelectionListener( selAdapter );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();

    BaseTransformDialog.setSize( shell );

    shell.open();
    Display display = parent.getDisplay();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return result;
  }

  private void enableFields() {
    boolean tableEnabled = !wDynamic.getSelection();
    wlPartitions.setEnabled( tableEnabled );
    wPartitions.setEnabled( tableEnabled );
  }

  public void dispose() {
    props.setScreen( new WindowProperty( shell ) );
    shell.dispose();
  }

  public void getData() {
    wName.setText( Const.NVL( partitionSchema.getName(), "" ) );

    refreshPartitions();

    wDynamic.setSelection( partitionSchema.isDynamicallyDefined() );
    wNumber.setText( Const.NVL( partitionSchema.getNumberOfPartitions(), "" ) );

    enableFields();

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
      IHopMetadataSerializer<PartitionSchema> serializer = metadataProvider.getSerializer( PartitionSchema.class );
      getInfo();

      if ( !partitionSchema.getName().equals( originalSchema.getName() ) ) {
        if ( serializer.exists( partitionSchema.getName() ) ) {
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
      originalSchema.setNumberOfPartitions( wNumber.getText() );
      originalSchema.setChanged();

      result = originalSchema.getName();

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
