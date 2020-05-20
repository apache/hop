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

package org.apache.hop.pipeline.transforms.mergerows;

import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.PluginDialog;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.errorhandling.IStream;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.jface.dialogs.MessageDialogWithToggle;
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
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

import java.util.List;

@PluginDialog(
        id = "MergeRows",
        image = "mergerows.svg",
        pluginType = PluginDialog.PluginType.TRANSFORM,
        documentationUrl = "http://www.project-hop.org/manual/latest/plugins/transforms/mergejoin.html"
)public class MergeRowsDialog extends BaseTransformDialog implements ITransformDialog {
  private static Class<?> PKG = MergeRowsMeta.class; // for i18n purposes, needed by Translator!!
  public static final String STRING_SORT_WARNING_PARAMETER = "MergeRowsSortWarning";

  private Label wlReference;
  private CCombo wReference;
  private FormData fdlReference, fdReference;

  private Label wlCompare;
  private CCombo wCompare;
  private FormData fdlCompare, fdCompare;

  private Label wlFlagfield;
  private Text wFlagfield;
  private FormData fdlFlagfield, fdFlagfield;

  private Label wlKeys;
  private TableView wKeys;
  private Button wbKeys;
  private FormData fdlKeys, fdKeys, fdbKeys;

  private Label wlValues;
  private TableView wValues;
  private Button wbValues;
  private FormData fdlValues, fdValues, fdbValues;

  private MergeRowsMeta input;

  public MergeRowsDialog( Shell parent, Object in, PipelineMeta tr, String sname ) {
    super( parent, (BaseTransformMeta) in, tr, sname );
    input = (MergeRowsMeta) in;
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX );
    props.setLook( shell );
    setShellImage( shell, input );

    ModifyListener lsMod = new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        input.setChanged();
      }
    };
    backupChanged = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "MergeRowsDialog.Shell.Label" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // TransformName line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "MergeRowsDialog.TransformName.Label" ) );
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

    // Get the previous transforms...
    String[] previousTransforms = pipelineMeta.getPrevTransformNames( transformName );

    // Send 'True' data to...
    wlReference = new Label( shell, SWT.RIGHT );
    wlReference.setText( BaseMessages.getString( PKG, "MergeRowsDialog.Reference.Label" ) );
    props.setLook( wlReference );
    fdlReference = new FormData();
    fdlReference.left = new FormAttachment( 0, 0 );
    fdlReference.right = new FormAttachment( middle, -margin );
    fdlReference.top = new FormAttachment( wTransformName, margin );
    wlReference.setLayoutData( fdlReference );
    wReference = new CCombo( shell, SWT.BORDER );
    props.setLook( wReference );

    if ( previousTransforms != null ) {
      wReference.setItems( previousTransforms );
    }

    wReference.addModifyListener( lsMod );
    fdReference = new FormData();
    fdReference.left = new FormAttachment( middle, 0 );
    fdReference.top = new FormAttachment( wTransformName, margin );
    fdReference.right = new FormAttachment( 100, 0 );
    wReference.setLayoutData( fdReference );

    // Send 'False' data to...
    wlCompare = new Label( shell, SWT.RIGHT );
    wlCompare.setText( BaseMessages.getString( PKG, "MergeRowsDialog.Compare.Label" ) );
    props.setLook( wlCompare );
    fdlCompare = new FormData();
    fdlCompare.left = new FormAttachment( 0, 0 );
    fdlCompare.right = new FormAttachment( middle, -margin );
    fdlCompare.top = new FormAttachment( wReference, margin );
    wlCompare.setLayoutData( fdlCompare );
    wCompare = new CCombo( shell, SWT.BORDER );
    props.setLook( wCompare );

    if ( previousTransforms != null ) {
      wCompare.setItems( previousTransforms );
    }

    wCompare.addModifyListener( lsMod );
    fdCompare = new FormData();
    fdCompare.top = new FormAttachment( wReference, margin );
    fdCompare.left = new FormAttachment( middle, 0 );
    fdCompare.right = new FormAttachment( 100, 0 );
    wCompare.setLayoutData( fdCompare );

    // TransformName line
    wlFlagfield = new Label( shell, SWT.RIGHT );
    wlFlagfield.setText( BaseMessages.getString( PKG, "MergeRowsDialog.FlagField.Label" ) );
    props.setLook( wlFlagfield );
    fdlFlagfield = new FormData();
    fdlFlagfield.left = new FormAttachment( 0, 0 );
    fdlFlagfield.right = new FormAttachment( middle, -margin );
    fdlFlagfield.top = new FormAttachment( wCompare, margin );
    wlFlagfield.setLayoutData( fdlFlagfield );
    wFlagfield = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFlagfield );
    wFlagfield.addModifyListener( lsMod );
    fdFlagfield = new FormData();
    fdFlagfield.top = new FormAttachment( wCompare, margin );
    fdFlagfield.left = new FormAttachment( middle, 0 );
    fdFlagfield.right = new FormAttachment( 100, 0 );
    wFlagfield.setLayoutData( fdFlagfield );

    // THE KEYS TO MATCH...
    wlKeys = new Label( shell, SWT.NONE );
    wlKeys.setText( BaseMessages.getString( PKG, "MergeRowsDialog.Keys.Label" ) );
    props.setLook( wlKeys );
    fdlKeys = new FormData();
    fdlKeys.left = new FormAttachment( 0, 0 );
    fdlKeys.top = new FormAttachment( wFlagfield, margin );
    wlKeys.setLayoutData( fdlKeys );

    int nrKeyRows = ( input.getKeyFields() != null ? input.getKeyFields().length : 1 );

    ColumnInfo[] ciKeys =
      new ColumnInfo[] { new ColumnInfo(
        BaseMessages.getString( PKG, "MergeRowsDialog.ColumnInfo.KeyField" ), ColumnInfo.COLUMN_TYPE_TEXT,
        false ), };

    wKeys =
      new TableView(
        pipelineMeta, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL, ciKeys,
        nrKeyRows, lsMod, props );

    fdKeys = new FormData();
    fdKeys.top = new FormAttachment( wlKeys, margin );
    fdKeys.left = new FormAttachment( 0, 0 );
    fdKeys.bottom = new FormAttachment( 100, -70 );
    fdKeys.right = new FormAttachment( 50, -margin );
    wKeys.setLayoutData( fdKeys );

    wbKeys = new Button( shell, SWT.PUSH );
    wbKeys.setText( BaseMessages.getString( PKG, "MergeRowsDialog.KeyFields.Button" ) );
    fdbKeys = new FormData();
    fdbKeys.top = new FormAttachment( wKeys, margin );
    fdbKeys.left = new FormAttachment( 0, 0 );
    fdbKeys.right = new FormAttachment( 50, -margin );
    wbKeys.setLayoutData( fdbKeys );
    wbKeys.addSelectionListener( new SelectionAdapter() {

      public void widgetSelected( SelectionEvent e ) {
        getKeys();
      }
    } );

    // VALUES TO COMPARE
    wlValues = new Label( shell, SWT.NONE );
    wlValues.setText( BaseMessages.getString( PKG, "MergeRowsDialog.Values.Label" ) );
    props.setLook( wlValues );
    fdlValues = new FormData();
    fdlValues.left = new FormAttachment( 50, 0 );
    fdlValues.top = new FormAttachment( wFlagfield, margin );
    wlValues.setLayoutData( fdlValues );

    int nrValueRows = ( input.getValueFields() != null ? input.getValueFields().length : 1 );

    ColumnInfo[] ciValues =
      new ColumnInfo[] { new ColumnInfo(
        BaseMessages.getString( PKG, "MergeRowsDialog.ColumnInfo.ValueField" ), ColumnInfo.COLUMN_TYPE_TEXT,
        false ), };

    wValues =
      new TableView(
        pipelineMeta, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL, ciValues,
        nrValueRows, lsMod, props );

    fdValues = new FormData();
    fdValues.top = new FormAttachment( wlValues, margin );
    fdValues.left = new FormAttachment( 50, 0 );
    fdValues.bottom = new FormAttachment( 100, -70 );
    fdValues.right = new FormAttachment( 100, 0 );
    wValues.setLayoutData( fdValues );

    wbValues = new Button( shell, SWT.PUSH );
    wbValues.setText( BaseMessages.getString( PKG, "MergeRowsDialog.ValueFields.Button" ) );
    fdbValues = new FormData();
    fdbValues.top = new FormAttachment( wValues, margin );
    fdbValues.left = new FormAttachment( 50, 0 );
    fdbValues.right = new FormAttachment( 100, 0 );
    wbValues.setLayoutData( fdbValues );
    wbValues.addSelectionListener( new SelectionAdapter() {

      public void widgetSelected( SelectionEvent e ) {
        getValues();
      }
    } );

    // Some buttons
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOk, wCancel }, margin, wbKeys );

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
    input.setChanged( backupChanged );

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
    List<IStream> infoStreams = input.getTransformIOMeta().getInfoStreams();

    wReference.setText( Const.NVL( infoStreams.get( 0 ).getTransformName(), "" ) );
    wCompare.setText( Const.NVL( infoStreams.get( 1 ).getTransformName(), "" ) );
    if ( input.getFlagField() != null ) {
      wFlagfield.setText( input.getFlagField() );
    }

    for ( int i = 0; i < input.getKeyFields().length; i++ ) {
      TableItem item = wKeys.table.getItem( i );
      if ( input.getKeyFields()[ i ] != null ) {
        item.setText( 1, input.getKeyFields()[ i ] );
      }
    }
    for ( int i = 0; i < input.getValueFields().length; i++ ) {
      TableItem item = wValues.table.getItem( i );
      if ( input.getValueFields()[ i ] != null ) {
        item.setText( 1, input.getValueFields()[ i ] );
      }
    }

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;
    input.setChanged( backupChanged );
    dispose();
  }

  private void ok() {
    if ( Utils.isEmpty( wTransformName.getText() ) ) {
      return;
    }

    List<IStream> infoStreams = input.getTransformIOMeta().getInfoStreams();
    infoStreams.get( 0 ).setTransformMeta( pipelineMeta.findTransform( wReference.getText() ) );
    infoStreams.get( 1 ).setTransformMeta( pipelineMeta.findTransform( wCompare.getText() ) );
    input.setFlagField( wFlagfield.getText() );

    int nrKeys = wKeys.nrNonEmpty();
    int nrValues = wValues.nrNonEmpty();

    input.allocate( nrKeys, nrValues );

    //CHECKSTYLE:Indentation:OFF
    for ( int i = 0; i < nrKeys; i++ ) {
      TableItem item = wKeys.getNonEmpty( i );
      input.getKeyFields()[ i ] = item.getText( 1 );
    }

    //CHECKSTYLE:Indentation:OFF
    for ( int i = 0; i < nrValues; i++ ) {
      TableItem item = wValues.getNonEmpty( i );
      input.getValueFields()[ i ] = item.getText( 1 );
    }

    transformName = wTransformName.getText(); // return value

    // PDI-13509 Fix
    if ( nrKeys > 0 && "Y".equalsIgnoreCase( props.getCustomParameter( STRING_SORT_WARNING_PARAMETER, "Y" ) ) ) {
      MessageDialogWithToggle md =
        new MessageDialogWithToggle( shell,
          BaseMessages.getString( PKG, "MergeRowsDialog.MergeRowsWarningDialog.DialogTitle" ), null,
          BaseMessages.getString( PKG, "MergeRowsDialog.MergeRowsWarningDialog.DialogMessage", Const.CR ) + Const.CR,
          MessageDialog.WARNING,
          new String[] { BaseMessages.getString( PKG, "MergeRowsDialog.MergeRowsWarningDialog.Option1" ) },
          0,
          BaseMessages.getString( PKG, "MergeRowsDialog.MergeRowsWarningDialog.Option2" ), "N".equalsIgnoreCase(
          props.getCustomParameter( STRING_SORT_WARNING_PARAMETER, "Y" ) ) );
      MessageDialogWithToggle.setDefaultImage( GuiResource.getInstance().getImageHopUi() );
      md.open();
      props.setCustomParameter( STRING_SORT_WARNING_PARAMETER, md.getToggleState() ? "N" : "Y" );
    }


    dispose();
  }

  private void getKeys() {
    try {
      TransformMeta transformMeta = pipelineMeta.findTransform( wReference.getText() );
      if ( transformMeta != null ) {
        IRowMeta prev = pipelineMeta.getTransformFields( transformMeta );
        if ( prev != null ) {
          BaseTransformDialog.getFieldsFromPrevious( prev, wKeys, 1, new int[] { 1 }, new int[] {}, -1, -1, null );
        }
      }
    } catch ( HopException e ) {
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "MergeRowsDialog.ErrorGettingFields.DialogTitle" ), BaseMessages
        .getString( PKG, "MergeRowsDialog.ErrorGettingFields.DialogMessage" ), e );
    }
  }

  private void getValues() {
    try {
      TransformMeta transformMeta = pipelineMeta.findTransform( wReference.getText() );
      if ( transformMeta != null ) {
        IRowMeta prev = pipelineMeta.getTransformFields( transformMeta );
        if ( prev != null ) {
          BaseTransformDialog.getFieldsFromPrevious( prev, wValues, 1, new int[] { 1 }, new int[] {}, -1, -1, null );
        }
      }
    } catch ( HopException e ) {
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "MergeRowsDialog.ErrorGettingFields.DialogTitle" ), BaseMessages
        .getString( PKG, "MergeRowsDialog.ErrorGettingFields.DialogMessage" ), e );
    }
  }
}
