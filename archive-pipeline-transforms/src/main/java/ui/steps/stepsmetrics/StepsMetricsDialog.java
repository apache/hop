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

package org.apache.hop.ui.pipeline.transforms.transformsmetrics;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.transformsmetrics.TransformsMetrics;
import org.apache.hop.pipeline.transforms.transformsmetrics.TransformsMetricsMeta;
import org.apache.hop.ui.core.PropsUI;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
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
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

import java.util.ArrayList;
import java.util.List;

public class TransformsMetricsDialog extends BaseTransformDialog implements ITransformDialog {
  private static Class<?> PKG = TransformsMetrics.class; // for i18n purposes, needed by Translator!!

  private static final String[] YES_NO_COMBO = new String[] {
    BaseMessages.getString( PKG, "System.Combo.No" ), BaseMessages.getString( PKG, "System.Combo.Yes" ) };

  private String[] previousTransforms;
  private TransformsMetricsMeta input;

  private Label wlFields;
  private TableView wFields;
  private FormData fdlFields, fdFields;

  private CTabFolder wTabFolder;
  private FormData fdTabFolder;

  private CTabItem wGeneralTab, wFieldsTab;

  private FormData fdGeneralComp, fdFieldsComp;

  private Label wlTransformNameField;
  private TextVar wTransformNameField;
  private FormData fdlTransformNameField, fdTransformNameField;

  private Label wlTransformIdField;
  private TextVar wTransformIdField;
  private FormData fdlTransformIdField, fdTransformIdField;

  private Label wlLinesinputField;
  private TextVar wLinesinputField;
  private FormData fdlLinesinputField, fdLinesinputField;

  private Label wlLinesoutputField;
  private TextVar wLinesoutputField;
  private FormData fdlLinesoutputField, fdLinesoutputField;

  private Label wlLinesreadField;
  private TextVar wLinesreadField;
  private FormData fdlLinesreadField, fdLinesreadField;

  private Label wlLineswrittenField;
  private TextVar wLineswrittenField;
  private FormData fdlLineswrittenField, fdLineswrittenField;

  private Label wlLineserrorsField;
  private TextVar wLineserrorsField;
  private FormData fdlLineserrorsField, fdLineserrorsField;

  private Label wlSecondsField;
  private TextVar wSecondsField;
  private FormData fdlSecondsField, fdSecondsField;

  private Label wlLinesupdatedField;
  private TextVar wLinesupdatedField;
  private FormData fdlLinesupdatedField, fdLinesupdatedField;

  public TransformsMetricsDialog( Shell parent, Object in, PipelineMeta tr, String sname ) {
    super( parent, (BaseTransformMeta) in, tr, sname );
    input = (TransformsMetricsMeta) in;
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
    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "TransformsMetricsDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // TransformName line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "TransformsMetricsDialog.TransformName.Label" ) );
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

    wTabFolder = new CTabFolder( shell, SWT.BORDER );
    props.setLook( wTabFolder, PropsUI.WIDGET_STYLE_TAB );

    // ////////////////////////
    // START OF GENERAL TAB///
    // /
    wGeneralTab = new CTabItem( wTabFolder, SWT.NONE );
    wGeneralTab.setText( BaseMessages.getString( PKG, "TransformsMetricsDialog.General" ) );
    Composite wGeneralComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wGeneralComp );
    FormLayout fileLayout = new FormLayout();
    fileLayout.marginWidth = 3;
    fileLayout.marginHeight = 3;
    wGeneralComp.setLayout( fileLayout );

    // Get the previous transforms...
    setTransformNames();

    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wGet = new Button( shell, SWT.PUSH );
    wGet.setText( BaseMessages.getString( PKG, "TransformsMetricsDialog.getTransforms.Label" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOK, wGet, wCancel }, margin, null );

    // Table with fields
    wlFields = new Label( wGeneralComp, SWT.NONE );
    wlFields.setText( BaseMessages.getString( PKG, "TransformsMetricsDialog.Fields.Label" ) );
    props.setLook( wlFields );
    fdlFields = new FormData();
    fdlFields.left = new FormAttachment( 0, 0 );
    fdlFields.top = new FormAttachment( wTransformName, margin );
    wlFields.setLayoutData( fdlFields );

    final int FieldsCols = 3;
    final int FieldsRows = input.getTransformName().length;

    ColumnInfo[] colinf = new ColumnInfo[ FieldsCols ];
    colinf[ 0 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "TransformsMetricsDialog.Fieldname.Transform" ), ColumnInfo.COLUMN_TYPE_CCOMBO,
        previousTransforms, false );
    colinf[ 1 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "TransformsMetricsDialog.Fieldname.CopyNr" ), ColumnInfo.COLUMN_TYPE_TEXT,
        false );
    colinf[ 2 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "TransformsMetricsDialog.Required.Column" ), ColumnInfo.COLUMN_TYPE_CCOMBO,
        YES_NO_COMBO );
    colinf[ 1 ].setUsingVariables( true );
    wFields =
      new TableView(
        pipelineMeta, wGeneralComp, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, FieldsRows, lsMod, props );

    fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment( wlFields, margin );
    fdFields.right = new FormAttachment( 100, 0 );
    fdFields.bottom = new FormAttachment( 100, -2 * margin );
    wFields.setLayoutData( fdFields );

    fdGeneralComp = new FormData();
    fdGeneralComp.left = new FormAttachment( 0, 0 );
    fdGeneralComp.top = new FormAttachment( 0, 0 );
    fdGeneralComp.right = new FormAttachment( 100, 0 );
    fdGeneralComp.bottom = new FormAttachment( 100, 0 );
    wGeneralComp.setLayoutData( fdGeneralComp );

    wGeneralComp.layout();
    wGeneralTab.setControl( wGeneralComp );

    // ///////////////////////////////////////////////////////////
    // / END OF GENERAL TAB
    // ///////////////////////////////////////////////////////////

    // ////////////////////////
    // START OF FIELDS TAB///
    // /
    wFieldsTab = new CTabItem( wTabFolder, SWT.NONE );
    wFieldsTab.setText( BaseMessages.getString( PKG, "TransformsMetricsDialog.Group.Fields" ) );
    Composite wFieldsComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wFieldsComp );
    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = 3;
    fieldsLayout.marginHeight = 3;
    wFieldsComp.setLayout( fieldsLayout );

    // TransformName line
    wlTransformNameField = new Label( wFieldsComp, SWT.RIGHT );
    wlTransformNameField.setText( BaseMessages.getString( PKG, "TransformsMetricsDialog.Label.TransformnameField" ) );
    props.setLook( wlTransformNameField );
    fdlTransformNameField = new FormData();
    fdlTransformNameField.left = new FormAttachment( 0, 0 );
    fdlTransformNameField.top = new FormAttachment( 0, margin );
    fdlTransformNameField.right = new FormAttachment( middle, -margin );
    wlTransformNameField.setLayoutData( fdlTransformNameField );
    wTransformNameField = new TextVar( pipelineMeta, wFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wTransformNameField.setText( "" );
    props.setLook( wTransformNameField );
    wTransformNameField.addModifyListener( lsMod );
    fdTransformNameField = new FormData();
    fdTransformNameField.left = new FormAttachment( middle, 0 );
    fdTransformNameField.top = new FormAttachment( 0, margin );
    fdTransformNameField.right = new FormAttachment( 100, -margin );
    wTransformNameField.setLayoutData( fdTransformNameField );

    // Transform ID field line
    wlTransformIdField = new Label( wFieldsComp, SWT.RIGHT );
    wlTransformIdField.setText( BaseMessages.getString( PKG, "TransformsMetricsDialog.Label.TransformIdField" ) );
    props.setLook( wlTransformIdField );
    fdlTransformIdField = new FormData();
    fdlTransformIdField.left = new FormAttachment( 0, 0 );
    fdlTransformIdField.top = new FormAttachment( wTransformNameField, margin );
    fdlTransformIdField.right = new FormAttachment( middle, -margin );
    wlTransformIdField.setLayoutData( fdlTransformIdField );
    wTransformIdField = new TextVar( pipelineMeta, wFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wTransformIdField.setText( "" );
    props.setLook( wTransformIdField );
    wTransformIdField.addModifyListener( lsMod );
    fdTransformIdField = new FormData();
    fdTransformIdField.left = new FormAttachment( middle, 0 );
    fdTransformIdField.top = new FormAttachment( wTransformNameField, margin );
    fdTransformIdField.right = new FormAttachment( 100, -margin );
    wTransformIdField.setLayoutData( fdTransformIdField );

    // Linesinput line
    wlLinesinputField = new Label( wFieldsComp, SWT.RIGHT );
    wlLinesinputField.setText( BaseMessages.getString( PKG, "TransformsMetricsDialog.Label.LinesinputField" ) );
    props.setLook( wlLinesinputField );
    fdlLinesinputField = new FormData();
    fdlLinesinputField.left = new FormAttachment( 0, 0 );
    fdlLinesinputField.top = new FormAttachment( wTransformIdField, margin );
    fdlLinesinputField.right = new FormAttachment( middle, -margin );
    wlLinesinputField.setLayoutData( fdlLinesinputField );
    wLinesinputField = new TextVar( pipelineMeta, wFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wLinesinputField.setText( "" );
    props.setLook( wLinesinputField );
    wLinesinputField.addModifyListener( lsMod );
    fdLinesinputField = new FormData();
    fdLinesinputField.left = new FormAttachment( middle, 0 );
    fdLinesinputField.top = new FormAttachment( wTransformIdField, margin );
    fdLinesinputField.right = new FormAttachment( 100, -margin );
    wLinesinputField.setLayoutData( fdLinesinputField );

    // Linesoutput line
    wlLinesoutputField = new Label( wFieldsComp, SWT.RIGHT );
    wlLinesoutputField.setText( BaseMessages.getString( PKG, "TransformsMetricsDialog.Label.LinesoutputField" ) );
    props.setLook( wlLinesoutputField );
    fdlLinesoutputField = new FormData();
    fdlLinesoutputField.left = new FormAttachment( 0, 0 );
    fdlLinesoutputField.top = new FormAttachment( wLinesinputField, margin );
    fdlLinesoutputField.right = new FormAttachment( middle, -margin );
    wlLinesoutputField.setLayoutData( fdlLinesoutputField );
    wLinesoutputField = new TextVar( pipelineMeta, wFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wLinesoutputField.setText( "" );
    props.setLook( wLinesoutputField );
    wLinesoutputField.addModifyListener( lsMod );
    fdLinesoutputField = new FormData();
    fdLinesoutputField.left = new FormAttachment( middle, 0 );
    fdLinesoutputField.top = new FormAttachment( wLinesinputField, margin );
    fdLinesoutputField.right = new FormAttachment( 100, -margin );
    wLinesoutputField.setLayoutData( fdLinesoutputField );

    // Linesread line
    wlLinesreadField = new Label( wFieldsComp, SWT.RIGHT );
    wlLinesreadField.setText( BaseMessages.getString( PKG, "TransformsMetricsDialog.Label.LinesreadField" ) );
    props.setLook( wlLinesreadField );
    fdlLinesreadField = new FormData();
    fdlLinesreadField.left = new FormAttachment( 0, 0 );
    fdlLinesreadField.top = new FormAttachment( wLinesoutputField, margin );
    fdlLinesreadField.right = new FormAttachment( middle, -margin );
    wlLinesreadField.setLayoutData( fdlLinesreadField );
    wLinesreadField = new TextVar( pipelineMeta, wFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wLinesreadField.setText( "" );
    props.setLook( wLinesreadField );
    wLinesreadField.addModifyListener( lsMod );
    fdLinesreadField = new FormData();
    fdLinesreadField.left = new FormAttachment( middle, 0 );
    fdLinesreadField.top = new FormAttachment( wLinesoutputField, margin );
    fdLinesreadField.right = new FormAttachment( 100, -margin );
    wLinesreadField.setLayoutData( fdLinesreadField );

    // Linesupdated line
    wlLinesupdatedField = new Label( wFieldsComp, SWT.RIGHT );
    wlLinesupdatedField.setText( BaseMessages.getString( PKG, "TransformsMetricsDialog.Label.LinesupdatedField" ) );
    props.setLook( wlLinesupdatedField );
    fdlLinesupdatedField = new FormData();
    fdlLinesupdatedField.left = new FormAttachment( 0, 0 );
    fdlLinesupdatedField.top = new FormAttachment( wLinesreadField, margin );
    fdlLinesupdatedField.right = new FormAttachment( middle, -margin );
    wlLinesupdatedField.setLayoutData( fdlLinesupdatedField );
    wLinesupdatedField = new TextVar( pipelineMeta, wFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wLinesupdatedField.setText( "" );
    props.setLook( wLinesupdatedField );
    wLinesupdatedField.addModifyListener( lsMod );
    fdLinesupdatedField = new FormData();
    fdLinesupdatedField.left = new FormAttachment( middle, 0 );
    fdLinesupdatedField.top = new FormAttachment( wLinesreadField, margin );
    fdLinesupdatedField.right = new FormAttachment( 100, -margin );
    wLinesupdatedField.setLayoutData( fdLinesupdatedField );

    // Lineswritten line
    wlLineswrittenField = new Label( wFieldsComp, SWT.RIGHT );
    wlLineswrittenField.setText( BaseMessages.getString( PKG, "TransformsMetricsDialog.Label.LineswrittenField" ) );
    props.setLook( wlLineswrittenField );
    fdlLineswrittenField = new FormData();
    fdlLineswrittenField.left = new FormAttachment( 0, 0 );
    fdlLineswrittenField.top = new FormAttachment( wLinesupdatedField, margin );
    fdlLineswrittenField.right = new FormAttachment( middle, -margin );
    wlLineswrittenField.setLayoutData( fdlLineswrittenField );
    wLineswrittenField = new TextVar( pipelineMeta, wFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wLineswrittenField.setText( "" );
    props.setLook( wLineswrittenField );
    wLineswrittenField.addModifyListener( lsMod );
    fdLineswrittenField = new FormData();
    fdLineswrittenField.left = new FormAttachment( middle, 0 );
    fdLineswrittenField.top = new FormAttachment( wLinesupdatedField, margin );
    fdLineswrittenField.right = new FormAttachment( 100, -margin );
    wLineswrittenField.setLayoutData( fdLineswrittenField );

    // Lineserrors line
    wlLineserrorsField = new Label( wFieldsComp, SWT.RIGHT );
    wlLineserrorsField.setText( BaseMessages.getString( PKG, "TransformsMetricsDialog.Label.LineserrorsField" ) );
    props.setLook( wlLineserrorsField );
    fdlLineserrorsField = new FormData();
    fdlLineserrorsField.left = new FormAttachment( 0, 0 );
    fdlLineserrorsField.top = new FormAttachment( wLineswrittenField, margin );
    fdlLineserrorsField.right = new FormAttachment( middle, -margin );
    wlLineserrorsField.setLayoutData( fdlLineserrorsField );
    wLineserrorsField = new TextVar( pipelineMeta, wFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wLineserrorsField.setText( "" );
    props.setLook( wLineserrorsField );
    wLineserrorsField.addModifyListener( lsMod );
    fdLineserrorsField = new FormData();
    fdLineserrorsField.left = new FormAttachment( middle, 0 );
    fdLineserrorsField.top = new FormAttachment( wLineswrittenField, margin );
    fdLineserrorsField.right = new FormAttachment( 100, -margin );
    wLineserrorsField.setLayoutData( fdLineserrorsField );

    // Seconds line
    wlSecondsField = new Label( wFieldsComp, SWT.RIGHT );
    wlSecondsField.setText( BaseMessages.getString( PKG, "TransformsMetricsDialog.Label.DurationField" ) );
    props.setLook( wlSecondsField );
    fdlSecondsField = new FormData();
    fdlSecondsField.left = new FormAttachment( 0, 0 );
    fdlSecondsField.top = new FormAttachment( wLineserrorsField, margin );
    fdlSecondsField.right = new FormAttachment( middle, -margin );
    wlSecondsField.setLayoutData( fdlSecondsField );
    wSecondsField = new TextVar( pipelineMeta, wFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wSecondsField.setText( "" );
    props.setLook( wSecondsField );
    wSecondsField.addModifyListener( lsMod );
    fdSecondsField = new FormData();
    fdSecondsField.left = new FormAttachment( middle, 0 );
    fdSecondsField.top = new FormAttachment( wLineserrorsField, margin );
    fdSecondsField.right = new FormAttachment( 100, -margin );
    wSecondsField.setLayoutData( fdSecondsField );

    fdFieldsComp = new FormData();
    fdFieldsComp.left = new FormAttachment( 0, 0 );
    fdFieldsComp.top = new FormAttachment( 0, 0 );
    fdFieldsComp.right = new FormAttachment( 100, 0 );
    fdFieldsComp.bottom = new FormAttachment( 100, 0 );
    wFieldsComp.setLayoutData( fdFieldsComp );
    wFieldsComp.layout();
    wFieldsTab.setControl( wFieldsComp );

    // ///////////////////////////////////////////////////////////
    // / END OF FIELDS TAB
    // ///////////////////////////////////////////////////////////

    fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( wTransformName, margin );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.bottom = new FormAttachment( 100, -50 );
    wTabFolder.setLayoutData( fdTabFolder );

    // Add listeners
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };
    lsGet = new Listener() {
      public void handleEvent( Event e ) {
        get();
      }
    };
    lsOK = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };

    wCancel.addListener( SWT.Selection, lsCancel );
    wOK.addListener( SWT.Selection, lsOK );
    wGet.addListener( SWT.Selection, lsGet );

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

    wTabFolder.setSelection( 0 );
    // Set the shell size, based upon previous time...
    setSize();

    getData();
    input.setChanged( changed );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return transformName;
  }

  private void setTransformNames() {
    previousTransforms = pipelineMeta.getTransformNames();
    String[] nextTransforms = pipelineMeta.getNextTransformNames( transformMeta );

    List<String> entries = new ArrayList<>();
    for ( int i = 0; i < previousTransforms.length; i++ ) {
      if ( !previousTransforms[ i ].equals( transformName ) ) {
        if ( nextTransforms != null ) {
          for ( int j = 0; j < nextTransforms.length; j++ ) {
            if ( !nextTransforms[ j ].equals( previousTransforms[ i ] ) ) {
              entries.add( previousTransforms[ i ] );
            }
          }
        }
      }
    }
    previousTransforms = entries.toArray( new String[ entries.size() ] );
  }

  private void get() {
    wFields.removeAll();
    Table table = wFields.table;

    for ( int i = 0; i < previousTransforms.length; i++ ) {
      TableItem ti = new TableItem( table, SWT.NONE );
      ti.setText( 0, "" + ( i + 1 ) );
      ti.setText( 1, previousTransforms[ i ] );
      ti.setText( 2, "0" );
      ti.setText( 3, BaseMessages.getString( PKG, "System.Combo.No" ) );
    }
    wFields.removeEmptyRows();
    wFields.setRowNums();
    wFields.optWidth( true );

  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    Table table = wFields.table;
    if ( input.getTransformName().length > 0 ) {
      table.removeAll();
    }
    for ( int i = 0; i < input.getTransformName().length; i++ ) {
      TableItem ti = new TableItem( table, SWT.NONE );
      ti.setText( 0, "" + ( i + 1 ) );
      if ( input.getTransformName()[ i ] != null ) {
        ti.setText( 1, input.getTransformName()[ i ] );
        ti.setText( 2, String.valueOf( Const.toInt( input.getTransformCopyNr()[ i ], 0 ) ) );
        ti.setText( 3, input.getRequiredTransformsDesc( input.getTransformRequired()[ i ] ) );
      }
    }

    wFields.removeEmptyRows();
    wFields.setRowNums();
    wFields.optWidth( true );

    if ( input.getTransformNameFieldName() != null ) {
      wTransformNameField.setText( input.getTransformNameFieldName() );
    }
    if ( input.getTransformIdFieldName() != null ) {
      wTransformIdField.setText( input.getTransformIdFieldName() );
    }
    if ( input.getTransformLinesInputFieldName() != null ) {
      wLinesinputField.setText( input.getTransformLinesInputFieldName() );
    }
    if ( input.getTransformLinesOutputFieldName() != null ) {
      wLinesoutputField.setText( input.getTransformLinesOutputFieldName() );
    }
    if ( input.getTransformLinesReadFieldName() != null ) {
      wLinesreadField.setText( input.getTransformLinesReadFieldName() );
    }
    if ( input.getTransformLinesWrittenFieldName() != null ) {
      wLineswrittenField.setText( input.getTransformLinesWrittenFieldName() );
    }
    if ( input.getTransformLinesUpdatedFieldName() != null ) {
      wLinesupdatedField.setText( input.getTransformLinesUpdatedFieldName() );
    }
    if ( input.getTransformLinesErrorsFieldName() != null ) {
      wLineserrorsField.setText( input.getTransformLinesErrorsFieldName() );
    }
    if ( input.getTransformSecondsFieldName() != null ) {
      wSecondsField.setText( input.getTransformSecondsFieldName() );
    }

    wTransformName.selectAll();
    wTransformName.setFocus();
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

    getInfo( input );
    dispose();
  }

  private void getInfo( TransformsMetricsMeta in ) {
    transformName = wTransformName.getText(); // return value
    int nrTransforms = wFields.nrNonEmpty();
    in.allocate( nrTransforms );
    for ( int i = 0; i < nrTransforms; i++ ) {
      TableItem ti = wFields.getNonEmpty( i );
      TransformMeta tm = pipelineMeta.findTransform( ti.getText( 1 ) );
      //CHECKSTYLE:Indentation:OFF
      if ( tm != null ) {
        in.getTransformName()[ i ] = tm.getName();
        in.getTransformCopyNr()[ i ] = "" + Const.toInt( ti.getText( 2 ), 0 );
        in.getTransformRequired()[ i ] = in.getRequiredTransformsCode( ti.getText( 3 ) );
      }

    }

    in.setTransformNameFieldName( wTransformNameField.getText() );
    in.setTransformIdFieldName( wTransformIdField.getText() );
    in.setTransformLinesInputFieldName( wLinesinputField.getText() );
    in.setTransformLinesOutputFieldName( wLinesoutputField.getText() );
    in.setTransformLinesReadFieldName( wLinesreadField.getText() );
    in.setTransformLinesWrittenFieldName( wLineswrittenField.getText() );
    in.setTransformLinesUpdatedFieldName( wLinesupdatedField.getText() );
    in.setTransformLinesErrorsFieldName( wLineserrorsField.getText() );
    in.setTransformSecondsFieldName( wSecondsField.getText() );

  }
}
