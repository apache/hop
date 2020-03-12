/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.ui.trans.steps.analyticquery;

import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.PluginDialog;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.BaseStepMeta;
import org.apache.hop.trans.step.StepDialogInterface;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.trans.steps.analyticquery.AnalyticQueryMeta;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GUIResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.trans.step.BaseStepDialog;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.jface.dialogs.MessageDialogWithToggle;
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
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@PluginDialog( id = "AnalyticQuery", image = "AQI.svg", pluginType = PluginDialog.PluginType.STEP,
  documentationUrl = "http://wiki.pentaho.com/display/EAI/Analytic+Query" )
public class AnalyticQueryDialog extends BaseStepDialog implements StepDialogInterface {
  private static Class<?> PKG = AnalyticQueryDialog.class; // for i18n purposes, needed by Translator2!!

  public static final String STRING_SORT_WARNING_PARAMETER = "AnalyticQuerySortWarning";
  private Label wlGroup;
  private TableView wGroup;
  private FormData fdlGroup, fdGroup;

  private Label wlAgg;
  private TableView wAgg;
  private FormData fdlAgg, fdAgg;
  private Button wGet, wGetAgg;
  private FormData fdGet, fdGetAgg;
  private Listener lsGet, lsGetAgg;

  private AnalyticQueryMeta input;
  private ColumnInfo[] ciKey;
  private ColumnInfo[] ciReturn;

  private Map<String, Integer> inputFields;

  public AnalyticQueryDialog( Shell parent, Object in, TransMeta transMeta, String sname ) {
    super( parent, (BaseStepMeta) in, transMeta, sname );
    input = (AnalyticQueryMeta) in;
    inputFields = new HashMap<String, Integer>();
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
    backupChanged = input.hasChanged();
    // backupAllRows = input.passAllRows();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "AnalyticQueryDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // Stepname line
    wlStepname = new Label( shell, SWT.RIGHT );
    wlStepname.setText( BaseMessages.getString( PKG, "AnalyticQueryDialog.Stepname.Label" ) );
    props.setLook( wlStepname );
    fdlStepname = new FormData();
    fdlStepname.left = new FormAttachment( 0, 0 );
    fdlStepname.right = new FormAttachment( middle, -margin );
    fdlStepname.top = new FormAttachment( 0, margin );
    wlStepname.setLayoutData( fdlStepname );
    wStepname = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wStepname.setText( stepname );
    props.setLook( wStepname );
    wStepname.addModifyListener( lsMod );
    fdStepname = new FormData();
    fdStepname.left = new FormAttachment( middle, 0 );
    fdStepname.top = new FormAttachment( 0, margin );
    fdStepname.right = new FormAttachment( 100, 0 );
    wStepname.setLayoutData( fdStepname );

    wlGroup = new Label( shell, SWT.NONE );
    wlGroup.setText( BaseMessages.getString( PKG, "AnalyticQueryDialog.Group.Label" ) );
    props.setLook( wlGroup );
    fdlGroup = new FormData();
    fdlGroup.left = new FormAttachment( 0, 0 );
    fdlGroup.top = new FormAttachment( wlStepname, margin );
    wlGroup.setLayoutData( fdlGroup );

    int nrKeyCols = 1;
    int nrKeyRows = ( input.getGroupField() != null ? input.getGroupField().length : 1 );

    ciKey = new ColumnInfo[ nrKeyCols ];
    ciKey[ 0 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "AnalyticQueryDialog.ColumnInfo.GroupField" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false );

    wGroup =
      new TableView(
        transMeta, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL, ciKey,
        nrKeyRows, lsMod, props );

    wGet = new Button( shell, SWT.PUSH );
    wGet.setText( BaseMessages.getString( PKG, "AnalyticQueryDialog.GetFields.Button" ) );
    fdGet = new FormData();
    fdGet.top = new FormAttachment( wlGroup, margin );
    fdGet.right = new FormAttachment( 100, 0 );
    wGet.setLayoutData( fdGet );

    fdGroup = new FormData();
    fdGroup.left = new FormAttachment( 0, 0 );
    fdGroup.top = new FormAttachment( wlGroup, margin );
    fdGroup.right = new FormAttachment( wGet, -margin );
    fdGroup.bottom = new FormAttachment( 45, 0 );
    wGroup.setLayoutData( fdGroup );

    // THE Aggregate fields
    wlAgg = new Label( shell, SWT.NONE );
    wlAgg.setText( BaseMessages.getString( PKG, "AnalyticQueryDialog.Aggregates.Label" ) );
    props.setLook( wlAgg );
    fdlAgg = new FormData();
    fdlAgg.left = new FormAttachment( 0, 0 );
    fdlAgg.top = new FormAttachment( wGroup, margin );
    wlAgg.setLayoutData( fdlAgg );

    int UpInsCols = 4;
    int UpInsRows = ( input.getAggregateField() != null ? input.getAggregateField().length : 1 );

    ciReturn = new ColumnInfo[ UpInsCols ];
    ciReturn[ 0 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "AnalyticQueryDialog.ColumnInfo.Name" ), ColumnInfo.COLUMN_TYPE_TEXT,
        false );
    ciReturn[ 1 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "AnalyticQueryDialog.ColumnInfo.Subject" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false );
    ciReturn[ 2 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "AnalyticQueryDialog.ColumnInfo.Type" ), ColumnInfo.COLUMN_TYPE_CCOMBO,
        AnalyticQueryMeta.typeGroupLongDesc );
    ciReturn[ 3 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "AnalyticQueryDialog.ColumnInfo.Value" ), ColumnInfo.COLUMN_TYPE_TEXT,
        false );
    ciReturn[ 3 ].setToolTip( BaseMessages.getString( PKG, "AnalyticQueryDialog.ColumnInfo.Value.Tooltip" ) );

    wAgg =
      new TableView(
        transMeta, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL, ciReturn,
        UpInsRows, lsMod, props );

    wGetAgg = new Button( shell, SWT.PUSH );
    wGetAgg.setText( BaseMessages.getString( PKG, "AnalyticQueryDialog.GetLookupFields.Button" ) );
    fdGetAgg = new FormData();
    fdGetAgg.top = new FormAttachment( wlAgg, margin );
    fdGetAgg.right = new FormAttachment( 100, 0 );
    wGetAgg.setLayoutData( fdGetAgg );

    //
    // Search the fields in the background

    final Runnable runnable = new Runnable() {
      public void run() {
        StepMeta stepMeta = transMeta.findStep( stepname );
        if ( stepMeta != null ) {
          try {
            RowMetaInterface row = transMeta.getPrevStepFields( stepMeta );

            // Remember these fields...
            for ( int i = 0; i < row.size(); i++ ) {
              inputFields.put( row.getValueMeta( i ).getName(), Integer.valueOf( i ) );
            }
            setComboBoxes();
          } catch ( HopException e ) {
            logError( BaseMessages.getString( PKG, "System.Dialog.GetFieldsFailed.Message" ) );
          }
        }
      }
    };
    new Thread( runnable ).start();

    // THE BUTTONS
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOK, wCancel }, margin, null );

    fdAgg = new FormData();
    fdAgg.left = new FormAttachment( 0, 0 );
    fdAgg.top = new FormAttachment( wlAgg, margin );
    fdAgg.right = new FormAttachment( wGetAgg, -margin );
    fdAgg.bottom = new FormAttachment( wOK, -margin );
    wAgg.setLayoutData( fdAgg );

    // Add listeners
    lsOK = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };
    lsGet = new Listener() {
      public void handleEvent( Event e ) {
        get();
      }
    };
    lsGetAgg = new Listener() {
      public void handleEvent( Event e ) {
        getAgg();
      }
    };
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };

    wOK.addListener( SWT.Selection, lsOK );
    wGet.addListener( SWT.Selection, lsGet );
    wGetAgg.addListener( SWT.Selection, lsGetAgg );
    wCancel.addListener( SWT.Selection, lsCancel );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wStepname.addSelectionListener( lsDef );

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
    return stepname;
  }

  protected void setComboBoxes() {
    // Something was changed in the row.
    //
    final Map<String, Integer> fields = new HashMap<String, Integer>();

    // Add the currentMeta fields...
    fields.putAll( inputFields );

    Set<String> keySet = fields.keySet();
    List<String> entries = new ArrayList<>( keySet );

    String[] fieldNames = entries.toArray( new String[ entries.size() ] );

    Const.sortStrings( fieldNames );
    ciKey[ 0 ].setComboValues( fieldNames );
    ciReturn[ 1 ].setComboValues( fieldNames );
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    if ( log.isDebug() ) {
      logDebug( BaseMessages.getString( PKG, "AnalyticQueryDialog.Log.GettingKeyInfo" ) );
    }

    if ( input.getGroupField() != null ) {
      for ( int i = 0; i < input.getGroupField().length; i++ ) {
        TableItem item = wGroup.table.getItem( i );
        if ( input.getGroupField()[ i ] != null ) {
          item.setText( 1, input.getGroupField()[ i ] );
        }
      }
    }

    if ( input.getAggregateField() != null ) {
      for ( int i = 0; i < input.getAggregateField().length; i++ ) {
        TableItem item = wAgg.table.getItem( i );
        if ( input.getAggregateField()[ i ] != null ) {
          item.setText( 1, input.getAggregateField()[ i ] );
        }
        if ( input.getSubjectField()[ i ] != null ) {
          item.setText( 2, input.getSubjectField()[ i ] );
        }
        item.setText( 3, AnalyticQueryMeta.getTypeDescLong( input.getAggregateType()[ i ] ) );
        int value = input.getValueField()[ i ];
        String valuetext = Integer.toString( value );
        if ( valuetext != null ) {
          item.setText( 4, valuetext );
        }
      }
    }

    wGroup.setRowNums();
    wGroup.optWidth( true );
    wAgg.setRowNums();
    wAgg.optWidth( true );

    wStepname.selectAll();
    wStepname.setFocus();
  }

  private void cancel() {
    stepname = null;
    input.setChanged( backupChanged );
    dispose();
  }

  private void ok() {
    if ( Utils.isEmpty( wStepname.getText() ) ) {
      return;
    }

    int sizegroup = wGroup.nrNonEmpty();
    int nrfields = wAgg.nrNonEmpty();

    input.allocate( sizegroup, nrfields );

    //CHECKSTYLE:Indentation:OFF
    for ( int i = 0; i < sizegroup; i++ ) {
      TableItem item = wGroup.getNonEmpty( i );
      input.getGroupField()[ i ] = item.getText( 1 );
    }

    //CHECKSTYLE:Indentation:OFF
    for ( int i = 0; i < nrfields; i++ ) {
      TableItem item = wAgg.getNonEmpty( i );
      input.getAggregateField()[ i ] = item.getText( 1 );
      input.getSubjectField()[ i ] = item.getText( 2 );
      input.getAggregateType()[ i ] = AnalyticQueryMeta.getType( item.getText( 3 ) );
      input.getValueField()[ i ] = Const.toInt( item.getText( 4 ), 1 );
    }

    stepname = wStepname.getText();

    if ( "Y".equalsIgnoreCase( props.getCustomParameter( STRING_SORT_WARNING_PARAMETER, "Y" ) ) ) {
      MessageDialogWithToggle md =
        new MessageDialogWithToggle( shell,
          BaseMessages.getString( PKG, "AnalyticQueryDialog.GroupByWarningDialog.DialogTitle" ), null,
          BaseMessages.getString( PKG, "AnalyticQueryDialog.GroupByWarningDialog.DialogMessage", Const.CR ) + Const.CR,
          MessageDialog.WARNING,
          new String[] { BaseMessages.getString( PKG, "AnalyticQueryDialog.GroupByWarningDialog.Option1" ) },
          0, BaseMessages.getString( PKG, "AnalyticQueryDialog.GroupByWarningDialog.Option2" ),
          "N".equalsIgnoreCase( props.getCustomParameter( STRING_SORT_WARNING_PARAMETER, "Y" ) ) );
      MessageDialogWithToggle.setDefaultImage( GUIResource.getInstance().getImageHopUi() );
      md.open();
      props.setCustomParameter( STRING_SORT_WARNING_PARAMETER, md.getToggleState() ? "N" : "Y" );
      props.saveProps();
    }

    dispose();
  }

  private void get() {
    try {
      RowMetaInterface r = transMeta.getPrevStepFields( stepname );
      if ( r != null && !r.isEmpty() ) {
        BaseStepDialog.getFieldsFromPrevious( r, wGroup, 1, new int[] { 1 }, new int[] {}, -1, -1, null );
      }
    } catch ( HopException ke ) {
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "AnalyticQueryDialog.FailedToGetFields.DialogTitle" ), BaseMessages
        .getString( PKG, "AnalyticQueryDialog.FailedToGetFields.DialogMessage" ), ke );
    }
  }

  private void getAgg() {
    try {
      RowMetaInterface r = transMeta.getPrevStepFields( stepname );
      if ( r != null && !r.isEmpty() ) {
        BaseStepDialog.getFieldsFromPrevious( r, wAgg, 1, new int[] { 1, 2 }, new int[] {}, -1, -1, null );
      }
    } catch ( HopException ke ) {
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "AnalyticQueryDialog.FailedToGetFields.DialogTitle" ), BaseMessages
        .getString( PKG, "AnalyticQueryDialog.FailedToGetFields.DialogMessage" ), ke );
    }
  }
}
