/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.concatfields;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.textfileoutput.TextFileField;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.pipeline.transform.ITableItemInsertListener;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Point;
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
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

/*
 * ConcatFieldsDialog
 *
 */
public class ConcatFieldsDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = ConcatFieldsMeta.class; // For Translator

  private CTabFolder wTabFolder;
  private FormData fdTabFolder;

  private CTabItem wFieldsTab;

  private FormData fdFieldsComp;

  private Label wlTargetFieldName;
  private TextVar wTargetFieldName;
  private FormData fdlTargetFieldName, fdTargetFieldName;

  private Label wlTargetFieldLength;
  private Text wTargetFieldLength;
  private FormData fdlTargetFieldLength, fdTargetFieldLength;

  private Label wlSeparator;
  private Button wbSeparator;
  private TextVar wSeparator;
  private FormData fdlSeparator, fdbSeparator, fdSeparator;

  private Label wlEnclosure;
  private TextVar wEnclosure;
  private FormData fdlEnclosure, fdEnclosure;

  private TableView wFields;
  private FormData fdFields;

  private ConcatFieldsMeta input;

  private Button wMinWidth;
  private Listener lsMinWidth;
  private boolean gotEncodings = false;

  private ColumnInfo[] colinf;

  private Map<String, Integer> inputFields;

  public ConcatFieldsDialog( Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, pipelineMeta, sname );
    input = (ConcatFieldsMeta) in;
    inputFields = new HashMap<>();
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
    shell.setText( BaseMessages.getString( PKG, "ConcatFieldsDialog.DialogTitle" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // transformName line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "System.Label.TransformName" ) );
    props.setLook( wlTransformName );
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment( 0, 0 );
    fdlTransformName.top = new FormAttachment( 0, margin );
    fdlTransformName.right = new FormAttachment( middle, -margin );
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

    // TargetFieldName line
    wlTargetFieldName = new Label( shell, SWT.RIGHT );
    wlTargetFieldName.setText( BaseMessages.getString( PKG, "ConcatFieldsDialog.TargetFieldName.Label" ) );
    wlTargetFieldName.setToolTipText( BaseMessages.getString( PKG, "ConcatFieldsDialog.TargetFieldName.Tooltip" ) );
    props.setLook( wlTargetFieldName );
    fdlTargetFieldName = new FormData();
    fdlTargetFieldName.left = new FormAttachment( 0, 0 );
    fdlTargetFieldName.top = new FormAttachment( wTransformName, margin );
    fdlTargetFieldName.right = new FormAttachment( middle, -margin );
    wlTargetFieldName.setLayoutData( fdlTargetFieldName );
    wTargetFieldName = new TextVar( variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wTargetFieldName.setText( "" );
    props.setLook( wTargetFieldName );
    wTargetFieldName.addModifyListener( lsMod );
    fdTargetFieldName = new FormData();
    fdTargetFieldName.left = new FormAttachment( middle, 0 );
    fdTargetFieldName.top = new FormAttachment( wTransformName, margin );
    fdTargetFieldName.right = new FormAttachment( 100, 0 );
    wTargetFieldName.setLayoutData( fdTargetFieldName );

    // TargetFieldLength line
    wlTargetFieldLength = new Label( shell, SWT.RIGHT );
    wlTargetFieldLength.setText( BaseMessages.getString( PKG, "ConcatFieldsDialog.TargetFieldLength.Label" ) );
    wlTargetFieldLength.setToolTipText( BaseMessages.getString(
      PKG, "ConcatFieldsDialog.TargetFieldLength.Tooltip" ) );
    props.setLook( wlTargetFieldLength );
    fdlTargetFieldLength = new FormData();
    fdlTargetFieldLength.left = new FormAttachment( 0, 0 );
    fdlTargetFieldLength.top = new FormAttachment( wTargetFieldName, margin );
    fdlTargetFieldLength.right = new FormAttachment( middle, -margin );
    wlTargetFieldLength.setLayoutData( fdlTargetFieldLength );
    wTargetFieldLength = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wTargetFieldLength );
    wTargetFieldLength.addModifyListener( lsMod );
    fdTargetFieldLength = new FormData();
    fdTargetFieldLength.left = new FormAttachment( middle, 0 );
    fdTargetFieldLength.top = new FormAttachment( wTargetFieldName, margin );
    fdTargetFieldLength.right = new FormAttachment( 100, 0 );
    wTargetFieldLength.setLayoutData( fdTargetFieldLength );

    // Separator
    wlSeparator = new Label( shell, SWT.RIGHT );
    wlSeparator.setText( BaseMessages.getString( PKG, "ConcatFieldsDialog.Separator.Label" ) );
    props.setLook( wlSeparator );
    fdlSeparator = new FormData();
    fdlSeparator.left = new FormAttachment( 0, 0 );
    fdlSeparator.top = new FormAttachment( wTargetFieldLength, margin );
    fdlSeparator.right = new FormAttachment( middle, -margin );
    wlSeparator.setLayoutData( fdlSeparator );

    wbSeparator = new Button( shell, SWT.PUSH | SWT.CENTER );
    props.setLook( wbSeparator );
    wbSeparator.setText( BaseMessages.getString( PKG, "ConcatFieldsDialog.Separator.Button" ) );
    fdbSeparator = new FormData();
    fdbSeparator.right = new FormAttachment( 100, 0 );
    fdbSeparator.top = new FormAttachment( wTargetFieldLength, margin );
    wbSeparator.setLayoutData( fdbSeparator );
    wbSeparator.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent se ) {
        // wSeparator.insert("\t");
        wSeparator.getTextWidget().insert( "\t" );
      }
    } );

    wSeparator = new TextVar( variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSeparator );
    wSeparator.addModifyListener( lsMod );
    fdSeparator = new FormData();
    fdSeparator.left = new FormAttachment( middle, 0 );
    fdSeparator.top = new FormAttachment( wTargetFieldLength, margin );
    fdSeparator.right = new FormAttachment( wbSeparator, -margin );
    wSeparator.setLayoutData( fdSeparator );

    // Enclosure line...
    wlEnclosure = new Label( shell, SWT.RIGHT );
    wlEnclosure.setText( BaseMessages.getString( PKG, "ConcatFieldsDialog.Enclosure.Label" ) );
    props.setLook( wlEnclosure );
    fdlEnclosure = new FormData();
    fdlEnclosure.left = new FormAttachment( 0, 0 );
    fdlEnclosure.top = new FormAttachment( wSeparator, margin );
    fdlEnclosure.right = new FormAttachment( middle, -margin );
    wlEnclosure.setLayoutData( fdlEnclosure );
    wEnclosure = new TextVar( variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wEnclosure );
    wEnclosure.addModifyListener( lsMod );
    fdEnclosure = new FormData();
    fdEnclosure.left = new FormAttachment( middle, 0 );
    fdEnclosure.top = new FormAttachment( wSeparator, margin );
    fdEnclosure.right = new FormAttachment( 100, 0 );
    wEnclosure.setLayoutData( fdEnclosure );

    // ////////////////////////
    // START OF TABS
    // /

    wTabFolder = new CTabFolder( shell, SWT.BORDER );
    props.setLook( wTabFolder, Props.WIDGET_STYLE_TAB );

    // Fields tab...
    //
    wFieldsTab = new CTabItem( wTabFolder, SWT.NONE );
    wFieldsTab.setText( BaseMessages.getString( PKG, "ConcatFieldsDialog.FieldsTab.TabTitle" ) );

    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = Const.FORM_MARGIN;
    fieldsLayout.marginHeight = Const.FORM_MARGIN;

    Composite wFieldsComp = new Composite( wTabFolder, SWT.NONE );
    wFieldsComp.setLayout( fieldsLayout );
    props.setLook( wFieldsComp );

    wGet = new Button( wFieldsComp, SWT.PUSH );
    wGet.setText( BaseMessages.getString( PKG, "System.Button.GetFields" ) );
    wGet.setToolTipText( BaseMessages.getString( PKG, "System.Tooltip.GetFields" ) );

    wMinWidth = new Button( wFieldsComp, SWT.PUSH );
    wMinWidth.setText( BaseMessages.getString( PKG, "ConcatFieldsDialog.MinWidth.Button" ) );
    wMinWidth.setToolTipText( BaseMessages.getString( PKG, "ConcatFieldsDialog.MinWidth.Tooltip" ) );

    setButtonPositions( new Button[] { wGet, wMinWidth }, margin, null );

    final int FieldsCols = 10;
    final int FieldsRows = input.getOutputFields().length;

    // Prepare a list of possible formats...
    String[] dats = Const.getDateFormats();
    String[] nums = Const.getNumberFormats();
    int totsize = dats.length + nums.length;
    String[] formats = new String[totsize];
    for ( int x = 0; x < dats.length; x++ ) {
      formats[x] = dats[x];
    }
    for ( int x = 0; x < nums.length; x++ ) {
      formats[dats.length + x] = nums[x];
    }

    colinf = new ColumnInfo[FieldsCols];
    colinf[0] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "ConcatFieldsDialog.NameColumn.Column" ), ColumnInfo.COLUMN_TYPE_CCOMBO,
        new String[] { "" }, false );
    colinf[1] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "ConcatFieldsDialog.TypeColumn.Column" ), ColumnInfo.COLUMN_TYPE_CCOMBO,
        ValueMetaBase.getTypes() );
    colinf[2] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "ConcatFieldsDialog.FormatColumn.Column" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, formats );
    colinf[3] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "ConcatFieldsDialog.LengthColumn.Column" ), ColumnInfo.COLUMN_TYPE_TEXT,
        false );
    colinf[4] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "ConcatFieldsDialog.PrecisionColumn.Column" ),
        ColumnInfo.COLUMN_TYPE_TEXT, false );
    colinf[5] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "ConcatFieldsDialog.CurrencyColumn.Column" ),
        ColumnInfo.COLUMN_TYPE_TEXT, false );
    colinf[6] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "ConcatFieldsDialog.DecimalColumn.Column" ), ColumnInfo.COLUMN_TYPE_TEXT,
        false );
    colinf[7] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "ConcatFieldsDialog.GroupColumn.Column" ), ColumnInfo.COLUMN_TYPE_TEXT,
        false );
    colinf[8] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "ConcatFieldsDialog.TrimTypeColumn.Column" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, ValueMetaBase.trimTypeDesc, true );
    colinf[9] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "ConcatFieldsDialog.NullColumn.Column" ), ColumnInfo.COLUMN_TYPE_TEXT,
        false );

    wFields =
      new TableView(
        variables, wFieldsComp, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, FieldsRows, lsMod, props );

    fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment( 0, 0 );
    fdFields.right = new FormAttachment( 100, 0 );
    fdFields.bottom = new FormAttachment( wGet, -margin );
    wFields.setLayoutData( fdFields );

    //
    // Search the fields in the background

    final Runnable runnable = new Runnable() {
      public void run() {
        TransformMeta transformMeta = pipelineMeta.findTransform( transformName );
        if ( transformMeta != null ) {
          try {
            IRowMeta row = pipelineMeta.getPrevTransformFields( variables, transformMeta );

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

    fdFieldsComp = new FormData();
    fdFieldsComp.left = new FormAttachment( 0, 0 );
    fdFieldsComp.top = new FormAttachment( 0, 0 );
    fdFieldsComp.right = new FormAttachment( 100, 0 );
    fdFieldsComp.bottom = new FormAttachment( 100, 0 );
    wFieldsComp.setLayoutData( fdFieldsComp );

    wFieldsComp.layout();
    wFieldsTab.setControl( wFieldsComp );

    fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( wEnclosure, margin );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.bottom = new FormAttachment( 100, -50 );
    wTabFolder.setLayoutData( fdTabFolder );

    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );

    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOk, wCancel }, margin, wTabFolder );

    // Add listeners
    lsOk = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };
    lsGet = new Listener() {
      public void handleEvent( Event e ) {
        get();
      }
    };
    lsMinWidth = new Listener() {
      public void handleEvent( Event e ) {
        setMinimalWidth();
      }
    };
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };

    wOk.addListener( SWT.Selection, lsOk );
    wGet.addListener( SWT.Selection, lsGet );
    wMinWidth.addListener( SWT.Selection, lsMinWidth );
    wCancel.addListener( SWT.Selection, lsCancel );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wTransformName.addSelectionListener( lsDef );
    wTargetFieldName.addSelectionListener( lsDef );
    wTargetFieldLength.addSelectionListener( lsDef );
    wSeparator.addSelectionListener( lsDef );

    // Whenever something changes, set the tooltip to the expanded version:
    wTargetFieldName.addModifyListener( new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        wTargetFieldName.setToolTipText( variables.resolve( wTargetFieldName.getText() ) );
      }
    } );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    lsResize = new Listener() {
      public void handleEvent( Event event ) {
        Point size = shell.getSize();
        wFields.setSize( size.x - 10, size.y - 50 );
        wFields.table.setSize( size.x - 10, size.y - 50 );
        wFields.redraw();
      }
    };
    shell.addListener( SWT.Resize, lsResize );

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

  protected void setComboBoxes() {
    // Something was changed in the row.
    //
    final Map<String, Integer> fields = new HashMap<>();

    // Add the currentMeta fields...
    fields.putAll( inputFields );

    Set<String> keySet = fields.keySet();
    List<String> entries = new ArrayList<>( keySet );

    String[] fieldNames = entries.toArray( new String[entries.size()] );

    Const.sortStrings( fieldNames );
    colinf[0].setComboValues( fieldNames );
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    // New concat fields
    if ( input.getTargetFieldName() != null ) {
      wTargetFieldName.setText( input.getTargetFieldName() );
    }
    wTargetFieldLength.setText( "" + input.getTargetFieldLength() );
    wSeparator.setText( Const.NVL( input.getSeparator(), "" ) );
    wEnclosure.setText( Const.NVL( input.getEnclosure(), "" ) );

    logDebug( "getting fields info..." );

    for ( int i = 0; i < input.getOutputFields().length; i++ ) {
      TextFileField field = input.getOutputFields()[i];

      TableItem item = wFields.table.getItem( i );
      if ( field.getName() != null ) {
        item.setText( 1, field.getName() );
      }
      item.setText( 2, field.getTypeDesc() );
      if ( field.getFormat() != null ) {
        item.setText( 3, field.getFormat() );
      }
      if ( field.getLength() >= 0 ) {
        item.setText( 4, "" + field.getLength() );
      }
      if ( field.getPrecision() >= 0 ) {
        item.setText( 5, "" + field.getPrecision() );
      }
      if ( field.getCurrencySymbol() != null ) {
        item.setText( 6, field.getCurrencySymbol() );
      }
      if ( field.getDecimalSymbol() != null ) {
        item.setText( 7, field.getDecimalSymbol() );
      }
      if ( field.getGroupingSymbol() != null ) {
        item.setText( 8, field.getGroupingSymbol() );
      }
      String trim = field.getTrimTypeDesc();
      if ( trim != null ) {
        item.setText( 9, trim );
      }
      if ( field.getNullString() != null ) {
        item.setText( 10, field.getNullString() );
      }
    }

    wFields.optWidth( true );

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;

    input.setChanged( backupChanged );

    dispose();
  }

  private void getInfo( ConcatFieldsMeta tfoi ) {
    // New concat fields
    tfoi.setTargetFieldName( wTargetFieldName.getText() );
    tfoi.setTargetFieldLength( Const.toInt( wTargetFieldLength.getText(), 0 ) );
    tfoi.setSeparator( wSeparator.getText() );
    tfoi.setEnclosure( wEnclosure.getText() );

    int i;

    int nrFields = wFields.nrNonEmpty();

    tfoi.allocate( nrFields );

    for ( i = 0; i < nrFields; i++ ) {
      TextFileField field = new TextFileField();

      TableItem item = wFields.getNonEmpty( i );
      field.setName( item.getText( 1 ) );
      field.setType( item.getText( 2 ) );
      field.setFormat( item.getText( 3 ) );
      field.setLength( Const.toInt( item.getText( 4 ), -1 ) );
      field.setPrecision( Const.toInt( item.getText( 5 ), -1 ) );
      field.setCurrencySymbol( item.getText( 6 ) );
      field.setDecimalSymbol( item.getText( 7 ) );
      field.setGroupingSymbol( item.getText( 8 ) );
      field.setTrimType( ValueMetaBase.getTrimTypeByDesc( item.getText( 9 ) ) );
      field.setNullString( item.getText( 10 ) );
      //CHECKSTYLE:Indentation:OFF
      tfoi.getOutputFields()[i] = field;
    }
  }

  private void ok() {
    if ( StringUtil.isEmpty( wTransformName.getText() ) ) {
      return;
    }

    transformName = wTransformName.getText(); // return value

    getInfo( input );

    dispose();
  }

  private void get() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields( variables, transformName );
      if ( r != null ) {
        ITableItemInsertListener listener = new ITableItemInsertListener() {
          public boolean tableItemInserted( TableItem tableItem, IValueMeta v ) {
            if ( v.isNumber() ) {
              if ( v.getLength() > 0 ) {
                int le = v.getLength();
                int pr = v.getPrecision();

                if ( v.getPrecision() <= 0 ) {
                  pr = 0;
                }

                String mask = "";
                for ( int m = 0; m < le - pr; m++ ) {
                  mask += "0";
                }
                if ( pr > 0 ) {
                  mask += ".";
                }
                for ( int m = 0; m < pr; m++ ) {
                  mask += "0";
                }
                tableItem.setText( 3, mask );
              }
            }
            return true;
          }
        };
        BaseTransformDialog.getFieldsFromPrevious( r, wFields, 1, new int[] { 1 }, new int[] { 2 }, 4, 5, listener );
      }
    } catch ( HopException ke ) {
      new ErrorDialog( shell, BaseMessages.getString( PKG, "System.Dialog.GetFieldsFailed.Title" ), BaseMessages
        .getString( PKG, "System.Dialog.GetFieldsFailed.Message" ), ke );
    }

  }

  /**
   * Sets the output width to minimal width...
   *
   */
  public void setMinimalWidth() {
    int nrNonEmptyFields = wFields.nrNonEmpty();
    for ( int i = 0; i < nrNonEmptyFields; i++ ) {
      TableItem item = wFields.getNonEmpty( i );

      item.setText( 4, "" );
      item.setText( 5, "" );
      item.setText( 9, ValueMetaBase.getTrimTypeDesc( IValueMeta.TRIM_TYPE_BOTH ) );

      int type = ValueMetaBase.getType( item.getText( 2 ) );
      switch ( type ) {
        case IValueMeta.TYPE_STRING:
          item.setText( 3, "" );
          break;
        case IValueMeta.TYPE_INTEGER:
          item.setText( 3, "0" );
          break;
        case IValueMeta.TYPE_NUMBER:
          item.setText( 3, "0.#####" );
          break;
        case IValueMeta.TYPE_DATE:
          break;
        default:
          break;
      }
    }

    for ( int i = 0; i < input.getOutputFields().length; i++ ) {
      input.getOutputFields()[i].setTrimType( IValueMeta.TRIM_TYPE_BOTH );
    }

    wFields.optWidth( true );
  }

}
