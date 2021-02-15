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

package org.apache.hop.pipeline.transforms.terafast;

import org.apache.hop.core.Const;
import org.apache.hop.core.SourceToTargetMapping;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.IPluginProperty;
import org.apache.hop.core.util.KeyValue;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.SimpleFileSelection;
import org.apache.hop.ui.core.dialog.EnterMappingDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.*;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.pipeline.transform.ITableItemInsertListener;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

import java.util.List;
import java.util.*;
import org.apache.hop.core.variables.IVariables;

public class TeraFastDialog extends BaseTransformDialog implements ITransformDialog {

  private static final Class<?> PKG = TeraFastMeta.class; // For Translator

  private static final int FORM_ATTACHMENT_OFFSET = 100;

  private static final int FORM_ATTACHMENT_FACTOR = -15;

  private final TeraFastMeta meta;

  private MetaSelectionLine<DatabaseMeta> wConnection;

  private TextVar wTable;

  private TextVar wFastLoadPath;

  private Button wbFastLoadPath;

  private TextVar wControlFile;

  private Button wbControlFile;

  private TextVar wDataFile;

  private Button wbDataFile;

  private Button wbLogFile;

  private TextVar wLogFile;

  private TextVar wErrLimit;

  private TextVar wSessions;

  private Button wUseControlFile;

  private Button wVariableSubstitution;

  private Button wbTruncateTable;

  private TableView wReturn;

  private Button wGetLU;

  private Button wDoMapping;

  private ColumnInfo[] ciReturn;

  private final Map<String, Integer> inputFields = new HashMap<>();

  /**
   * List of ColumnInfo that should have the field names of the selected database table.
   */
  private final List<ColumnInfo> tableFieldColumns = new ArrayList<>();

  /**
   * Constructor.
   *
   * @param parent       parent shell.
   * @param baseTransformMeta transform meta
   * @param pipelineMeta    transaction meta
   * @param transformName     name of transform.
   */
  public TeraFastDialog( final Shell parent, IVariables variables, final Object baseTransformMeta, final PipelineMeta pipelineMeta,
                         final String transformName ) {
    super( parent, variables, (BaseTransformMeta) baseTransformMeta, pipelineMeta, transformName );
    this.meta = (TeraFastMeta) baseTransformMeta;
  }

  /**
   * @param property property.
   * @param textVar  text varibale.
   */
  public static void setTextIfPropertyValue( final IPluginProperty property, final TextVar textVar ) {
    if ( property.evaluate() ) {
      textVar.setText( ( (KeyValue<?>) property ).stringValue() );
    }
  }

  /**
   * @param property property.
   * @param combo    text variable.
   */
  public static void setTextIfPropertyValue( final IPluginProperty property, final CCombo combo ) {
    if ( property.evaluate() ) {
      combo.setText( ( (KeyValue<?>) property ).stringValue() );
    }
  }

  /**
   * {@inheritDoc}
   *
   * @see ITransformDialog#open()
   */
  public String open() {
    this.changed = this.meta.hasChanged();

    final Shell parent = getParent();
    final Display display = parent.getDisplay();

    this.shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX );
    this.props.setLook( this.shell );
    setShellImage( this.shell, this.meta );

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    this.shell.setLayout( formLayout );
    this.shell.setText( BaseMessages.getString( PKG, "TeraFastDialog.Shell.Title" ) );

    buildUi();
    listeners();

    //
    // Search the fields in the background
    //

    final Runnable runnable = () -> {
      final TransformMeta transformMetaSearchFields =
        TeraFastDialog.this.pipelineMeta.findTransform( TeraFastDialog.this.transformName );
      if ( transformMetaSearchFields == null ) {
        return;
      }
      try {
        final IRowMeta row = TeraFastDialog.this.pipelineMeta.getPrevTransformFields( variables, transformMetaSearchFields );

        // Remember these fields...
        for ( int i = 0; i < row.size(); i++ ) {
          TeraFastDialog.this.inputFields.put( row.getValueMeta( i ).getName(), i);
        }

        setComboBoxes();
      } catch ( HopException e ) {
        TeraFastDialog.this.logError( BaseMessages.getString( PKG, "System.Dialog.GetFieldsFailed.Message" ) );
      }
    };
    new Thread( runnable ).start();
    //
    // // Set the shell size, based upon previous time...
    setSize();

    getData();
    this.meta.setChanged( this.changed );
    disableInputs();

    this.shell.open();
    while ( !this.shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return this.transformName;
  }

  /**
   * ...
   */
  protected void setComboBoxes() {
    // Something was changed in the row.
    //
    final Map<String, Integer> fields = new HashMap<>();

    // Add the currentMeta fields...
    fields.putAll( this.inputFields );

    Set<String> keySet = fields.keySet();
    List<String> entries = new ArrayList<>( keySet );

    String[] fieldNames = entries.toArray( new String[ entries.size() ] );
    Const.sortStrings( fieldNames );
    // return fields
    this.ciReturn[ 1 ].setComboValues( fieldNames );
  }

  /**
   * Set data values in dialog.
   */
  public void getData() {
    setTextIfPropertyValue( this.meta.getFastloadPath(), this.wFastLoadPath );
    setTextIfPropertyValue( this.meta.getControlFile(), this.wControlFile );
    setTextIfPropertyValue( this.meta.getDataFile(), this.wDataFile );
    setTextIfPropertyValue( this.meta.getLogFile(), this.wLogFile );
    setTextIfPropertyValue( this.meta.getTargetTable(), this.wTable );
    setTextIfPropertyValue( this.meta.getErrorLimit(), this.wErrLimit );
    setTextIfPropertyValue( this.meta.getSessions(), this.wSessions );
    setTextIfPropertyValue( this.meta.getConnectionName(), this.wConnection.getComboWidget() );
    this.wbTruncateTable.setSelection( this.meta.getTruncateTable().getValue() );
    this.wUseControlFile.setSelection( this.meta.getUseControlFile().getValue() );
    this.wVariableSubstitution.setSelection( this.meta.getVariableSubstitution().getValue() );

    if ( this.meta.getTableFieldList().getValue().size() == this.meta.getStreamFieldList().getValue().size() ) {
      for ( int i = 0; i < this.meta.getTableFieldList().getValue().size(); i++ ) {
        TableItem item = this.wReturn.table.getItem( i );
        item.setText( 1, this.meta.getTableFieldList().getValue().get( i ) );
        item.setText( 2, this.meta.getStreamFieldList().getValue().get( i ) );
      }
    }
    if ( this.meta.getDbMeta() != null ) {
      this.wConnection.setText( this.meta.getConnectionName().getValue() );
    }
    setTableFieldCombo();

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  /**
   * Configure listeners.
   */
  private void listeners() {
    this.lsCancel = event -> cancel();
    this.lsOk = event -> ok();

    this.wCancel.addListener( SWT.Selection, this.lsCancel );
    this.wOk.addListener( SWT.Selection, this.lsOk );

    this.lsDef = new SelectionAdapter() {
      @Override
      public void widgetDefaultSelected( final SelectionEvent event ) {
        ok();
      }
    };

    Listener lsGetLU = event -> getUpdate();

    this.wGetLU.addListener( SWT.Selection, lsGetLU);
    this.wTransformName.addSelectionListener( this.lsDef );
    final String allFileTypes = BaseMessages.getString( PKG, "TeraFastDialog.Filetype.All" );
    this.wbControlFile
      .addSelectionListener( new SimpleFileSelection( this.shell, this.wControlFile, allFileTypes ) );
    this.wbDataFile.addSelectionListener( new SimpleFileSelection( this.shell, this.wDataFile, allFileTypes ) );
    this.wbFastLoadPath.addSelectionListener( new SimpleFileSelection(
      this.shell, this.wFastLoadPath, allFileTypes ) );
    this.wbLogFile.addSelectionListener( new SimpleFileSelection( this.shell, this.wLogFile, allFileTypes ) );

    this.wDoMapping.addListener( SWT.Selection, event -> generateMappings() );

//    this.wAscLink.addListener( SWT.Selection, new Listener() {
//      public void handleEvent( final Event event ) {
//        Program.launch( event.text );
//      }
//    } );

    // Detect X or ALT-F4 or something that kills this window...
    this.shell.addShellListener( new ShellAdapter() {
      @Override
      public void shellClosed( final ShellEvent event ) {
        cancel();
      }
    } );
  }

  /**
   * Reads in the fields from the previous transforms and from the ONE next transform and opens an EnterMappingDialog with this
   * information. After the user did the mapping, those information is put into the Select/Rename table.
   */
  public void generateMappings() {

    // Determine the source and target fields...
    //
    IRowMeta sourceFields;
    IRowMeta targetFields;

    try {
      sourceFields = this.pipelineMeta.getPrevTransformFields( variables, this.transformMeta );
    } catch ( HopException e ) {
      new ErrorDialog( this.shell,
        BaseMessages.getString( PKG, "TeraFastDialog.DoMapping.UnableToFindSourceFields.Title" ),
        BaseMessages.getString( PKG, "TeraFastDialog.DoMapping.UnableToFindSourceFields.Message" ), e );
      return;
    }
    // refresh fields
    this.meta.getTargetTable().setValue( this.wTable.getText() );
    try {
      targetFields = this.meta.getRequiredFields( variables );
    } catch ( HopException e ) {
      new ErrorDialog( this.shell,
        BaseMessages.getString( PKG, "TeraFastDialog.DoMapping.UnableToFindTargetFields.Title" ),
        BaseMessages.getString( PKG, "TeraFastDialog.DoMapping.UnableToFindTargetFields.Message" ), e );
      return;
    }

    String[] inputNames = new String[ sourceFields.size() ];
    for ( int i = 0; i < sourceFields.size(); i++ ) {
      IValueMeta value = sourceFields.getValueMeta( i );
      inputNames[ i ] = value.getName();
    }

    // Create the existing mapping list...
    //
    List<SourceToTargetMapping> mappings = new ArrayList<>();
    StringBuilder missingSourceFields = new StringBuilder();
    StringBuilder missingTargetFields = new StringBuilder();

    int nrFields = this.wReturn.nrNonEmpty();
    for ( int i = 0; i < nrFields; i++ ) {
      TableItem item = this.wReturn.getNonEmpty( i );
      String source = item.getText( 2 );
      String target = item.getText( 1 );

      int sourceIndex = sourceFields.indexOfValue( source );
      if ( sourceIndex < 0 ) {
        missingSourceFields.append( Const.CR + "   " + source + " --> " + target );
      }
      int targetIndex = targetFields.indexOfValue( target );
      if ( targetIndex < 0 ) {
        missingTargetFields.append( Const.CR + "   " + source + " --> " + target );
      }
      if ( sourceIndex < 0 || targetIndex < 0 ) {
        continue;
      }

      SourceToTargetMapping mapping = new SourceToTargetMapping( sourceIndex, targetIndex );
      mappings.add( mapping );
    }

    EnterMappingDialog d =
      new EnterMappingDialog( TeraFastDialog.this.shell, sourceFields.getFieldNames(), targetFields
        .getFieldNames(), mappings );
    mappings = d.open();

    // mappings == null if the user pressed cancel
    //
    if ( mappings != null ) {
      // Clear and re-populate!
      //
      this.wReturn.table.removeAll();
      this.wReturn.table.setItemCount( mappings.size() );
      for ( int i = 0; i < mappings.size(); i++ ) {
        SourceToTargetMapping mapping = mappings.get( i );
        TableItem item = this.wReturn.table.getItem( i );
        item.setText( 2, sourceFields.getValueMeta( mapping.getSourcePosition() ).getName() );
        item.setText( 1, targetFields.getValueMeta( mapping.getTargetPosition() ).getName() );
      }
      this.wReturn.setRowNums();
      this.wReturn.optWidth( true );
    }
  }

  /**
   * ...
   */
  public void getUpdate() {
    try {
      final IRowMeta row = this.pipelineMeta.getPrevTransformFields( variables, this.transformName );
      if ( row != null ) {
        ITableItemInsertListener listener = ( tableItem, value ) -> {
          // possible to check format of input fields
          return true;
        };
        BaseTransformDialog.getFieldsFromPrevious(
          row, this.wReturn, 1, new int[] { 1, 2 }, new int[] {}, -1, -1, listener );
      }
    } catch ( HopException ke ) {
      new ErrorDialog(
        this.shell, BaseMessages.getString( PKG, "TeraFastDialog.FailedToGetFields.DialogTitle" ), BaseMessages
        .getString( PKG, "TeraFastDialog.FailedToGetFields.DialogMessage" ), ke );
    }
  }

  /**
   * Dialog is closed.
   */
  public void cancel() {
    this.transformName = null;
    this.meta.setChanged( this.changed );
    dispose();
  }

  /**
   * Ok clicked.
   */
  public void ok() {
    this.transformName = this.wTransformName.getText(); // return value
    this.meta.getUseControlFile().setValue( this.wUseControlFile.getSelection() );
    this.meta.getVariableSubstitution().setValue( this.wVariableSubstitution.getSelection() );
    this.meta.getControlFile().setValue( this.wControlFile.getText() );
    this.meta.getFastloadPath().setValue( this.wFastLoadPath.getText() );
    this.meta.getDataFile().setValue( this.wDataFile.getText() );
    this.meta.getLogFile().setValue( this.wLogFile.getText() );
    this.meta.getErrorLimit().setValue( Const.toInt( this.wErrLimit.getText(), TeraFastMeta.DEFAULT_ERROR_LIMIT ) );
    this.meta.getSessions().setValue( Const.toInt( this.wSessions.getText(), TeraFastMeta.DEFAULT_SESSIONS ) );
    this.meta.getTargetTable().setValue( this.wTable.getText() );
    this.meta.getConnectionName().setValue( this.wConnection.getText() );
    this.meta.getTruncateTable().setValue(
      this.wbTruncateTable.getSelection() && this.wbTruncateTable.getEnabled() );
    this.meta.setDbMeta( this.pipelineMeta.findDatabase( this.wConnection.getText() ) );

    this.meta.getTableFieldList().getValue().clear();
    this.meta.getStreamFieldList().getValue().clear();
    int nrFields = this.wReturn.nrNonEmpty();
    for ( int i = 0; i < nrFields; i++ ) {
      TableItem item = this.wReturn.getNonEmpty( i );
      this.meta.getTableFieldList().getValue().add( item.getText( 1 ) );
      this.meta.getStreamFieldList().getValue().add( item.getText( 2 ) );
    }

    dispose();
  }

  /**
   * Build UI.
   */
  protected void buildUi() {
    final PluginWidgetFactory factory = new PluginWidgetFactory( this.shell, variables );
    factory.setMiddle( this.props.getMiddlePct() );

    // Buttons at the bottom
    this.wOk = factory.createPushButton( BaseMessages.getString( PKG, "System.Button.OK" ) );
    this.wCancel = factory.createPushButton( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    setButtonPositions( new Button[] { this.wOk, this.wCancel, }, factory.getMargin(), null );

    final ModifyListener lsMod = event -> getMeta().setChanged();
      final SelectionAdapter lsSel = new SelectionAdapter() {
        @Override
        public void widgetSelected( final SelectionEvent event ) {
          getMeta().setChanged();
        }
      };
    
    this.buildTransformNameLine( factory );
    this.buildUseControlFileLine( factory );
    this.buildControlFileLine( factory );
    this.buildVariableSubstitutionLine( factory );
    this.buildFastloadLine( factory );
    this.buildLogFileLine( factory );

    // Connection line
    this.wConnection = addConnectionLine( this.shell, this.wLogFile, meta.getDbMeta(), lsMod );
    this.buildTableLine( factory );
    this.buildTruncateTableLine( factory );
    this.buildDataFileLine( factory );
    this.buildSessionsLine( factory );
    this.buildErrorLimitLine( factory );
    this.buildFieldTable( factory );
    this.buildAscLink( factory );


    this.wTransformName.addModifyListener( lsMod );
    this.wControlFile.addModifyListener( lsMod );
    this.wFastLoadPath.addModifyListener( lsMod );
    this.wLogFile.addModifyListener( lsMod );
    this.wConnection.addModifyListener( lsMod );
    this.wTable.addModifyListener( lsMod );
    this.wDataFile.addModifyListener( lsMod );
    this.wSessions.addModifyListener( lsMod );
    this.wErrLimit.addModifyListener( lsMod );
    this.wbTruncateTable.addSelectionListener( lsSel );
    this.wUseControlFile.addSelectionListener( lsSel );
    this.wVariableSubstitution.addSelectionListener( lsSel );
    this.wReturn.addModifyListener( lsMod );
  }

  /**
   * @param factory factory to use.
   */
  protected void buildControlFileLine( final PluginWidgetFactory factory ) {
    final Control topControl = this.wUseControlFile;

    Label wlControlFile = factory.createRightLabel(BaseMessages.getString(PKG, "TeraFastDialog.ControlFile.Label"));
    this.props.setLook(wlControlFile);
    wlControlFile.setLayoutData( factory.createLabelLayoutData( topControl ) );

    this.wbControlFile = factory.createPushButton( BaseMessages.getString( PKG, "TeraFastDialog.Browse.Button" ) );
    this.props.setLook( this.wbControlFile );
    FormData formData = factory.createControlLayoutData( topControl );
    formData.left = null;
    this.wbControlFile.setLayoutData( formData );

    this.wControlFile = factory.createSingleTextVarLeft();
    this.props.setLook( this.wControlFile );
    formData = factory.createControlLayoutData( topControl );
    formData.right = new FormAttachment( this.wbControlFile, -factory.getMargin() );
    this.wControlFile.setLayoutData( formData );
  }

  /**
   * @param factory factory to use.
   */
  protected void buildFastloadLine( final PluginWidgetFactory factory ) {
    final Control topControl = this.wVariableSubstitution;

    Label wlFastLoadPath = factory.createRightLabel(BaseMessages.getString(PKG, "TeraFastDialog.FastloadPath.Label"));
    this.props.setLook(wlFastLoadPath);
    wlFastLoadPath.setLayoutData( factory.createLabelLayoutData( topControl ) );

    this.wbFastLoadPath = factory.createPushButton( BaseMessages.getString( PKG, "TeraFastDialog.Browse.Button" ) );
    this.props.setLook( this.wbFastLoadPath );
    FormData formData = factory.createControlLayoutData( topControl );
    formData.left = null;
    this.wbFastLoadPath.setLayoutData( formData );

    this.wFastLoadPath = factory.createSingleTextVarLeft();
    this.props.setLook( this.wFastLoadPath );
    formData = factory.createControlLayoutData( topControl );
    formData.right = new FormAttachment( this.wbFastLoadPath, -factory.getMargin() );
    this.wFastLoadPath.setLayoutData( formData );
  }

  /**
   * @param factory factory to use.
   */
  protected void buildUseControlFileLine( final PluginWidgetFactory factory ) {
    final Control topControl = this.wTransformName;

    Label wlUseControlFile = factory.createRightLabel(BaseMessages.getString(PKG, "TeraFastDialog.UseControlFile.Label"));
    this.props.setLook(wlUseControlFile);
    wlUseControlFile.setLayoutData( factory.createLabelLayoutData( topControl ) );

    this.wUseControlFile = new Button( this.shell, SWT.CHECK );
    this.props.setLook( this.wUseControlFile );
    this.wUseControlFile.setLayoutData( factory.createButtonLayoutData( wlUseControlFile ) );

    this.wUseControlFile.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( final SelectionEvent event ) {
        disableInputs();
      }
    } );
  }

  /**
   * @param factory factory to use.
   */
  protected void buildVariableSubstitutionLine( final PluginWidgetFactory factory ) {
    final Control topControl = this.wControlFile;

    Label wlVariableSubstitution = factory.createRightLabel(BaseMessages.getString(PKG, "TeraFastDialog.VariableSubstitution.Label"));
    this.props.setLook(wlVariableSubstitution);
    wlVariableSubstitution.setLayoutData( factory.createLabelLayoutData( topControl ) );

    this.wVariableSubstitution = new Button( this.shell, SWT.CHECK );
    this.props.setLook( this.wVariableSubstitution );
    this.wVariableSubstitution.setLayoutData( factory.createButtonLayoutData( wlVariableSubstitution ) );
  }

  /**
   * @param factory factory to use.
   */
  protected void buildLogFileLine( final PluginWidgetFactory factory ) {
    final Control topControl = this.wFastLoadPath;

    Label wlLogFile = factory.createRightLabel(BaseMessages.getString(PKG, "TeraFastDialog.LogFile.Label"));
    this.props.setLook(wlLogFile);
    wlLogFile.setLayoutData( factory.createLabelLayoutData( topControl ) );

    this.wbLogFile = factory.createPushButton( BaseMessages.getString( PKG, "TeraFastDialog.Browse.Button" ) );
    this.props.setLook( this.wbLogFile );
    FormData formData = factory.createControlLayoutData( topControl );
    formData.left = null;
    this.wbLogFile.setLayoutData( formData );

    this.wLogFile = factory.createSingleTextVarLeft();
    this.props.setLook( this.wLogFile );
    formData = factory.createControlLayoutData( topControl );
    formData.right = new FormAttachment( this.wbLogFile, -factory.getMargin() );
    this.wLogFile.setLayoutData( formData );
  }

  /**
   * Build transform name line.
   *
   * @param factory factory to use.
   */
  protected void buildTransformNameLine( final PluginWidgetFactory factory ) {
    this.wlTransformName = factory.createRightLabel( BaseMessages.getString( PKG, "TeraFastDialog.TransformName.Label" ) );
    this.props.setLook( this.wlTransformName );
    this.fdlTransformName = factory.createLabelLayoutData( null );
    this.wlTransformName.setLayoutData( this.fdlTransformName );

    this.wTransformName = factory.createSingleTextLeft( this.transformName );
    this.props.setLook( this.wTransformName );
    this.fdTransformName = factory.createControlLayoutData( null );
    this.wTransformName.setLayoutData( this.fdTransformName );
  }

  /**
   * @param factory factory to use.
   */
  protected void buildTableLine( final PluginWidgetFactory factory ) {
    final Control topControl = this.wConnection;

    Label wlTable = factory.createRightLabel(BaseMessages.getString(PKG, "TeraFastDialog.TargetTable.Label"));
    this.props.setLook(wlTable);
    wlTable.setLayoutData( factory.createLabelLayoutData( topControl ) );

    this.wTable = factory.createSingleTextVarLeft();
    this.props.setLook( this.wTable );
    this.wTable.setLayoutData( factory.createControlLayoutData( topControl ) );

    this.wTable.addFocusListener( new FocusAdapter() {
      @Override
      public void focusLost( final FocusEvent event ) {
        setTableFieldCombo();
      }
    } );
  }

  /**
   * @param factory factory to use.
   */
  protected void buildTruncateTableLine( final PluginWidgetFactory factory ) {
    final Control topControl = this.wTable;

    Label wlTruncateTable = factory.createRightLabel(BaseMessages.getString(PKG, "TeraFastDialog.TruncateTable.Label"));
    this.props.setLook(wlTruncateTable);
    wlTruncateTable.setLayoutData( factory.createLabelLayoutData( topControl ) );

    this.wbTruncateTable = new Button( this.shell, SWT.CHECK );
    this.props.setLook( this.wbTruncateTable );
    this.wbTruncateTable.setLayoutData( factory.createButtonLayoutData( wlTruncateTable ) );
  }

  /**
   * @param factory factory to use.
   */
  protected void buildDataFileLine( final PluginWidgetFactory factory ) {
    final Control topControl = this.wbTruncateTable;

    Label wlDataFile = factory.createRightLabel(BaseMessages.getString(PKG, "TeraFastDialog.DataFile.Label"));
    this.props.setLook(wlDataFile);
    wlDataFile.setLayoutData( factory.createLabelLayoutData( topControl ) );

    this.wbDataFile = factory.createPushButton( BaseMessages.getString( PKG, "TeraFastDialog.Browse.Button" ) );
    this.props.setLook( this.wbDataFile );
    FormData formData = factory.createControlLayoutData( topControl );
    formData.left = null;
    this.wbDataFile.setLayoutData( formData );

    this.wDataFile = factory.createSingleTextVarLeft();
    this.props.setLook( this.wDataFile );
    formData = factory.createControlLayoutData( topControl );
    formData.right = new FormAttachment( this.wbDataFile, -factory.getMargin() );
    this.wDataFile.setLayoutData( formData );
  }

  /**
   * @param factory factory to use.
   */
  protected void buildSessionsLine( final PluginWidgetFactory factory ) {
    final Control topControl = this.wDataFile;

    Label wlSessions = factory.createRightLabel(BaseMessages.getString(PKG, "TeraFastDialog.Sessions.Label"));
    this.props.setLook(wlSessions);
    wlSessions.setLayoutData( factory.createLabelLayoutData( topControl ) );

    this.wSessions = factory.createSingleTextVarLeft();
    this.props.setLook( this.wSessions );
    this.wSessions.setLayoutData( factory.createControlLayoutData( topControl ) );
  }

  /**
   * @param factory factory to use.
   */
  protected void buildErrorLimitLine( final PluginWidgetFactory factory ) {
    final Control topControl = this.wSessions;

    Label wlErrLimit = factory.createRightLabel(BaseMessages.getString(PKG, "TeraFastDialog.ErrLimit.Label"));
    this.props.setLook(wlErrLimit);
    wlErrLimit.setLayoutData( factory.createLabelLayoutData( topControl ) );

    this.wErrLimit = factory.createSingleTextVarLeft();
    this.props.setLook( this.wErrLimit );
    this.wErrLimit.setLayoutData( factory.createControlLayoutData( topControl ) );
  }

  /**
   * @param factory factory to use.
   */
  protected void buildAscLink( final PluginWidgetFactory factory ) {
    final Control topControl = this.wReturn;

    FormData formData = factory.createLabelLayoutData( topControl );
    formData.right = null;
  }

  /**
   * @param factory factory to use.
   */
  protected void buildFieldTable( final PluginWidgetFactory factory ) {
    final Control topControl = this.wErrLimit;

    Label wlReturn = factory.createLabel(SWT.NONE, BaseMessages.getString(PKG, "TeraFastDialog.Fields.Label"));
    this.props.setLook(wlReturn);
    wlReturn.setLayoutData( factory.createLabelLayoutData( topControl ) );

    final int upInsCols = 2;
    final int upInsRows;
    if ( this.meta.getTableFieldList().isEmpty() ) {
      upInsRows = 1;
    } else {
      upInsRows = this.meta.getTableFieldList().size();
    }

    this.ciReturn = new ColumnInfo[ upInsCols ];
    this.ciReturn[ 0 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "TeraFastDialog.ColumnInfo.TableField" ), ColumnInfo.COLUMN_TYPE_CCOMBO,
        new String[] { "" }, false );
    this.ciReturn[ 1 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "TeraFastDialog.ColumnInfo.StreamField" ), ColumnInfo.COLUMN_TYPE_CCOMBO,
        new String[] { "" }, false );
    this.tableFieldColumns.add( this.ciReturn[ 0 ] );
    this.wReturn = new TableView(
      variables, this.shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
        this.ciReturn, upInsRows, null, this.props );

    this.wGetLU = factory.createPushButton( BaseMessages.getString( PKG, "TeraFastDialog.GetFields.Label" ) );
    FormData fdGetLU = new FormData();
    fdGetLU.top = new FormAttachment(wlReturn, factory.getMargin() );
    fdGetLU.right = new FormAttachment( FORM_ATTACHMENT_OFFSET, 0 );
    this.wGetLU.setLayoutData(fdGetLU);

    this.wDoMapping = factory.createPushButton( BaseMessages.getString( PKG, "TeraFastDialog.EditMapping.Label" ) );
    FormData fdDoMapping = new FormData();
    fdDoMapping.top = new FormAttachment( this.wGetLU, factory.getMargin() );
    fdDoMapping.right = new FormAttachment( FORM_ATTACHMENT_OFFSET, 0 );
    this.wDoMapping.setLayoutData(fdDoMapping);

    FormData formData = new FormData();
    formData.left = new FormAttachment( 0, 0 );
    formData.top = new FormAttachment(wlReturn, factory.getMargin() );
    formData.right = new FormAttachment( this.wGetLU, -factory.getMargin() );
    formData.bottom = new FormAttachment( wOk, -2*props.getMargin() );
    this.wReturn.setLayoutData( formData );
  }


  /**
   * Disable inputs.
   */
  public void disableInputs() {
    boolean useControlFile = this.wUseControlFile.getSelection();
    this.wbControlFile.setEnabled( useControlFile );
    this.wControlFile.setEnabled( useControlFile );
    this.wDataFile.setEnabled( !useControlFile );
    this.wbDataFile.setEnabled( !useControlFile );
    this.wSessions.setEnabled( !useControlFile );
    this.wErrLimit.setEnabled( !useControlFile );
    this.wReturn.setEnabled( !useControlFile );
    this.wGetLU.setEnabled( !useControlFile );
    this.wDoMapping.setEnabled( !useControlFile );
    this.wTable.setEnabled( !useControlFile );
    this.wbTruncateTable.setEnabled( !useControlFile );
    this.wConnection.setEnabled( !useControlFile );
    this.wVariableSubstitution.setEnabled( useControlFile );
  }

  /**
   * ...
   */
  public void setTableFieldCombo() {
    clearColInfo();
    new FieldLoader( this ).start();
  }

  /**
   * Clear.
   */
  private void clearColInfo() {
    for (final ColumnInfo colInfo : this.tableFieldColumns) {
      colInfo.setComboValues(new String[]{});
    }
  }

  /**
   * @return the meta
   */
  public TeraFastMeta getMeta() {
    return this.meta;
  }

  private static final class FieldLoader extends Thread {

    private final TeraFastDialog dialog;

    /**
     * Constructor.
     *
     * @param dialog dialog to set.
     */
    public FieldLoader( final TeraFastDialog dialog ) {
      this.dialog = dialog;
    }

    /**
     * {@inheritDoc}
     *
     * @see Runnable#run()
     */
    @Override
    public void run() {
      try {
        final IRowMeta rowMeta = this.dialog.meta.getRequiredFields( variables );
        if ( rowMeta == null ) {
          return;
        }

        final String[] fieldNames = rowMeta.getFieldNames();
        if ( fieldNames == null ) {
          return;
        }
        for ( int i = 0; i < this.dialog.tableFieldColumns.size(); i++ ) {
          final ColumnInfo colInfo = this.dialog.tableFieldColumns.get( i );
          if ( this.dialog.shell.isDisposed() ) {
            return;
          }
          this.dialog.shell.getDisplay().asyncExec( () -> {
            if ( FieldLoader.this.dialog.shell.isDisposed() ) {
              return;
            }
            colInfo.setComboValues( fieldNames );
          } );
        }
      } catch ( HopException e ) {
        this.dialog.logError( this.toString(), "Error while reading fields", e );
        // ignore any errors here. drop downs will not be
        // filled, but no problem for the user
      }
    }
  }
}
