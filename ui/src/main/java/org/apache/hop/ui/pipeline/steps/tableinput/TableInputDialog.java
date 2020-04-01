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

package org.apache.hop.ui.pipeline.steps.tableinput;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelinePreviewFactory;
import org.apache.hop.pipeline.step.BaseStepMeta;
import org.apache.hop.pipeline.step.StepDialogInterface;
import org.apache.hop.pipeline.step.StepMeta;
import org.apache.hop.pipeline.step.errorhandling.StreamInterface;
import org.apache.hop.pipeline.steps.tableinput.TableInputMeta;
import org.apache.hop.ui.core.database.dialog.DatabaseExplorerDialog;
import org.apache.hop.ui.core.dialog.EnterNumberDialog;
import org.apache.hop.ui.core.dialog.EnterTextDialog;
import org.apache.hop.ui.core.dialog.PreviewRowsDialog;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.StyledTextComp;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.file.job.HopGuiJobGraph;
import org.apache.hop.ui.pipeline.dialog.PipelinePreviewProgressDialog;
import org.apache.hop.ui.pipeline.step.BaseStepDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.FocusAdapter;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.KeyAdapter;
import org.eclipse.swt.events.KeyEvent;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.MouseAdapter;
import org.eclipse.swt.events.MouseEvent;
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

import java.util.List;

public class TableInputDialog extends BaseStepDialog implements StepDialogInterface {
  private static Class<?> PKG = TableInputMeta.class; // for i18n purposes, needed by Translator!!

  private MetaSelectionLine<DatabaseMeta> wConnection;

  private Label wlSQL;
  private StyledTextComp wSQL;
  private FormData fdlSQL, fdSQL;

  private Label wlDatefrom;
  private CCombo wDatefrom;
  private FormData fdlDatefrom, fdDatefrom;
  private Listener lsDatefrom;

  private Label wlLimit;
  private TextVar wLimit;
  private FormData fdlLimit, fdLimit;

  private Label wlEachRow;
  private Button wEachRow;
  private FormData fdlEachRow, fdEachRow;

  private Label wlVariables;
  private Button wVariables;
  private FormData fdlVariables, fdVariables;

  private Label wlLazyConversion;
  private Button wLazyConversion;
  private FormData fdlLazyConversion, fdLazyConversion;

  private Button wbTable;
  private FormData fdbTable;
  private Listener lsbTable;

  private TableInputMeta input;
  private boolean changedInDialog;

  private Label wlPosition;
  private FormData fdlPosition;

  public TableInputDialog( Shell parent, Object in, PipelineMeta pipelineMeta, String sname ) {
    super( parent, (BaseStepMeta) in, pipelineMeta, sname );
    input = (TableInputMeta) in;
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, input );

    ModifyListener lsMod = new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        changedInDialog = false; // for prompting if dialog is simply closed
        input.setChanged();
      }
    };
    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "TableInputDialog.TableInput" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // Stepname line
    wlStepname = new Label( shell, SWT.RIGHT );
    wlStepname.setText( BaseMessages.getString( PKG, "TableInputDialog.StepName" ) );
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

    // Connection line
    wConnection = addConnectionLine( shell, wStepname, input.getDatabaseMeta(), lsMod );

    // Some buttons
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wPreview = new Button( shell, SWT.PUSH );
    wPreview.setText( BaseMessages.getString( PKG, "System.Button.Preview" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    // wHelp = createHelpButton(shell, stepMeta);

    setButtonPositions( new Button[] { wOK, wPreview, wCancel }, margin, null );

    // Limit input ...
    wlLimit = new Label( shell, SWT.RIGHT );
    wlLimit.setText( BaseMessages.getString( PKG, "TableInputDialog.LimitSize" ) );
    props.setLook( wlLimit );
    fdlLimit = new FormData();
    fdlLimit.left = new FormAttachment( 0, 0 );
    fdlLimit.right = new FormAttachment( middle, -margin );
    fdlLimit.bottom = new FormAttachment( wOK, -2 * margin );
    wlLimit.setLayoutData( fdlLimit );
    wLimit = new TextVar( pipelineMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wLimit );
    wLimit.addModifyListener( lsMod );
    fdLimit = new FormData();
    fdLimit.left = new FormAttachment( middle, 0 );
    fdLimit.right = new FormAttachment( 100, 0 );
    fdLimit.bottom = new FormAttachment( wOK, -2 * margin );
    wLimit.setLayoutData( fdLimit );

    // Execute for each row?
    wlEachRow = new Label( shell, SWT.RIGHT );
    wlEachRow.setText( BaseMessages.getString( PKG, "TableInputDialog.ExecuteForEachRow" ) );
    props.setLook( wlEachRow );
    fdlEachRow = new FormData();
    fdlEachRow.left = new FormAttachment( 0, 0 );
    fdlEachRow.right = new FormAttachment( middle, -margin );
    fdlEachRow.bottom = new FormAttachment( wLimit, -margin );
    wlEachRow.setLayoutData( fdlEachRow );
    wEachRow = new Button( shell, SWT.CHECK );
    props.setLook( wEachRow );
    fdEachRow = new FormData();
    fdEachRow.left = new FormAttachment( middle, 0 );
    fdEachRow.right = new FormAttachment( 100, 0 );
    fdEachRow.bottom = new FormAttachment( wLimit, -margin );
    wEachRow.setLayoutData( fdEachRow );
    SelectionAdapter lsSelMod = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        input.setChanged();
      }
    };
    wEachRow.addSelectionListener( lsSelMod );

    // Read date from...
    wlDatefrom = new Label( shell, SWT.RIGHT );
    wlDatefrom.setText( BaseMessages.getString( PKG, "TableInputDialog.InsertDataFromStep" ) );
    props.setLook( wlDatefrom );
    fdlDatefrom = new FormData();
    fdlDatefrom.left = new FormAttachment( 0, 0 );
    fdlDatefrom.right = new FormAttachment( middle, -margin );
    fdlDatefrom.bottom = new FormAttachment( wEachRow, -margin );
    wlDatefrom.setLayoutData( fdlDatefrom );
    wDatefrom = new CCombo( shell, SWT.BORDER );
    props.setLook( wDatefrom );

    List<StepMeta> previousSteps = pipelineMeta.findPreviousSteps( pipelineMeta.findStep( stepname ) );
    for ( StepMeta stepMeta : previousSteps ) {
      wDatefrom.add( stepMeta.getName() );
    }

    wDatefrom.addModifyListener( lsMod );
    fdDatefrom = new FormData();
    fdDatefrom.left = new FormAttachment( middle, 0 );
    fdDatefrom.right = new FormAttachment( 100, 0 );
    fdDatefrom.bottom = new FormAttachment( wEachRow, -margin );
    wDatefrom.setLayoutData( fdDatefrom );

    // Replace variables in SQL?
    //
    wlVariables = new Label( shell, SWT.RIGHT );
    wlVariables.setText( BaseMessages.getString( PKG, "TableInputDialog.ReplaceVariables" ) );
    props.setLook( wlVariables );
    fdlVariables = new FormData();
    fdlVariables.left = new FormAttachment( 0, 0 );
    fdlVariables.right = new FormAttachment( middle, -margin );
    fdlVariables.bottom = new FormAttachment( wDatefrom, -margin );
    wlVariables.setLayoutData( fdlVariables );
    wVariables = new Button( shell, SWT.CHECK );
    props.setLook( wVariables );
    fdVariables = new FormData();
    fdVariables.left = new FormAttachment( middle, 0 );
    fdVariables.right = new FormAttachment( 100, 0 );
    fdVariables.bottom = new FormAttachment( wDatefrom, -margin );
    wVariables.setLayoutData( fdVariables );
    wVariables.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        input.setChanged();
        setSQLToolTip();
      }
    } );

    // Lazy conversion?
    //
    wlLazyConversion = new Label( shell, SWT.RIGHT );
    wlLazyConversion.setText( BaseMessages.getString( PKG, "TableInputDialog.LazyConversion" ) );
    props.setLook( wlLazyConversion );
    fdlLazyConversion = new FormData();
    fdlLazyConversion.left = new FormAttachment( 0, 0 );
    fdlLazyConversion.right = new FormAttachment( middle, -margin );
    fdlLazyConversion.bottom = new FormAttachment( wVariables, -margin );
    wlLazyConversion.setLayoutData( fdlLazyConversion );
    wLazyConversion = new Button( shell, SWT.CHECK );
    props.setLook( wLazyConversion );
    fdLazyConversion = new FormData();
    fdLazyConversion.left = new FormAttachment( middle, 0 );
    fdLazyConversion.right = new FormAttachment( 100, 0 );
    fdLazyConversion.bottom = new FormAttachment( wVariables, -margin );
    wLazyConversion.setLayoutData( fdLazyConversion );
    wLazyConversion.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        input.setChanged();
        setSQLToolTip();
      }
    } );

    wlPosition = new Label( shell, SWT.NONE );
    props.setLook( wlPosition );
    fdlPosition = new FormData();
    fdlPosition.left = new FormAttachment( 0, 0 );
    fdlPosition.right = new FormAttachment( 100, 0 );
    fdlPosition.bottom = new FormAttachment( wLazyConversion, -margin );
    wlPosition.setLayoutData( fdlPosition );

    // Table line...
    wlSQL = new Label( shell, SWT.NONE );
    wlSQL.setText( BaseMessages.getString( PKG, "TableInputDialog.SQL" ) );
    props.setLook( wlSQL );
    fdlSQL = new FormData();
    fdlSQL.left = new FormAttachment( 0, 0 );
    fdlSQL.top = new FormAttachment( wConnection, margin * 2 );
    wlSQL.setLayoutData( fdlSQL );

    wbTable = new Button( shell, SWT.PUSH | SWT.CENTER );
    props.setLook( wbTable );
    wbTable.setText( BaseMessages.getString( PKG, "TableInputDialog.GetSQLAndSelectStatement" ) );
    fdbTable = new FormData();
    fdbTable.right = new FormAttachment( 100, 0 );
    fdbTable.top = new FormAttachment( wConnection, margin * 2 );
    wbTable.setLayoutData( fdbTable );

    wSQL =
      new StyledTextComp( pipelineMeta, shell, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL, "" );
    props.setLook( wSQL, Props.WIDGET_STYLE_FIXED );
    wSQL.addModifyListener( lsMod );
    fdSQL = new FormData();
    fdSQL.left = new FormAttachment( 0, 0 );
    fdSQL.top = new FormAttachment( wbTable, margin );
    fdSQL.right = new FormAttachment( 100, -2 * margin );
    fdSQL.bottom = new FormAttachment( wlPosition, -margin );
    wSQL.setLayoutData( fdSQL );
    wSQL.addModifyListener( new ModifyListener() {
      public void modifyText( ModifyEvent arg0 ) {
        setSQLToolTip();
        setPosition();
      }
    } );

    wSQL.addKeyListener( new KeyAdapter() {
      public void keyPressed( KeyEvent e ) {
        setPosition();
      }

      public void keyReleased( KeyEvent e ) {
        setPosition();
      }
    } );
    wSQL.addFocusListener( new FocusAdapter() {
      public void focusGained( FocusEvent e ) {
        setPosition();
      }

      public void focusLost( FocusEvent e ) {
        setPosition();
      }
    } );
    wSQL.addMouseListener( new MouseAdapter() {
      public void mouseDoubleClick( MouseEvent e ) {
        setPosition();
      }

      public void mouseDown( MouseEvent e ) {
        setPosition();
      }

      public void mouseUp( MouseEvent e ) {
        setPosition();
      }
    } );

    // Text Higlighting
    wSQL.addLineStyleListener( new SQLValuesHighlight() );

    // Add listeners
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };
    lsPreview = new Listener() {
      public void handleEvent( Event e ) {
        preview();
      }
    };
    lsOK = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };
    lsbTable = new Listener() {
      public void handleEvent( Event e ) {
        getSQL();
      }
    };
    lsDatefrom = new Listener() {
      public void handleEvent( Event e ) {
        setFlags();
      }
    };

    wCancel.addListener( SWT.Selection, lsCancel );
    wPreview.addListener( SWT.Selection, lsPreview );
    wOK.addListener( SWT.Selection, lsOK );
    wbTable.addListener( SWT.Selection, lsbTable );
    wDatefrom.addListener( SWT.Selection, lsDatefrom );
    wDatefrom.addListener( SWT.FocusOut, lsDatefrom );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wStepname.addSelectionListener( lsDef );
    wLimit.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        checkCancel( e );
      }
    } );

    getData();
    changedInDialog = false; // for prompting if dialog is simply closed
    input.setChanged( changed );

    // Set the shell size, based upon previous time...
    setSize();

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return stepname;
  }

  public void setPosition() {

    String scr = wSQL.getText();
    int linenr = wSQL.getLineAtOffset( wSQL.getCaretOffset() ) + 1;
    int posnr = wSQL.getCaretOffset();

    // Go back from position to last CR: how many positions?
    int colnr = 0;
    while ( posnr > 0 && scr.charAt( posnr - 1 ) != '\n' && scr.charAt( posnr - 1 ) != '\r' ) {
      posnr--;
      colnr++;
    }
    wlPosition.setText( BaseMessages.getString( PKG, "TableInputDialog.Position.Label", "" + linenr, "" + colnr ) );

  }

  protected void setSQLToolTip() {
    if ( wVariables.getSelection() ) {
      wSQL.setToolTipText( pipelineMeta.environmentSubstitute( wSQL.getText() ) );
    }
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    if ( input.getSQL() != null ) {
      wSQL.setText( input.getSQL() );
    }
    if ( input.getDatabaseMeta() != null ) {
      wConnection.setText( input.getDatabaseMeta().getName() );
    }
    wLimit.setText( Const.NVL( input.getRowLimit(), "" ) );

    StreamInterface infoStream = input.getStepIOMeta().getInfoStreams().get( 0 );
    if ( infoStream.getStepMeta() != null ) {
      wDatefrom.setText( infoStream.getStepname() );
      wEachRow.setSelection( input.isExecuteEachInputRow() );
    } else {
      wEachRow.setEnabled( false );
      wlEachRow.setEnabled( false );
    }

    wVariables.setSelection( input.isVariableReplacementActive() );
    wLazyConversion.setSelection( input.isLazyConversionActive() );

    setSQLToolTip();
    setFlags();

    wStepname.selectAll();
    wStepname.setFocus();
  }

  private void checkCancel( ShellEvent e ) {
    if ( changedInDialog ) {
      int save = HopGuiJobGraph.showChangedWarning( shell, wStepname.getText() );
      if ( save == SWT.CANCEL ) {
        e.doit = false;
      } else if ( save == SWT.YES ) {
        ok();
      } else {
        cancel();
      }
    } else {
      cancel();
    }
  }

  private void cancel() {
    stepname = null;
    input.setChanged( changed );
    dispose();
  }

  private void getInfo( TableInputMeta meta, boolean preview ) {
    meta.setSQL( preview && !Utils.isEmpty( wSQL.getSelectionText() ) ? wSQL.getSelectionText() : wSQL.getText() );
    meta.setDatabaseMeta( pipelineMeta.findDatabase( wConnection.getText() ) );
    meta.setRowLimit( wLimit.getText() );
    StreamInterface infoStream = input.getStepIOMeta().getInfoStreams().get( 0 );
    infoStream.setStepMeta( pipelineMeta.findStep( wDatefrom.getText() ) );
    meta.setExecuteEachInputRow( wEachRow.getSelection() );
    meta.setVariableReplacementActive( wVariables.getSelection() );
    meta.setLazyConversionActive( wLazyConversion.getSelection() );
  }

  private void ok() {
    if ( Utils.isEmpty( wStepname.getText() ) ) {
      return;
    }

    stepname = wStepname.getText(); // return value
    // copy info to TextFileInputMeta class (input)

    getInfo( input, false );

    if ( input.getDatabaseMeta() == null ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString( PKG, "TableInputDialog.SelectValidConnection" ) );
      mb.setText( BaseMessages.getString( PKG, "TableInputDialog.DialogCaptionError" ) );
      mb.open();
      return;
    }

    dispose();
  }

  private void getSQL() {
    DatabaseMeta inf = pipelineMeta.findDatabase( wConnection.getText() );
    if ( inf != null ) {
      DatabaseExplorerDialog std = new DatabaseExplorerDialog( shell, SWT.NONE, inf, pipelineMeta.getDatabases() );
      if ( std.open() ) {
        String sql =
          "SELECT *"
            + Const.CR + "FROM "
            + inf.getQuotedSchemaTableCombination( std.getSchemaName(), std.getTableName() ) + Const.CR;
        wSQL.setText( sql );

        MessageBox yn = new MessageBox( shell, SWT.YES | SWT.NO | SWT.CANCEL | SWT.ICON_QUESTION );
        yn.setMessage( BaseMessages.getString( PKG, "TableInputDialog.IncludeFieldNamesInSQL" ) );
        yn.setText( BaseMessages.getString( PKG, "TableInputDialog.DialogCaptionQuestion" ) );
        int id = yn.open();
        switch ( id ) {
          case SWT.CANCEL:
            break;
          case SWT.NO:
            wSQL.setText( sql );
            break;
          case SWT.YES:
            Database db = new Database( loggingObject, inf );
            db.shareVariablesWith( pipelineMeta );
            try {
              db.connect();
              RowMetaInterface fields = db.getQueryFields( sql, false );
              if ( fields != null ) {
                sql = "SELECT" + Const.CR;
                for ( int i = 0; i < fields.size(); i++ ) {
                  ValueMetaInterface field = fields.getValueMeta( i );
                  if ( i == 0 ) {
                    sql += "  ";
                  } else {
                    sql += ", ";
                  }
                  sql += inf.quoteField( field.getName() ) + Const.CR;
                }
                sql +=
                  "FROM "
                    + inf.getQuotedSchemaTableCombination( std.getSchemaName(), std.getTableName() )
                    + Const.CR;
                wSQL.setText( sql );
              } else {
                MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
                mb.setMessage( BaseMessages.getString( PKG, "TableInputDialog.ERROR_CouldNotRetrieveFields" )
                  + Const.CR + BaseMessages.getString( PKG, "TableInputDialog.PerhapsNoPermissions" ) );
                mb.setText( BaseMessages.getString( PKG, "TableInputDialog.DialogCaptionError2" ) );
                mb.open();
              }
            } catch ( HopException e ) {
              MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
              mb.setText( BaseMessages.getString( PKG, "TableInputDialog.DialogCaptionError3" ) );
              mb.setMessage( BaseMessages.getString( PKG, "TableInputDialog.AnErrorOccurred" )
                + Const.CR + e.getMessage() );
              mb.open();
            } finally {
              db.disconnect();
            }
            break;
          default:
            break;
        }
      }
    } else {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString( PKG, "TableInputDialog.ConnectionNoLongerAvailable" ) );
      mb.setText( BaseMessages.getString( PKG, "TableInputDialog.DialogCaptionError4" ) );
      mb.open();
    }

  }

  private void setFlags() {
    if ( !Utils.isEmpty( wDatefrom.getText() ) ) {
      // The foreach check box...
      wEachRow.setEnabled( true );
      wlEachRow.setEnabled( true );

      // The preview button...
      wPreview.setEnabled( false );
    } else {
      // The foreach check box...
      wEachRow.setEnabled( false );
      wEachRow.setSelection( false );
      wlEachRow.setEnabled( false );

      // The preview button...
      wPreview.setEnabled( true );
    }

  }

  /**
   * Preview the data generated by this step. This generates a pipeline using this step & a dummy and previews it.
   */
  private void preview() {
    // Create the table input reader step...
    TableInputMeta oneMeta = new TableInputMeta();
    getInfo( oneMeta, true );

    PipelineMeta previewMeta = PipelinePreviewFactory.generatePreviewPipeline( pipelineMeta, oneMeta, wStepname.getText() );

    EnterNumberDialog numberDialog = new EnterNumberDialog( shell, props.getDefaultPreviewSize(),
      BaseMessages.getString( PKG, "TableInputDialog.EnterPreviewSize" ),
      BaseMessages.getString( PKG, "TableInputDialog.NumberOfRowsToPreview" ) );
    int previewSize = numberDialog.open();
    if ( previewSize > 0 ) {
      PipelinePreviewProgressDialog progressDialog =
        new PipelinePreviewProgressDialog(
          shell, previewMeta, new String[] { wStepname.getText() }, new int[] { previewSize } );
      progressDialog.open();

      Pipeline pipeline = progressDialog.getPipeline();
      String loggingText = progressDialog.getLoggingText();

      if ( !progressDialog.isCancelled() ) {
        if ( pipeline.getResult() != null && pipeline.getResult().getNrErrors() > 0 ) {
          EnterTextDialog etd =
            new EnterTextDialog(
              shell, BaseMessages.getString( PKG, "System.Dialog.PreviewError.Title" ), BaseMessages
              .getString( PKG, "System.Dialog.PreviewError.Message" ), loggingText, true );
          etd.setReadOnly();
          etd.open();
        } else {
          PreviewRowsDialog prd =
            new PreviewRowsDialog(
              shell, pipelineMeta, SWT.NONE, wStepname.getText(), progressDialog.getPreviewRowsMeta( wStepname
              .getText() ), progressDialog.getPreviewRows( wStepname.getText() ), loggingText );
          prd.open();
        }
      }

    }
  }

}
