/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
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

package org.apache.hop.ui.pipeline.transforms.xbaseinput;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelinePreviewFactory;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.xbaseinput.XBaseInputMeta;
import org.apache.hop.ui.core.dialog.EnterNumberDialog;
import org.apache.hop.ui.core.dialog.EnterTextDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.PreviewRowsDialog;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.dialog.PipelinePreviewProgressDialog;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
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
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

import java.util.List;

public class XBaseInputDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = XBaseInputMeta.class; // for i18n purposes, needed by Translator!!

  private Label wlFilename;
  private Button wbFilename;
  private TextVar wFilename;
  private FormData fdlFilename, fdbFilename, fdFilename;

  private Group gAccepting;
  private FormData fdAccepting;

  private Label wlAccFilenames;
  private Button wAccFilenames;
  private FormData fdlAccFilenames, fdAccFilenames;

  private Label wlAccField;
  private Text wAccField;
  private FormData fdlAccField, fdAccField;

  private Label wlAccTransform;
  private CCombo wAccTransform;
  private FormData fdlAccTransform, fdAccTransform;

  private Label wlLimit;
  private Text wLimit;
  private FormData fdlLimit, fdLimit;

  private Label wlAddRownr;
  private Button wAddRownr;
  private FormData fdlAddRownr, fdAddRownr;

  private Label wlFieldRownr;
  private Text wFieldRownr;
  private FormData fdlFieldRownr, fdFieldRownr;

  private Label wlInclFilename;
  private Button wInclFilename;
  private FormData fdlInclFilename, fdInclFilename;

  private Label wlInclFilenameField;
  private Text wInclFilenameField;
  private FormData fdlInclFilenameField, fdInclFilenameField;

  private Label wlCharactersetName;
  private Text wCharactersetName;
  private FormData fdlCharactersetName, fdCharactersetName;

  private XBaseInputMeta input;
  private boolean backupChanged, backupAddRownr;

  public XBaseInputDialog( Shell parent, Object in, PipelineMeta tr, String sname ) {
    super( parent, (BaseTransformMeta) in, tr, sname );
    input = (XBaseInputMeta) in;
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
    backupAddRownr = input.isRowNrAdded();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "XBaseInputDialog.Dialog.Title" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // TransformName line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "System.Label.TransformName" ) );
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

    // Filename line
    wlFilename = new Label( shell, SWT.RIGHT );
    wlFilename.setText( BaseMessages.getString( PKG, "System.Label.Filename" ) );
    props.setLook( wlFilename );
    fdlFilename = new FormData();
    fdlFilename.left = new FormAttachment( 0, 0 );
    fdlFilename.top = new FormAttachment( wTransformName, margin );
    fdlFilename.right = new FormAttachment( middle, -margin );
    wlFilename.setLayoutData( fdlFilename );

    wbFilename = new Button( shell, SWT.PUSH | SWT.CENTER );
    props.setLook( wbFilename );
    wbFilename.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    fdbFilename = new FormData();
    fdbFilename.right = new FormAttachment( 100, 0 );
    fdbFilename.top = new FormAttachment( wTransformName, margin );
    wbFilename.setLayoutData( fdbFilename );

    wFilename = new TextVar( pipelineMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFilename );
    wFilename.addModifyListener( lsMod );
    fdFilename = new FormData();
    fdFilename.left = new FormAttachment( middle, 0 );
    fdFilename.right = new FormAttachment( wbFilename, -margin );
    fdFilename.top = new FormAttachment( wTransformName, margin );
    wFilename.setLayoutData( fdFilename );

    // Accepting filenames group
    //

    gAccepting = new Group( shell, SWT.SHADOW_ETCHED_IN );
    gAccepting.setText( BaseMessages.getString( PKG, "XBaseInputDialog.AcceptingGroup.Label" ) );
    FormLayout acceptingLayout = new FormLayout();
    acceptingLayout.marginWidth = 3;
    acceptingLayout.marginHeight = 3;
    gAccepting.setLayout( acceptingLayout );
    props.setLook( gAccepting );

    // Accept filenames from previous transforms?
    //
    wlAccFilenames = new Label( gAccepting, SWT.RIGHT );
    wlAccFilenames.setText( BaseMessages.getString( PKG, "XBaseInputDialog.AcceptFilenames.Label" ) );
    props.setLook( wlAccFilenames );
    fdlAccFilenames = new FormData();
    fdlAccFilenames.top = new FormAttachment( 0, margin );
    fdlAccFilenames.left = new FormAttachment( 0, 0 );
    fdlAccFilenames.right = new FormAttachment( middle, -margin );
    wlAccFilenames.setLayoutData( fdlAccFilenames );
    wAccFilenames = new Button( gAccepting, SWT.CHECK );
    wAccFilenames.setToolTipText( BaseMessages.getString( PKG, "XBaseInputDialog.AcceptFilenames.Tooltip" ) );
    props.setLook( wAccFilenames );
    fdAccFilenames = new FormData();
    fdAccFilenames.top = new FormAttachment( 0, margin );
    fdAccFilenames.left = new FormAttachment( middle, 0 );
    fdAccFilenames.right = new FormAttachment( 100, 0 );
    wAccFilenames.setLayoutData( fdAccFilenames );
    wAccFilenames.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        setFlags();
        input.setChanged();
      }
    } );

    // Which transform to read from?
    wlAccTransform = new Label( gAccepting, SWT.RIGHT );
    wlAccTransform.setText( BaseMessages.getString( PKG, "XBaseInputDialog.AcceptTransform.Label" ) );
    props.setLook( wlAccTransform );
    fdlAccTransform = new FormData();
    fdlAccTransform.top = new FormAttachment( wAccFilenames, margin );
    fdlAccTransform.left = new FormAttachment( 0, 0 );
    fdlAccTransform.right = new FormAttachment( middle, -margin );
    wlAccTransform.setLayoutData( fdlAccTransform );
    wAccTransform = new CCombo( gAccepting, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wAccTransform.setToolTipText( BaseMessages.getString( PKG, "XBaseInputDialog.AcceptTransform.Tooltip" ) );
    props.setLook( wAccTransform );
    fdAccTransform = new FormData();
    fdAccTransform.top = new FormAttachment( wAccFilenames, margin );
    fdAccTransform.left = new FormAttachment( middle, 0 );
    fdAccTransform.right = new FormAttachment( 100, 0 );
    wAccTransform.setLayoutData( fdAccTransform );

    // Which field?
    //
    wlAccField = new Label( gAccepting, SWT.RIGHT );
    wlAccField.setText( BaseMessages.getString( PKG, "XBaseInputDialog.AcceptField.Label" ) );
    props.setLook( wlAccField );
    fdlAccField = new FormData();
    fdlAccField.top = new FormAttachment( wAccTransform, margin );
    fdlAccField.left = new FormAttachment( 0, 0 );
    fdlAccField.right = new FormAttachment( middle, -margin );
    wlAccField.setLayoutData( fdlAccField );
    wAccField = new Text( gAccepting, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wAccField.setToolTipText( BaseMessages.getString( PKG, "XBaseInputDialog.AcceptField.Tooltip" ) );
    props.setLook( wAccField );
    fdAccField = new FormData();
    fdAccField.top = new FormAttachment( wAccTransform, margin );
    fdAccField.left = new FormAttachment( middle, 0 );
    fdAccField.right = new FormAttachment( 100, 0 );
    wAccField.setLayoutData( fdAccField );

    // Fill in the source transforms...
    List<TransformMeta> prevTransforms = pipelineMeta.findPreviousTransforms( pipelineMeta.findTransform( transformName ) );
    for ( TransformMeta prevTransform : prevTransforms ) {
      wAccTransform.add( prevTransform.getName() );
    }

    fdAccepting = new FormData();
    fdAccepting.left = new FormAttachment( middle, 0 );
    fdAccepting.right = new FormAttachment( 100, 0 );
    fdAccepting.top = new FormAttachment( wFilename, margin * 2 );
    // fdAccepting.bottom = new FormAttachment(wAccTransform, margin);
    gAccepting.setLayoutData( fdAccepting );

    // Limit input ...
    wlLimit = new Label( shell, SWT.RIGHT );
    wlLimit.setText( BaseMessages.getString( PKG, "XBaseInputDialog.LimitSize.Label" ) );
    props.setLook( wlLimit );
    fdlLimit = new FormData();
    fdlLimit.left = new FormAttachment( 0, 0 );
    fdlLimit.right = new FormAttachment( middle, -margin );
    fdlLimit.top = new FormAttachment( gAccepting, margin * 2 );
    wlLimit.setLayoutData( fdlLimit );
    wLimit = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wLimit );
    wLimit.addModifyListener( lsMod );
    fdLimit = new FormData();
    fdLimit.left = new FormAttachment( middle, 0 );
    fdLimit.top = new FormAttachment( gAccepting, margin * 2 );
    fdLimit.right = new FormAttachment( 100, 0 );
    wLimit.setLayoutData( fdLimit );

    // Add rownr (1...)?
    wlAddRownr = new Label( shell, SWT.RIGHT );
    wlAddRownr.setText( BaseMessages.getString( PKG, "XBaseInputDialog.AddRowNr.Label" ) );
    props.setLook( wlAddRownr );
    fdlAddRownr = new FormData();
    fdlAddRownr.left = new FormAttachment( 0, 0 );
    fdlAddRownr.top = new FormAttachment( wLimit, margin );
    fdlAddRownr.right = new FormAttachment( middle, -margin );
    wlAddRownr.setLayoutData( fdlAddRownr );
    wAddRownr = new Button( shell, SWT.CHECK );
    props.setLook( wAddRownr );
    wAddRownr.setToolTipText( BaseMessages.getString( PKG, "XBaseInputDialog.AddRowNr.Tooltip" ) );
    fdAddRownr = new FormData();
    fdAddRownr.left = new FormAttachment( middle, 0 );
    fdAddRownr.top = new FormAttachment( wLimit, margin );
    wAddRownr.setLayoutData( fdAddRownr );
    wAddRownr.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
        setFlags();
      }
    } );

    // FieldRownr input ...
    wlFieldRownr = new Label( shell, SWT.LEFT );
    wlFieldRownr.setText( BaseMessages.getString( PKG, "XBaseInputDialog.FieldnameOfRowNr.Label" ) );
    props.setLook( wlFieldRownr );
    fdlFieldRownr = new FormData();
    fdlFieldRownr.left = new FormAttachment( wAddRownr, margin );
    fdlFieldRownr.top = new FormAttachment( wLimit, margin );
    wlFieldRownr.setLayoutData( fdlFieldRownr );
    wFieldRownr = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFieldRownr );
    wFieldRownr.addModifyListener( lsMod );
    fdFieldRownr = new FormData();
    fdFieldRownr.left = new FormAttachment( wlFieldRownr, margin );
    fdFieldRownr.top = new FormAttachment( wLimit, margin );
    fdFieldRownr.right = new FormAttachment( 100, 0 );
    wFieldRownr.setLayoutData( fdFieldRownr );

    wlInclFilename = new Label( shell, SWT.RIGHT );
    wlInclFilename.setText( BaseMessages.getString( PKG, "XBaseInputDialog.InclFilename.Label" ) );
    props.setLook( wlInclFilename );
    fdlInclFilename = new FormData();
    fdlInclFilename.left = new FormAttachment( 0, 0 );
    fdlInclFilename.top = new FormAttachment( wFieldRownr, margin );
    fdlInclFilename.right = new FormAttachment( middle, -margin );
    wlInclFilename.setLayoutData( fdlInclFilename );
    wInclFilename = new Button( shell, SWT.CHECK );
    props.setLook( wInclFilename );
    wInclFilename.setToolTipText( BaseMessages.getString( PKG, "XBaseInputDialog.InclFilename.Tooltip" ) );
    fdInclFilename = new FormData();
    fdInclFilename.left = new FormAttachment( middle, 0 );
    fdInclFilename.top = new FormAttachment( wFieldRownr, margin );
    wInclFilename.setLayoutData( fdInclFilename );
    wInclFilename.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        input.setChanged();
        setFlags();
      }
    } );

    wlInclFilenameField = new Label( shell, SWT.LEFT );
    wlInclFilenameField.setText( BaseMessages.getString( PKG, "XBaseInputDialog.InclFilenameField.Label" ) );
    props.setLook( wlInclFilenameField );
    fdlInclFilenameField = new FormData();
    fdlInclFilenameField.left = new FormAttachment( wInclFilename, margin );
    fdlInclFilenameField.top = new FormAttachment( wFieldRownr, margin );
    wlInclFilenameField.setLayoutData( fdlInclFilenameField );
    wInclFilenameField = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wInclFilenameField );
    wInclFilenameField.addModifyListener( lsMod );
    fdInclFilenameField = new FormData();
    fdInclFilenameField.left = new FormAttachment( wlInclFilenameField, margin );
    fdInclFilenameField.top = new FormAttachment( wFieldRownr, margin );
    fdInclFilenameField.right = new FormAttachment( 100, 0 );
    wInclFilenameField.setLayoutData( fdInclFilenameField );

    // #CRQ-6087
    //
    wlCharactersetName = new Label( shell, SWT.RIGHT );
    wlCharactersetName.setText( BaseMessages.getString( PKG, "XBaseInputDialog.CharactersetName.Label" ) );
    props.setLook( wlCharactersetName );
    fdlCharactersetName = new FormData();
    fdlCharactersetName.left = new FormAttachment( 0, 0 );
    fdlCharactersetName.right = new FormAttachment( middle, -margin );
    fdlCharactersetName.top = new FormAttachment( wInclFilename, margin );
    wlCharactersetName.setLayoutData( fdlCharactersetName );
    wCharactersetName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wCharactersetName.setToolTipText( BaseMessages.getString( PKG, "XBaseInputDialog.CharactersetName.Tooltip" ) );
    props.setLook( wCharactersetName );
    wCharactersetName.addModifyListener( lsMod );
    fdCharactersetName = new FormData();
    fdCharactersetName.left = new FormAttachment( middle, 0 );
    fdCharactersetName.top = new FormAttachment( wInclFilename, margin );
    fdCharactersetName.right = new FormAttachment( 100, 0 );
    wCharactersetName.setLayoutData( fdCharactersetName );

    // Some buttons
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wPreview = new Button( shell, SWT.PUSH );
    wPreview.setText( BaseMessages.getString( PKG, "System.Button.Preview" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOk, wPreview, wCancel }, margin, wCharactersetName );

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
    lsOk = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };

    wCancel.addListener( SWT.Selection, lsCancel );
    wPreview.addListener( SWT.Selection, lsPreview );
    wOk.addListener( SWT.Selection, lsOk );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wTransformName.addSelectionListener( lsDef );
    wLimit.addSelectionListener( lsDef );
    wFieldRownr.addSelectionListener( lsDef );
    wAccField.addSelectionListener( lsDef );

    wFilename.addModifyListener( new ModifyListener() {
      public void modifyText( ModifyEvent arg0 ) {
        wFilename.setToolTipText( pipelineMeta.environmentSubstitute( wFilename.getText() ) );
      }
    } );

    wbFilename.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        FileDialog dialog = new FileDialog( shell, SWT.OPEN );
        dialog.setFilterExtensions( new String[] { "*.dbf;*.DBF", "*" } );
        if ( wFilename.getText() != null ) {
          dialog.setFileName( wFilename.getText() );
        }

        dialog.setFilterNames( new String[] {
          BaseMessages.getString( PKG, "XBaseInputDialog.Filter.DBaseFiles" ),
          BaseMessages.getString( PKG, "System.FileType.AllFiles" ) } );

        if ( dialog.open() != null ) {
          String str = dialog.getFilterPath() + Const.FILE_SEPARATOR + dialog.getFileName();
          wFilename.setText( str );
        }
      }
    } );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();
    input.setChanged( changed );

    // Set the shell size, based upon previous time...
    setSize();

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return transformName;
  }

  protected void setFlags() {
    wlFieldRownr.setEnabled( wAddRownr.getSelection() );
    wFieldRownr.setEnabled( wAddRownr.getSelection() );

    wlInclFilenameField.setEnabled( wInclFilename.getSelection() );
    wInclFilenameField.setEnabled( wInclFilename.getSelection() );

    wlFilename.setEnabled( !wAccFilenames.getSelection() );
    wFilename.setEnabled( !wAccFilenames.getSelection() );
    wbFilename.setEnabled( !wAccFilenames.getSelection() );
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    if ( input.getDbfFileName() != null ) {
      wFilename.setText( input.getDbfFileName() );
      wFilename.setToolTipText( pipelineMeta.environmentSubstitute( input.getDbfFileName() ) );
    }
    wFieldRownr.setText( Const.NVL( input.getRowNrField(), "" ) );

    wInclFilename.setSelection( input.includeFilename() );
    wInclFilenameField.setText( Const.NVL( input.getFilenameField(), "" ) );

    wAccFilenames.setSelection( input.isAcceptingFilenames() );
    wAccField.setText( Const.NVL( input.getAcceptingField(), "" ) );
    wAccTransform.setText( Const.NVL( input.getAcceptingTransform() == null ? "" : input.getAcceptingTransform().getName(), "" ) );
    wCharactersetName.setText( Const.NVL( input.getCharactersetName(), "" ) );

    setFlags();

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;
    input.setRowNrAdded( backupAddRownr );
    input.setChanged( backupChanged );
    dispose();
  }

  public void getInfo( XBaseInputMeta meta ) throws HopTransformException {
    // copy info to Meta class (input)
    meta.setDbfFileName( wFilename.getText() );
    meta.setRowLimit( Const.toInt( wLimit.getText(), 0 ) );
    meta.setRowNrAdded( wAddRownr.getSelection() );
    meta.setRowNrField( wFieldRownr.getText() );

    meta.setIncludeFilename( wInclFilename.getSelection() );
    meta.setFilenameField( wInclFilenameField.getText() );

    meta.setAcceptingFilenames( wAccFilenames.getSelection() );
    meta.setAcceptingField( wAccField.getText() );
    meta.setAcceptingTransform( pipelineMeta.findTransform( wAccTransform.getText() ) );
    meta.setCharactersetName( wCharactersetName.getText() );

    if ( Utils.isEmpty( meta.getDbfFileName() ) && !meta.isAcceptingFilenames() ) {
      throw new HopTransformException( BaseMessages.getString( PKG, "XBaseInputDialog.Exception.SpecifyAFileToUse" ) );
    }
  }

  private void ok() {
    if ( Utils.isEmpty( wTransformName.getText() ) ) {
      return;
    }

    try {
      transformName = wTransformName.getText(); // return value
      getInfo( input );
    } catch ( HopTransformException e ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( e.toString() );
      mb.setText( BaseMessages.getString( PKG, "System.Warning" ) );
      mb.open();
    }
    dispose();
  }

  // Preview the data
  private void preview() {
    // Create the XML input transform
    try {
      XBaseInputMeta oneMeta = new XBaseInputMeta();
      getInfo( oneMeta );

      if ( oneMeta.isAcceptingFilenames() ) {
        MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_INFORMATION );
        mb.setMessage( BaseMessages.getString( PKG, "XBaseInputDialog.Dialog.SpecifyASampleFile.Message" ) );
        mb.setText( BaseMessages.getString( PKG, "XBaseInputDialog.Dialog.SpecifyASampleFile.Title" ) );
        mb.open();
        return;
      }

      PipelineMeta previewMeta = PipelinePreviewFactory.generatePreviewTransformation(
        pipelineMeta,
        oneMeta,
        wTransformName.getText() );

      EnterNumberDialog numberDialog = new EnterNumberDialog( shell, props.getDefaultPreviewSize(),
        BaseMessages.getString( PKG, "XBaseInputDialog.PreviewSize.DialogTitle" ),
        BaseMessages.getString( PKG, "XBaseInputDialog.PreviewSize.DialogMessage" ) );
      int previewSize = numberDialog.open();
      if ( previewSize > 0 ) {
        PipelinePreviewProgressDialog progressDialog = new PipelinePreviewProgressDialog(
          shell,
          previewMeta,
          new String[] { wTransformName.getText() },
          new int[] { previewSize } );
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
          }
        }

        PreviewRowsDialog prd =
          new PreviewRowsDialog(
            shell, pipelineMeta, SWT.NONE, wTransformName.getText(), progressDialog.getPreviewRowsMeta( wTransformName
            .getText() ), progressDialog.getPreviewRows( wTransformName.getText() ), loggingText );
        prd.open();
      }
    } catch ( Exception e ) {
      new ErrorDialog( shell, BaseMessages.getString( PKG, "System.Dialog.PreviewError.Title" ), BaseMessages
        .getString( PKG, "System.Dialog.PreviewError.Message" ), e );
    }
  }
}
