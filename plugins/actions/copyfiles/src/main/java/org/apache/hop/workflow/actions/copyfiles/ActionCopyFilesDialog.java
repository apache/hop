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

package org.apache.hop.workflow.actions.copyfiles;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.ITextVarButtonRenderCallback;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.IActionDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.ToolItem;

import java.util.HashMap;
import java.util.Map;

/**
 * This dialog allows you to edit the Copy Files action settings.
 *
 * @author Samatar Hassan
 * @since 06-05-2007
 */
public class ActionCopyFilesDialog extends ActionDialog implements IActionDialog {
  private static final Class<?> PKG = ActionCopyFiles.class; // For Translator

  protected static final String[] FILETYPES = new String[] { BaseMessages.getString(
    PKG, "JobCopyFiles.Filetype.All" ) };

  public static final String LOCAL_ENVIRONMENT = "Local";
  public static final String STATIC_ENVIRONMENT = "<Static>";

  protected Text wName;

  protected Button wPrevious;
  protected Button wCopyEmptyFolders;
  protected Button wOverwriteFiles;
  protected Button wIncludeSubfolders;
  protected Button wRemoveSourceFiles;
  protected Button wAddFileToResult;
  protected Button wDestinationIsAFile;
  protected Button wCreateDestinationFolder;

  protected ActionCopyFiles action;
  protected Shell shell;

  protected boolean changed;

  private Label wlFields;

  protected TableView wFields;

  private ToolItem deleteToolItem; // Delete

  public ActionCopyFilesDialog( Shell parent, IAction action, WorkflowMeta workflowMeta ) {
    super( parent, workflowMeta );
    this.action = (ActionCopyFiles) action;

    if ( this.action.getName() == null ) {
      this.action.setName( BaseMessages.getString( PKG, "JobCopyFiles.Name.Default" ) );
    }
  }

  protected void initUI() {
    Shell parent = getParent();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.MIN | SWT.MAX | SWT.RESIZE );
    props.setLook( shell );
    Button helpButton = WorkflowDialog.setShellImage( shell, action );

    ModifyListener lsMod = e -> action.setChanged();
    changed = action.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "JobCopyFiles.Title" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Filename line
    Label wlName = new Label( shell, SWT.LEFT );
    wlName.setText( BaseMessages.getString( PKG, "JobCopyFiles.Name.Label" ) );
    props.setLook( wlName );
    FormData fdlName = new FormData();
    fdlName.left = new FormAttachment( 0, 0 );
    fdlName.right = new FormAttachment( middle, -margin );
    fdlName.top = new FormAttachment( 0, margin );
    wlName.setLayoutData( fdlName );
    wName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wName );
    wName.addModifyListener( lsMod );
    FormData fdName = new FormData();
    fdName.left = new FormAttachment( 0, 0 );
    fdName.top = new FormAttachment( wlName, margin );
    fdName.right = new FormAttachment( 40, 0 );
    wName.setLayoutData( fdName );
    Label wlIcon = new Label( shell, SWT.RIGHT );
    wlIcon.setImage( getImage() );
    props.setLook( wlIcon );
    FormData fdlIcon = new FormData();
    fdlIcon.top = new FormAttachment( 0, margin * 3 );
    fdlIcon.right = new FormAttachment( 100, -margin );
    wlIcon.setLayoutData( fdlIcon );

    Label lTopSeparator = new Label( shell, SWT.HORIZONTAL | SWT.SEPARATOR );
    FormData fdTopSeparator = new FormData();
    fdTopSeparator.top = new FormAttachment( wlIcon, margin );
    fdTopSeparator.left = new FormAttachment( 0, 0 );
    fdTopSeparator.right = new FormAttachment( 100, 0 );
    lTopSeparator.setLayoutData( fdTopSeparator );


    CTabFolder wTabFolder = new CTabFolder( shell, SWT.BORDER );
    props.setLook( wTabFolder, Props.WIDGET_STYLE_TAB );

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( lTopSeparator, margin * 3 );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.bottom = new FormAttachment( 100, -60 );
    wTabFolder.setLayoutData( fdTabFolder );

    // ///////////////////////////////////////////////////////////
    // / START OF FILES TAB
    // ///////////////////////////////////////////////////////////

    CTabItem wFilesTab = new CTabItem( wTabFolder, SWT.NONE );
    wFilesTab.setText( BaseMessages.getString( PKG, "JobCopyFiles.Tab.Files.Label" ) );

    Composite wFilesComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wFilesComp );

    FormLayout filesLayout = new FormLayout();
    filesLayout.marginWidth = 3;
    filesLayout.marginHeight = 3;
    wFilesComp.setLayout( filesLayout );

    FormData fdFilesComp = new FormData();
    fdFilesComp.left = new FormAttachment( 0, 0 );
    fdFilesComp.top = new FormAttachment( 0, 0 );
    fdFilesComp.right = new FormAttachment( 100, 0 );
    fdFilesComp.bottom = new FormAttachment( 100, 0 );
    wFilesComp.setLayoutData( fdFilesComp );

    wFilesComp.layout();
    wFilesTab.setControl( wFilesComp );

    // ///////////////////////////////////////////////////////////
    // / END OF FILES TAB
    // ///////////////////////////////////////////////////////////

    // ////////////////////////
    // START OF SETTINGS TAB ///
    // ////////////////////////

    CTabItem wSettingsTab = new CTabItem( wTabFolder, SWT.NONE );
    wSettingsTab.setText( BaseMessages.getString( PKG, "JobCopyFiles.Settings.Label" ) );

    Composite wSettingsComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wSettingsComp );

    FormLayout settingsLayout = new FormLayout();
    settingsLayout.marginWidth = 3;
    settingsLayout.marginHeight = 3;
    wSettingsComp.setLayout( settingsLayout );

    SelectionAdapter listener = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
      }
    };

    wIncludeSubfolders = createSettingsButton( wSettingsComp,
      BaseMessages.getString( PKG, "JobCopyFiles.IncludeSubfolders.Label" ),
      BaseMessages.getString( PKG, "JobCopyFiles.IncludeSubfolders.Tooltip" ),
      null, listener );

    wDestinationIsAFile = createSettingsButton( wSettingsComp,
      BaseMessages.getString( PKG, "JobCopyFiles.DestinationIsAFile.Label" ),
      BaseMessages.getString( PKG, "JobCopyFiles.DestinationIsAFile.Tooltip" ),
      wIncludeSubfolders, listener );

    wCopyEmptyFolders = createSettingsButton( wSettingsComp,
      BaseMessages.getString( PKG, "JobCopyFiles.CopyEmptyFolders.Label" ),
      BaseMessages.getString( PKG, "JobCopyFiles.CopyEmptyFolders.Tooltip" ),
      wDestinationIsAFile, listener );

    wCreateDestinationFolder = createSettingsButton(wSettingsComp,
      BaseMessages.getString( PKG, "JobCopyFiles.CreateDestinationFolder.Label" ),
      BaseMessages.getString( PKG, "JobCopyFiles.CreateDestinationFolder.Tooltip" ),
      wCopyEmptyFolders,
      listener );

    wOverwriteFiles = createSettingsButton( wSettingsComp,
      BaseMessages.getString( PKG, "JobCopyFiles.OverwriteFiles.Label" ),
      BaseMessages.getString( PKG, "JobCopyFiles.OverwriteFiles.Tooltip" ),
      wCreateDestinationFolder, listener );

    wRemoveSourceFiles = createSettingsButton( wSettingsComp,
      BaseMessages.getString( PKG, "JobCopyFiles.RemoveSourceFiles.Label" ),
      BaseMessages.getString( PKG, "JobCopyFiles.RemoveSourceFiles.Tooltip" ),
      wOverwriteFiles, listener );

    wPrevious = createSettingsButton( wSettingsComp,
      BaseMessages.getString( PKG, "JobCopyFiles.Previous.Label" ),
      BaseMessages.getString( PKG, "JobCopyFiles.Previous.Tooltip" ),
      wRemoveSourceFiles, listener );

    wAddFileToResult = createSettingsButton( wSettingsComp,
      BaseMessages.getString( PKG, "JobCopyFiles.AddFileToResult.Label" ),
      BaseMessages.getString( PKG, "JobCopyFiles.AddFileToResult.Tooltip" ),
      wPrevious, listener );

    FormData fdSettingsComp = new FormData();
    fdSettingsComp.left = new FormAttachment( 0, 0 );
    fdSettingsComp.top = new FormAttachment( 0, 0 );
    fdSettingsComp.right = new FormAttachment( 100, 0 );
    fdSettingsComp.bottom = new FormAttachment( 100, 0 );
    wSettingsComp.setLayoutData( fdSettingsComp );

    wSettingsComp.layout();
    wSettingsTab.setControl( wSettingsComp );
    props.setLook( wSettingsComp );

    // ///////////////////////////////////////////////////////////
    // / END OF SETTINGS TAB
    // ///////////////////////////////////////////////////////////

    ToolBar tb = new ToolBar( wFilesComp, SWT.HORIZONTAL | SWT.FLAT );
    props.setLook( tb );
    FormData fdTb = new FormData();
    fdTb.right = new FormAttachment( 100, 0 );
    fdTb.top = new FormAttachment( wFilesComp, margin );
    tb.setLayoutData( fdTb );

    deleteToolItem = new ToolItem( tb, SWT.PUSH );
    deleteToolItem.setImage( GuiResource.getInstance().getImageDelete() );
    deleteToolItem.setToolTipText( BaseMessages.getString( PKG, "JobCopyFiles.FilenameDelete.Tooltip" ) );
    deleteToolItem.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        int[] idx = wFields.getSelectionIndices();
        wFields.remove( idx );
        wFields.removeEmptyRows();
        wFields.setRowNums();
      }
    } );

    wlFields = new Label( wFilesComp, SWT.NONE );
    wlFields.setText( BaseMessages.getString( PKG, "JobCopyFiles.Fields.Label" ) );
    props.setLook( wlFields );
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment( 0, margin );
    fdlFields.right = new FormAttachment( middle, -margin );
    fdlFields.top = new FormAttachment( wFilesComp, 15 );
    wlFields.setLayoutData( fdlFields );

    int rows =
      action.sourceFileFolder == null ? 1 : ( action.sourceFileFolder.length == 0
        ? 0 : action.sourceFileFolder.length );
    final int FieldsRows = rows;

    ColumnInfo[] colinf =
      new ColumnInfo[] {
        new ColumnInfo( BaseMessages.getString( PKG, "JobCopyFiles.Fields.SourceEnvironment.Label" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, false, true ),
        new ColumnInfo( BaseMessages.getString( PKG, "JobCopyFiles.Fields.SourceFileFolder.Label" ),
          ColumnInfo.COLUMN_TYPE_TEXT_BUTTON, false ),
        new ColumnInfo( BaseMessages.getString( PKG, "JobCopyFiles.Fields.Wildcard.Label" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo( BaseMessages.getString( PKG, "JobCopyFiles.Fields.DestinationEnvironment.Label" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, false, true ),
        new ColumnInfo( BaseMessages.getString( PKG, "JobCopyFiles.Fields.DestinationFileFolder.Label" ),
          ColumnInfo.COLUMN_TYPE_TEXT_BUTTON, false ) };

    setComboValues( colinf[ 0 ] );

    ITextVarButtonRenderCallback callback = () -> {
      String envType = wFields.getActiveTableItem().getText( wFields.getActiveTableColumn() - 1 );
      return !STATIC_ENVIRONMENT.equalsIgnoreCase( envType );
    };

    colinf[ 1 ].setUsingVariables( true );
    colinf[ 1 ].setToolTip( BaseMessages.getString( PKG, "JobCopyFiles.Fields.SourceFileFolder.Tooltip" ) );
    colinf[ 1 ].setTextVarButtonSelectionListener( getFileSelectionAdapter() );
    colinf[ 1 ].setRenderTextVarButtonCallback( callback );

    colinf[ 2 ].setUsingVariables( true );
    colinf[ 2 ].setToolTip( BaseMessages.getString( PKG, "JobCopyFiles.Fields.Wildcard.Tooltip" ) );

    setComboValues( colinf[ 3 ] );

    colinf[ 4 ].setUsingVariables( true );
    colinf[ 4 ].setToolTip( BaseMessages.getString( PKG, "JobCopyFiles.Fields.DestinationFileFolder.Tooltip" ) );
    colinf[ 4 ].setTextVarButtonSelectionListener( getFileSelectionAdapter() );

    wFields =
      new TableView(
        variables, wFilesComp, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, FieldsRows, lsMod, props );

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, margin );
    fdFields.top = new FormAttachment( tb, margin );
    fdFields.right = new FormAttachment( 100, -margin );
    fdFields.bottom = new FormAttachment( 100, -margin );
    wFields.setLayoutData( fdFields );

    refreshArgFromPrevious();

    Button wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    Button wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    Label lBottomSeparator = new Label( shell, SWT.HORIZONTAL | SWT.SEPARATOR );
    FormData fdBottomSeparator = new FormData();
    fdBottomSeparator.top = new FormAttachment( wTabFolder, margin * 3 );
    fdBottomSeparator.left = new FormAttachment( 0, 0 );
    fdBottomSeparator.right = new FormAttachment( 100, 0 );
    lBottomSeparator.setLayoutData( fdBottomSeparator );

    BaseTransformDialog.positionBottomRightButtons( shell, new Button[] { wOk, wCancel }, margin, lBottomSeparator );
    FormData fdOk = (FormData) wOk.getLayoutData();
    FormData fdHelpButton = new FormData();
    fdHelpButton.top = fdOk.top;
    fdHelpButton.left = new FormAttachment( 0, margin );
    helpButton.setLayoutData( fdHelpButton );

    // Add listeners
    wCancel.addListener( SWT.Selection, ( Event e ) -> cancel());
    wOk.addListener( SWT.Selection, ( Event e ) -> ok());

    SelectionAdapter lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wName.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();
    wTabFolder.setSelection( 0 );

  }

  @Override
  public IAction open() {
    initUI();
    BaseTransformDialog.setSize( shell );
    shell.open();
    Display display = getParent().getDisplay();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return action;
  }

  protected Button createSettingsButton( Composite p, String text, String title, Control top, SelectionAdapter sa ) {
    Button button = new Button( p, SWT.CHECK );
    button.setText( text );
    button.setToolTipText( title );
    props.setLook( button );
    FormData fd = new FormData();
    fd.left = new FormAttachment( 0, Const.MARGIN * 2 );
    if ( top == null ) {
      fd.top = new FormAttachment( 0, 10 );
    } else {
      fd.top = new FormAttachment( top, 5 );
    }
    fd.right = new FormAttachment( 100, 0 );
    button.setLayoutData( fd );
    button.addSelectionListener( sa );
    return button;
  }

  protected SelectionAdapter getFileSelectionAdapter() {
    return new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
          String filename = BaseDialog.presentFileDialog( shell, null, null, true ); // all files
          if (filename!=null ) {
            wFields.getActiveTableItem().setText( wFields.getActiveTableColumn(), filename );
          }

      }
    };
  }

  private void refreshArgFromPrevious() {
    wlFields.setEnabled( !wPrevious.getSelection() );
    wFields.setEnabled( !wPrevious.getSelection() );
    deleteToolItem.setEnabled( !wPrevious.getSelection() );
  }

  public void dispose() {
    WindowProperty winprop = new WindowProperty( shell );
    props.setScreen( winprop );
    shell.dispose();
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    if ( action.getName() != null ) {
      wName.setText( action.getName() );
    }
    wCopyEmptyFolders.setSelection( action.copyEmptyFolders );

    if ( action.sourceFileFolder != null ) {
      for ( int i = 0; i < action.sourceFileFolder.length; i++ ) {
        TableItem ti = wFields.table.getItem( i );
        if ( action.sourceFileFolder[ i ] != null ) {
          String sourceUrl = action.sourceFileFolder[ i ];
          String clusterName = action.getConfigurationBy( sourceUrl );
          if ( clusterName != null ) {
            clusterName =
              clusterName.startsWith( ActionCopyFiles.LOCAL_SOURCE_FILE ) ? LOCAL_ENVIRONMENT : clusterName;
            clusterName =
              clusterName.startsWith( ActionCopyFiles.STATIC_SOURCE_FILE ) ? STATIC_ENVIRONMENT : clusterName;

            ti.setText( 1, clusterName );
            sourceUrl =
              clusterName.equals( LOCAL_ENVIRONMENT ) || clusterName.equals( STATIC_ENVIRONMENT ) ? sourceUrl
                : action.getUrlPath( sourceUrl );
          }
          if ( sourceUrl != null ) {
            sourceUrl = sourceUrl.replace( ActionCopyFiles.SOURCE_URL + i + "-", "" );
          } else {
            sourceUrl = "";
          }
          ti.setText( 2, sourceUrl );
        }
        if ( action.wildcard[ i ] != null ) {
          ti.setText( 3, action.wildcard[ i ] );
        }
        if ( action.destinationFileFolder[ i ] != null ) {
          String destinationURL = action.destinationFileFolder[ i ];
          String clusterName = action.getConfigurationBy( destinationURL );
          if ( clusterName != null ) {
            clusterName = clusterName.startsWith( ActionCopyFiles.LOCAL_DEST_FILE ) ? LOCAL_ENVIRONMENT : clusterName;
            clusterName =
              clusterName.startsWith( ActionCopyFiles.STATIC_DEST_FILE ) ? STATIC_ENVIRONMENT : clusterName;
            ti.setText( 4, clusterName );
            destinationURL =
              clusterName.equals( LOCAL_ENVIRONMENT ) || clusterName.equals( STATIC_ENVIRONMENT ) ? destinationURL
                : action.getUrlPath( destinationURL );
          }
          if ( destinationURL != null ) {
            destinationURL = destinationURL.replace( ActionCopyFiles.DEST_URL + i + "-", "" );
          } else {
            destinationURL = "";
          }
          ti.setText( 5, destinationURL );
        }
      }

      wFields.setRowNums();
      wFields.optWidth( true );
    }
    wPrevious.setSelection( action.argFromPrevious );
    wOverwriteFiles.setSelection( action.overwriteFiles );
    wIncludeSubfolders.setSelection( action.includeSubFolders );
    wRemoveSourceFiles.setSelection( action.removeSourceFiles );
    wDestinationIsAFile.setSelection( action.destinationIsAFile );
    wCreateDestinationFolder.setSelection( action.createDestinationFolder );

    wAddFileToResult.setSelection( action.addResultFilenames );

    wName.selectAll();
    wName.setFocus();
  }

  private void cancel() {
    action.setChanged( changed );
    action = null;
    dispose();
  }

  protected void ok() {
    if ( Utils.isEmpty( wName.getText() ) ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setText( BaseMessages.getString( PKG, "System.TransformActionNameMissing.Title" ) );
      mb.setMessage( BaseMessages.getString( PKG, "System.ActionNameMissing.Msg" ) );
      mb.open();
      return;
    }

    action.setName( wName.getText() );
    action.setCopyEmptyFolders( wCopyEmptyFolders.getSelection() );
    action.setOverwriteFiles( wOverwriteFiles.getSelection() );
    action.setIncludeSubFolders( wIncludeSubfolders.getSelection() );
    action.setArgFromPrevious( wPrevious.getSelection() );
    action.setRemoveSourceFiles( wRemoveSourceFiles.getSelection() );
    action.setAddResultFilenames( wAddFileToResult.getSelection() );
    action.setDestinationIsAFile( wDestinationIsAFile.getSelection() );
    action.setCreateDestinationFolder( wCreateDestinationFolder.getSelection() );

    int nrItems = wFields.nrNonEmpty();


    Map<String, String> sourceDestinationMappings = new HashMap<>();
    action.sourceFileFolder = new String[ nrItems ];
    action.destinationFileFolder = new String[ nrItems ];
    action.wildcard = new String[ nrItems ];

    for ( int i = 0; i < nrItems; i++ ) {
      String sourceNc = wFields.getNonEmpty( i ).getText( 1 );
      sourceNc = sourceNc.equals( LOCAL_ENVIRONMENT ) ? ActionCopyFiles.LOCAL_SOURCE_FILE + i : sourceNc;
      sourceNc = sourceNc.equals( STATIC_ENVIRONMENT ) ? ActionCopyFiles.STATIC_SOURCE_FILE + i : sourceNc;
      String source = wFields.getNonEmpty( i ).getText( 2 );
      String wild = wFields.getNonEmpty( i ).getText( 3 );
      String destNc = wFields.getNonEmpty( i ).getText( 4 );
      destNc = destNc.equals( LOCAL_ENVIRONMENT ) ? ActionCopyFiles.LOCAL_DEST_FILE + i : destNc;
      destNc = destNc.equals( STATIC_ENVIRONMENT ) ? ActionCopyFiles.STATIC_DEST_FILE + i : destNc;
      String dest = wFields.getNonEmpty( i ).getText( 5 );
      source = ActionCopyFiles.SOURCE_URL + i + "-" + source;
      dest = ActionCopyFiles.DEST_URL + i + "-" + dest;
      action.sourceFileFolder[ i ] = action.loadURL( source, sourceNc, getMetadataProvider(), sourceDestinationMappings );
      action.destinationFileFolder[ i ] = action.loadURL( dest, destNc, getMetadataProvider(), sourceDestinationMappings );
      action.wildcard[ i ] = wild;
    }
    action.setConfigurationMappings( sourceDestinationMappings );

    dispose();
  }

  protected Image getImage() {
    return GuiResource.getInstance().getImage( "ui/images/CPY.svg", ConstUi.LARGE_ICON_SIZE, ConstUi.LARGE_ICON_SIZE );
  }

  public boolean showFileButtons() {
    return true;
  }

  protected void setComboValues( ColumnInfo colInfo ) {
    String[] values = { LOCAL_ENVIRONMENT, STATIC_ENVIRONMENT };
    colInfo.setComboValues( values );
  }
}
