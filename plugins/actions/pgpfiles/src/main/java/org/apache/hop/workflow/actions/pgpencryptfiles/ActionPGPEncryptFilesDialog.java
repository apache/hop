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

package org.apache.hop.workflow.actions.pgpencryptfiles;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.annotations.PluginDialog;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.hopgui.HopGuiExtensionPoint;
import org.apache.hop.ui.hopgui.delegates.HopGuiDirectoryDialogExtension;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IActionDialog;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
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
import org.eclipse.swt.widgets.DirectoryDialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This dialog allows you to edit the Move Files action settings.
 *
 * @author Samatar Hassan
 * @since 20-02-2008
 */
@PluginDialog( 
		  id = "PGP_ENCRYPT_FILES", 
		  image = "PGPEncryptFiles.svg", 
		  pluginType = PluginDialog.PluginType.ACTION,
		  documentationUrl = "https://www.project-hop.org/manual/latest/plugins/actions/"
)
public class ActionPGPEncryptFilesDialog extends ActionDialog implements IActionDialog {
  private static Class<?> PKG = ActionPGPEncryptFiles.class; // for i18n purposes, needed by Translator!!

  private static final String[] FILETYPES = new String[] { BaseMessages.getString(
    PKG, "JobPGPEncryptFiles.Filetype.All" ) };

  private Label wlName;
  private Text wName;
  private FormData fdlName, fdName;

  private Label wlSourceFileFolder;
  private Button wbSourceFileFolder, wbDestinationFileFolder, wbSourceDirectory, wbDestinationDirectory;

  private TextVar wSourceFileFolder;

  private Label wlIncludeSubfolders;
  private Button wIncludeSubfolders;
  private FormData fdlIncludeSubfolders, fdIncludeSubfolders;

  private Button wOk, wCancel;
  private Listener lsOk, lsCancel;

  private ActionPGPEncryptFiles action;
  private Shell shell;

  private SelectionAdapter lsDef;

  private boolean changed;

  private Label wlPrevious;

  private Button wPrevious;

  private FormData fdlPrevious, fdPrevious;

  private Label wlFields;

  private TableView wFields;

  private FormData fdlFields, fdFields;

  private Group wSettings;
  private FormData fdSettings;

  private Label wlDestinationFileFolder;
  private TextVar wDestinationFileFolder;
  private FormData fdlDestinationFileFolder, fdDestinationFileFolder;

  private Label wlGpgExe;
  private TextVar wGpgExe;
  private FormData fdlGpgExe, fdGpgExe;
  private Button wbbGpgExe;
  private FormData fdbbGpgExe;

  private Label wlWildcard;
  private TextVar wWildcard;
  private FormData fdlWildcard, fdWildcard;

  private Button wbdSourceFileFolder; // Delete
  private Button wbeSourceFileFolder; // Edit
  private Button wbaSourceFileFolder; // Add or change

  private CTabFolder wTabFolder;
  private Composite wGeneralComp, wAdvancedComp, wDestinationFileComp;
  private CTabItem wGeneralTab, wAdvancedTab, wDestinationFileTab;
  private FormData fdGeneralComp, fdAdvancedComp, fdDestinationFileComp;
  private FormData fdTabFolder;

  private Label wlCreateMoveToFolder;
  private Button wCreateMoveToFolder;
  private FormData fdlCreateMoveToFolder, fdCreateMoveToFolder;

  // Add File to result

  private Group wFileResult;
  private FormData fdFileResult;

  private Group wSuccessOn;
  private FormData fdSuccessOn;

  private Label wlAddFileToResult;
  private Button wAddFileToResult;
  private FormData fdlAddFileToResult, fdAddFileToResult;

  private Label wlCreateDestinationFolder;
  private Button wCreateDestinationFolder;
  private FormData fdlCreateDestinationFolder, fdCreateDestinationFolder;

  private Label wlDestinationIsAFile;
  private Button wDestinationIsAFile;
  private FormData fdlDestinationIsAFile, fdDestinationIsAFile;
  private FormData fdbeSourceFileFolder, fdbaSourceFileFolder, fdbdSourceFileFolder;

  private Label wlSuccessCondition;
  private CCombo wSuccessCondition;
  private FormData fdlSuccessCondition, fdSuccessCondition;

  private Label wlNrErrorsLessThan;
  private TextVar wNrErrorsLessThan;
  private FormData fdlNrErrorsLessThan, fdNrErrorsLessThan;

  private Group wDestinationFile;
  private FormData fdDestinationFile;

  private Group wMoveToGroup;
  private FormData fdMoveToGroup;

  private Label wlAddDate;
  private Button wAddDate;
  private FormData fdlAddDate, fdAddDate;

  private Label wlAddTime;
  private Button wAddTime;
  private FormData fdlAddTime, fdAddTime;

  private Label wlSpecifyFormat;
  private Button wSpecifyFormat;
  private FormData fdlSpecifyFormat, fdSpecifyFormat;

  private Label wlDateTimeFormat;
  private CCombo wDateTimeFormat;
  private FormData fdlDateTimeFormat, fdDateTimeFormat;

  private Label wlMovedDateTimeFormat;
  private CCombo wMovedDateTimeFormat;
  private FormData fdlMovedDateTimeFormat, fdMovedDateTimeFormat;

  private Label wlAddDateBeforeExtension;
  private Button wAddDateBeforeExtension;
  private FormData fdlAddDateBeforeExtension, fdAddDateBeforeExtension;

  private Label wlAddMovedDateBeforeExtension;
  private Button wAddMovedDateBeforeExtension;
  private FormData fdlAddMovedDateBeforeExtension, fdAddMovedDateBeforeExtension;

  private Label wlDoNotKeepFolderStructure;
  private Button wDoNotKeepFolderStructure;
  private FormData fdlDoNotKeepFolderStructure, fdDoNotKeepFolderStructure;

  private Label wlIfFileExists;
  private CCombo wIfFileExists;
  private FormData fdlIfFileExists, fdIfFileExists;

  private Label wlIfMovedFileExists;
  private CCombo wIfMovedFileExists;
  private FormData fdlIfMovedFileExists, fdIfMovedFileExists;

  private Button wbDestinationFolder;
  private Label wlDestinationFolder;
  private TextVar wDestinationFolder;
  private FormData fdlDestinationFolder, fdDestinationFolder, fdbDestinationFolder;

  private Label wlAddMovedDate;
  private Button wAddMovedDate;
  private FormData fdlAddMovedDate, fdAddMovedDate;

  private Label wlAddMovedTime;
  private Button wAddMovedTime;
  private FormData fdlAddMovedTime, fdAddMovedTime;

  private Label wlSpecifyMoveFormat;
  private Button wSpecifyMoveFormat;
  private FormData fdlSpecifyMoveFormat, fdSpecifyMoveFormat;

  private Label wlasciiMode;
  private Button wasciiMode;
  private FormData fdlasciiMode, fdasciiMode;

  public ActionPGPEncryptFilesDialog( Shell parent, IAction action,
                                      WorkflowMeta workflowMeta ) {
    super( parent, action, workflowMeta );
    this.action = (ActionPGPEncryptFiles) action;

    if ( this.action.getName() == null ) {
      this.action.setName( BaseMessages.getString( PKG, "JobPGPEncryptFiles.Name.Default" ) );
    }
  }

  @Override
  public IAction open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.MIN | SWT.MAX | SWT.RESIZE );
    props.setLook( shell );
    WorkflowDialog.setShellImage( shell, action );

    ModifyListener lsMod = new ModifyListener() {
      @Override
      public void modifyText( ModifyEvent e ) {
        action.setChanged();
      }
    };
    changed = action.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "JobPGPEncryptFiles.Title" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Filename line
    wlName = new Label( shell, SWT.RIGHT );
    wlName.setText( BaseMessages.getString( PKG, "JobPGPEncryptFiles.Name.Label" ) );
    props.setLook( wlName );
    fdlName = new FormData();
    fdlName.left = new FormAttachment( 0, 0 );
    fdlName.right = new FormAttachment( middle, -margin );
    fdlName.top = new FormAttachment( 0, margin );
    wlName.setLayoutData( fdlName );
    wName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wName );
    wName.addModifyListener( lsMod );
    fdName = new FormData();
    fdName.left = new FormAttachment( middle, 0 );
    fdName.top = new FormAttachment( 0, margin );
    fdName.right = new FormAttachment( 100, 0 );
    wName.setLayoutData( fdName );

    wTabFolder = new CTabFolder( shell, SWT.BORDER );
    props.setLook( wTabFolder, Props.WIDGET_STYLE_TAB );

    // ////////////////////////
    // START OF GENERAL TAB ///
    // ////////////////////////

    wGeneralTab = new CTabItem( wTabFolder, SWT.NONE );
    wGeneralTab.setText( BaseMessages.getString( PKG, "JobPGPEncryptFiles.Tab.General.Label" ) );

    wGeneralComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wGeneralComp );

    FormLayout generalLayout = new FormLayout();
    generalLayout.marginWidth = 3;
    generalLayout.marginHeight = 3;
    wGeneralComp.setLayout( generalLayout );

    // SETTINGS grouping?
    // ////////////////////////
    // START OF SETTINGS GROUP
    //

    wSettings = new Group( wGeneralComp, SWT.SHADOW_NONE );
    props.setLook( wSettings );
    wSettings.setText( BaseMessages.getString( PKG, "JobPGPEncryptFiles.Settings.Label" ) );

    FormLayout groupLayout = new FormLayout();
    groupLayout.marginWidth = 10;
    groupLayout.marginHeight = 10;
    wSettings.setLayout( groupLayout );

    // GPG Program
    wlGpgExe = new Label( wSettings, SWT.RIGHT );
    wlGpgExe.setText( BaseMessages.getString( PKG, "JobPGPEncryptFiles.GpgExe.Label" ) );
    props.setLook( wlGpgExe );
    fdlGpgExe = new FormData();
    fdlGpgExe.left = new FormAttachment( 0, 0 );
    fdlGpgExe.top = new FormAttachment( wName, margin );
    fdlGpgExe.right = new FormAttachment( middle, -margin );
    wlGpgExe.setLayoutData( fdlGpgExe );

    // Browse Source files button ...
    wbbGpgExe = new Button( wSettings, SWT.PUSH | SWT.CENTER );
    props.setLook( wbbGpgExe );
    wbbGpgExe.setText( BaseMessages.getString( PKG, "JobPGPEncryptFiles.BrowseFiles.Label" ) );
    fdbbGpgExe = new FormData();
    fdbbGpgExe.right = new FormAttachment( 100, -margin );
    fdbbGpgExe.top = new FormAttachment( wName, margin );
    wbbGpgExe.setLayoutData( fdbbGpgExe );

    wbbGpgExe.addListener( SWT.Selection, e-> BaseDialog.presentFileDialog( shell, wGpgExe, workflowMeta,
      new String[] { "*" }, FILETYPES, true )
    );

    wGpgExe = new TextVar( workflowMeta, wSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wGpgExe.setToolTipText( BaseMessages.getString( PKG, "JobPGPEncryptFiles.GpgExe.Tooltip" ) );
    props.setLook( wGpgExe );
    wGpgExe.addModifyListener( lsMod );
    fdGpgExe = new FormData();
    fdGpgExe.left = new FormAttachment( middle, 0 );
    fdGpgExe.top = new FormAttachment( wName, margin );
    fdGpgExe.right = new FormAttachment( wbbGpgExe, -margin );
    wGpgExe.setLayoutData( fdGpgExe );

    wlasciiMode = new Label( wSettings, SWT.RIGHT );
    wlasciiMode.setText( BaseMessages.getString( PKG, "JobPGPEncryptFiles.asciiMode.Label" ) );
    props.setLook( wlasciiMode );
    fdlasciiMode = new FormData();
    fdlasciiMode.left = new FormAttachment( 0, 0 );
    fdlasciiMode.top = new FormAttachment( wGpgExe, margin );
    fdlasciiMode.right = new FormAttachment( middle, -margin );
    wlasciiMode.setLayoutData( fdlasciiMode );
    wasciiMode = new Button( wSettings, SWT.CHECK );
    props.setLook( wasciiMode );
    wasciiMode.setToolTipText( BaseMessages.getString( PKG, "JobPGPEncryptFiles.asciiMode.Tooltip" ) );
    fdasciiMode = new FormData();
    fdasciiMode.left = new FormAttachment( middle, 0 );
    fdasciiMode.top = new FormAttachment( wGpgExe, margin );
    fdasciiMode.right = new FormAttachment( 100, 0 );
    wasciiMode.setLayoutData( fdasciiMode );
    wasciiMode.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
      }
    } );

    wlIncludeSubfolders = new Label( wSettings, SWT.RIGHT );
    wlIncludeSubfolders.setText( BaseMessages.getString( PKG, "JobPGPEncryptFiles.IncludeSubfolders.Label" ) );
    props.setLook( wlIncludeSubfolders );
    fdlIncludeSubfolders = new FormData();
    fdlIncludeSubfolders.left = new FormAttachment( 0, 0 );
    fdlIncludeSubfolders.top = new FormAttachment( wasciiMode, margin );
    fdlIncludeSubfolders.right = new FormAttachment( middle, -margin );
    wlIncludeSubfolders.setLayoutData( fdlIncludeSubfolders );
    wIncludeSubfolders = new Button( wSettings, SWT.CHECK );
    props.setLook( wIncludeSubfolders );
    wIncludeSubfolders.setToolTipText( BaseMessages
      .getString( PKG, "JobPGPEncryptFiles.IncludeSubfolders.Tooltip" ) );
    fdIncludeSubfolders = new FormData();
    fdIncludeSubfolders.left = new FormAttachment( middle, 0 );
    fdIncludeSubfolders.top = new FormAttachment( wasciiMode, margin );
    fdIncludeSubfolders.right = new FormAttachment( 100, 0 );
    wIncludeSubfolders.setLayoutData( fdIncludeSubfolders );
    wIncludeSubfolders.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
        CheckIncludeSubFolders();
      }
    } );

    // previous
    wlPrevious = new Label( wSettings, SWT.RIGHT );
    wlPrevious.setText( BaseMessages.getString( PKG, "JobPGPEncryptFiles.Previous.Label" ) );
    props.setLook( wlPrevious );
    fdlPrevious = new FormData();
    fdlPrevious.left = new FormAttachment( 0, 0 );
    fdlPrevious.top = new FormAttachment( wIncludeSubfolders, margin );
    fdlPrevious.right = new FormAttachment( middle, -margin );
    wlPrevious.setLayoutData( fdlPrevious );
    wPrevious = new Button( wSettings, SWT.CHECK );
    props.setLook( wPrevious );
    wPrevious.setSelection( action.argFromPrevious );
    wPrevious.setToolTipText( BaseMessages.getString( PKG, "JobPGPEncryptFiles.Previous.Tooltip" ) );
    fdPrevious = new FormData();
    fdPrevious.left = new FormAttachment( middle, 0 );
    fdPrevious.top = new FormAttachment( wIncludeSubfolders, margin );
    fdPrevious.right = new FormAttachment( 100, 0 );
    wPrevious.setLayoutData( fdPrevious );
    wPrevious.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {

        RefreshArgFromPrevious();

      }
    } );
    fdSettings = new FormData();
    fdSettings.left = new FormAttachment( 0, margin );
    fdSettings.top = new FormAttachment( wName, margin );
    fdSettings.right = new FormAttachment( 100, -margin );
    wSettings.setLayoutData( fdSettings );

    // ///////////////////////////////////////////////////////////
    // / END OF SETTINGS GROUP
    // ///////////////////////////////////////////////////////////

    // SourceFileFolder line
    wlSourceFileFolder = new Label( wGeneralComp, SWT.RIGHT );
    wlSourceFileFolder.setText( BaseMessages.getString( PKG, "JobPGPEncryptFiles.SourceFileFolder.Label" ) );
    props.setLook( wlSourceFileFolder );
    FormData fdlSourceFileFolder = new FormData();
    fdlSourceFileFolder.left = new FormAttachment( 0, 0 );
    fdlSourceFileFolder.top = new FormAttachment( wSettings, 2 * margin );
    fdlSourceFileFolder.right = new FormAttachment( middle, -margin );
    wlSourceFileFolder.setLayoutData( fdlSourceFileFolder );

    // Browse Source folders button ...
    wbSourceDirectory = new Button( wGeneralComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbSourceDirectory );
    wbSourceDirectory.setText( BaseMessages.getString( PKG, "JobPGPEncryptFiles.BrowseFolders.Label" ) );
    FormData fdbSourceDirectory = new FormData();
    fdbSourceDirectory.right = new FormAttachment( 100, 0 );
    fdbSourceDirectory.top = new FormAttachment( wSettings, margin );
    wbSourceDirectory.setLayoutData( fdbSourceDirectory );
    wbSourceDirectory.addListener( SWT.Selection, e-> BaseDialog.presentDirectoryDialog( shell, wSourceFileFolder, workflowMeta ) );


    // Browse Source files button ...
    wbSourceFileFolder = new Button( wGeneralComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbSourceFileFolder );
    wbSourceFileFolder.setText( BaseMessages.getString( PKG, "JobPGPEncryptFiles.BrowseFiles.Label" ) );
    FormData fdbSourceFileFolder = new FormData();
    fdbSourceFileFolder.right = new FormAttachment( wbSourceDirectory, -margin );
    fdbSourceFileFolder.top = new FormAttachment( wSettings, margin );
    wbSourceFileFolder.setLayoutData( fdbSourceFileFolder );

    // Browse Destination file add button ...
    wbaSourceFileFolder = new Button( wGeneralComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbaSourceFileFolder );
    wbaSourceFileFolder.setText( BaseMessages.getString( PKG, "JobPGPEncryptFiles.FilenameAdd.Button" ) );
    fdbaSourceFileFolder = new FormData();
    fdbaSourceFileFolder.right = new FormAttachment( wbSourceFileFolder, -margin );
    fdbaSourceFileFolder.top = new FormAttachment( wSettings, margin );
    wbaSourceFileFolder.setLayoutData( fdbaSourceFileFolder );

    wSourceFileFolder = new TextVar( workflowMeta, wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wSourceFileFolder
      .setToolTipText( BaseMessages.getString( PKG, "JobPGPEncryptFiles.SourceFileFolder.Tooltip" ) );

    props.setLook( wSourceFileFolder );
    wSourceFileFolder.addModifyListener( lsMod );
    FormData fdSourceFileFolder = new FormData();
    fdSourceFileFolder.left = new FormAttachment( middle, 0 );
    fdSourceFileFolder.top = new FormAttachment( wSettings, 2 * margin );
    fdSourceFileFolder.right = new FormAttachment( wbSourceFileFolder, -55 );
    wSourceFileFolder.setLayoutData( fdSourceFileFolder );

    // Whenever something changes, set the tooltip to the expanded version:
    wSourceFileFolder.addModifyListener( e -> wSourceFileFolder.setToolTipText( workflowMeta.environmentSubstitute( wSourceFileFolder.getText() ) ) );

    wbSourceFileFolder.addListener( SWT.Selection, e-> BaseDialog.presentFileDialog( shell, wSourceFileFolder, workflowMeta,
      new String[] { "*" }, FILETYPES, true )
    );

    // Destination
    wlDestinationFileFolder = new Label( wGeneralComp, SWT.RIGHT );
    wlDestinationFileFolder.setText( BaseMessages
      .getString( PKG, "JobPGPEncryptFiles.DestinationFileFolder.Label" ) );
    props.setLook( wlDestinationFileFolder );
    fdlDestinationFileFolder = new FormData();
    fdlDestinationFileFolder.left = new FormAttachment( 0, 0 );
    fdlDestinationFileFolder.top = new FormAttachment( wSourceFileFolder, margin );
    fdlDestinationFileFolder.right = new FormAttachment( middle, -margin );
    wlDestinationFileFolder.setLayoutData( fdlDestinationFileFolder );

    // Browse Destination folders button ...
    wbDestinationDirectory = new Button( wGeneralComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbDestinationDirectory );
    wbDestinationDirectory.setText( BaseMessages.getString( PKG, "JobPGPEncryptFiles.BrowseFolders.Label" ) );
    FormData fdbDestinationDirectory = new FormData();
    fdbDestinationDirectory.right = new FormAttachment( 100, 0 );
    fdbDestinationDirectory.top = new FormAttachment( wSourceFileFolder, margin );
    wbDestinationDirectory.setLayoutData( fdbDestinationDirectory );
    wbDestinationDirectory.addListener( SWT.Selection, e-> BaseDialog.presentDirectoryDialog( shell, wDestinationFileFolder, workflowMeta ) );


    // Browse Destination file browse button ...
    wbDestinationFileFolder = new Button( wGeneralComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbDestinationFileFolder );
    wbDestinationFileFolder.setText( BaseMessages.getString( PKG, "JobPGPEncryptFiles.BrowseFiles.Label" ) );
    FormData fdbDestinationFileFolder = new FormData();
    fdbDestinationFileFolder.right = new FormAttachment( wbDestinationDirectory, -margin );
    fdbDestinationFileFolder.top = new FormAttachment( wSourceFileFolder, margin );
    wbDestinationFileFolder.setLayoutData( fdbDestinationFileFolder );

    wDestinationFileFolder = new TextVar( workflowMeta, wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wDestinationFileFolder.setToolTipText( BaseMessages.getString(
      PKG, "JobPGPEncryptFiles.DestinationFileFolder.Tooltip" ) );
    props.setLook( wDestinationFileFolder );
    wDestinationFileFolder.addModifyListener( lsMod );
    fdDestinationFileFolder = new FormData();
    fdDestinationFileFolder.left = new FormAttachment( middle, 0 );
    fdDestinationFileFolder.top = new FormAttachment( wSourceFileFolder, margin );
    fdDestinationFileFolder.right = new FormAttachment( wbSourceFileFolder, -55 );
    wDestinationFileFolder.setLayoutData( fdDestinationFileFolder );

    wbDestinationFileFolder.addListener( SWT.Selection, e-> BaseDialog.presentFileDialog( shell, wDestinationFileFolder, workflowMeta,
      new String[] { "*" }, FILETYPES, true )
    );

    // Buttons to the right of the screen...
    wbdSourceFileFolder = new Button( wGeneralComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbdSourceFileFolder );
    wbdSourceFileFolder.setText( BaseMessages.getString( PKG, "JobPGPEncryptFiles.FilenameDelete.Button" ) );
    wbdSourceFileFolder
      .setToolTipText( BaseMessages.getString( PKG, "JobPGPEncryptFiles.FilenameDelete.Tooltip" ) );
    fdbdSourceFileFolder = new FormData();
    fdbdSourceFileFolder.right = new FormAttachment( 100, 0 );
    fdbdSourceFileFolder.top = new FormAttachment( wDestinationFileFolder, 40 );
    wbdSourceFileFolder.setLayoutData( fdbdSourceFileFolder );

    wbeSourceFileFolder = new Button( wGeneralComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbeSourceFileFolder );
    wbeSourceFileFolder.setText( BaseMessages.getString( PKG, "JobPGPEncryptFiles.FilenameEdit.Button" ) );
    wbeSourceFileFolder.setToolTipText( BaseMessages.getString( PKG, "JobPGPEncryptFiles.FilenameEdit.Tooltip" ) );
    fdbeSourceFileFolder = new FormData();
    fdbeSourceFileFolder.right = new FormAttachment( 100, 0 );
    fdbeSourceFileFolder.left = new FormAttachment( wbdSourceFileFolder, 0, SWT.LEFT );
    fdbeSourceFileFolder.top = new FormAttachment( wbdSourceFileFolder, margin );
    wbeSourceFileFolder.setLayoutData( fdbeSourceFileFolder );

    // Wildcard
    wlWildcard = new Label( wGeneralComp, SWT.RIGHT );
    wlWildcard.setText( BaseMessages.getString( PKG, "JobPGPEncryptFiles.Wildcard.Label" ) );
    props.setLook( wlWildcard );
    fdlWildcard = new FormData();
    fdlWildcard.left = new FormAttachment( 0, 0 );
    fdlWildcard.top = new FormAttachment( wDestinationFileFolder, margin );
    fdlWildcard.right = new FormAttachment( middle, -margin );
    wlWildcard.setLayoutData( fdlWildcard );

    wWildcard = new TextVar( workflowMeta, wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wWildcard.setToolTipText( BaseMessages.getString( PKG, "JobPGPEncryptFiles.Wildcard.Tooltip" ) );
    props.setLook( wWildcard );
    wWildcard.addModifyListener( lsMod );
    fdWildcard = new FormData();
    fdWildcard.left = new FormAttachment( middle, 0 );
    fdWildcard.top = new FormAttachment( wDestinationFileFolder, margin );
    fdWildcard.right = new FormAttachment( wbSourceFileFolder, -55 );
    wWildcard.setLayoutData( fdWildcard );

    wlFields = new Label( wGeneralComp, SWT.NONE );
    wlFields.setText( BaseMessages.getString( PKG, "JobPGPEncryptFiles.Fields.Label" ) );
    props.setLook( wlFields );
    fdlFields = new FormData();
    fdlFields.left = new FormAttachment( 0, 0 );
    fdlFields.right = new FormAttachment( middle, -margin );
    fdlFields.top = new FormAttachment( wWildcard, margin );
    wlFields.setLayoutData( fdlFields );

    int rows =
      action.sourceFileFolder == null ? 1 : ( action.sourceFileFolder.length == 0
        ? 0 : action.sourceFileFolder.length );
    final int FieldsRows = rows;

    ColumnInfo[] colinf =
      new ColumnInfo[] {
        new ColumnInfo(
          BaseMessages.getString( PKG, "JobPGPEncryptFiles.Fields.Action.Label" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, ActionPGPEncryptFiles.actionTypeDesc, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "JobPGPEncryptFiles.Fields.SourceFileFolder.Label" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "JobPGPEncryptFiles.Fields.Wildcard.Label" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "JobPGPEncryptFiles.Fields.UserID.Label" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "JobPGPEncryptFiles.Fields.DestinationFileFolder.Label" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ), };

    colinf[ 0 ].setUsingVariables( true );
    colinf[ 1 ].setUsingVariables( true );
    colinf[ 1 ].setToolTip( BaseMessages.getString( PKG, "JobPGPEncryptFiles.Fields.SourceFileFolder.Tooltip" ) );
    colinf[ 2 ].setUsingVariables( true );
    colinf[ 2 ].setToolTip( BaseMessages.getString( PKG, "JobPGPEncryptFiles.Fields.Wildcard.Tooltip" ) );
    colinf[ 3 ].setUsingVariables( true );
    colinf[ 3 ].setToolTip( BaseMessages.getString( PKG, "JobPGPEncryptFiles.Fields.UserID.Tooltip" ) );
    colinf[ 4 ]
      .setToolTip( BaseMessages.getString( PKG, "JobPGPEncryptFiles.Fields.DestinationFileFolder.Tooltip" ) );

    wFields =
      new TableView(
        workflowMeta, wGeneralComp, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, FieldsRows, lsMod, props );

    fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment( wlFields, margin );
    fdFields.right = new FormAttachment( wbeSourceFileFolder, -margin );
    fdFields.bottom = new FormAttachment( 100, -margin );
    wFields.setLayoutData( fdFields );

    RefreshArgFromPrevious();

    // Add the file to the list of files...
    SelectionAdapter selA = new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent arg0 ) {
        wFields.add( new String[] {
          ActionPGPEncryptFiles.actionTypeDesc[ 0 ], wSourceFileFolder.getText(), wWildcard.getText(), null,
          wDestinationFileFolder.getText() } );
        wSourceFileFolder.setText( "" );
        wDestinationFileFolder.setText( "" );
        wWildcard.setText( "" );
        wFields.removeEmptyRows();
        wFields.setRowNums();
        wFields.optWidth( true );
      }
    };
    wbaSourceFileFolder.addSelectionListener( selA );
    wSourceFileFolder.addSelectionListener( selA );

    // Delete files from the list of files...
    wbdSourceFileFolder.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent arg0 ) {
        int[] idx = wFields.getSelectionIndices();
        wFields.remove( idx );
        wFields.removeEmptyRows();
        wFields.setRowNums();
      }
    } );

    // Edit the selected file & remove from the list...
    wbeSourceFileFolder.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent arg0 ) {
        int idx = wFields.getSelectionIndex();
        if ( idx >= 0 ) {
          String[] string = wFields.getItem( idx );
          wSourceFileFolder.setText( string[ 0 ] );
          wDestinationFileFolder.setText( string[ 1 ] );
          wWildcard.setText( string[ 2 ] );
          wFields.remove( idx );
        }
        wFields.removeEmptyRows();
        wFields.setRowNums();
      }
    } );

    fdGeneralComp = new FormData();
    fdGeneralComp.left = new FormAttachment( 0, 0 );
    fdGeneralComp.top = new FormAttachment( 0, 0 );
    fdGeneralComp.right = new FormAttachment( 100, 0 );
    fdGeneralComp.bottom = new FormAttachment( 100, 0 );
    wGeneralComp.setLayoutData( fdGeneralComp );

    wGeneralComp.layout();
    wGeneralTab.setControl( wGeneralComp );
    props.setLook( wGeneralComp );

    // ///////////////////////////////////////////////////////////
    // / END OF GENERAL TAB
    // ///////////////////////////////////////////////////////////

    // ////////////////////////////////////
    // START OF DESTINATION FILE TAB ///
    // ///////////////////////////////////

    wDestinationFileTab = new CTabItem( wTabFolder, SWT.NONE );
    wDestinationFileTab.setText( BaseMessages.getString( PKG, "JobPGPEncryptFiles.DestinationFileTab.Label" ) );

    FormLayout DestcontentLayout = new FormLayout();
    DestcontentLayout.marginWidth = 3;
    DestcontentLayout.marginHeight = 3;

    wDestinationFileComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wDestinationFileComp );
    wDestinationFileComp.setLayout( DestcontentLayout );

    // DestinationFile grouping?
    // ////////////////////////
    // START OF DestinationFile GROUP
    //

    wDestinationFile = new Group( wDestinationFileComp, SWT.SHADOW_NONE );
    props.setLook( wDestinationFile );
    wDestinationFile.setText( BaseMessages.getString( PKG, "JobPGPEncryptFiles.GroupDestinationFile.Label" ) );

    FormLayout groupLayoutFile = new FormLayout();
    groupLayoutFile.marginWidth = 10;
    groupLayoutFile.marginHeight = 10;
    wDestinationFile.setLayout( groupLayoutFile );

    // Create destination folder/parent folder
    wlCreateDestinationFolder = new Label( wDestinationFile, SWT.RIGHT );
    wlCreateDestinationFolder.setText( BaseMessages.getString(
      PKG, "JobPGPEncryptFiles.CreateDestinationFolder.Label" ) );
    props.setLook( wlCreateDestinationFolder );
    fdlCreateDestinationFolder = new FormData();
    fdlCreateDestinationFolder.left = new FormAttachment( 0, 0 );
    fdlCreateDestinationFolder.top = new FormAttachment( 0, margin );
    fdlCreateDestinationFolder.right = new FormAttachment( middle, -margin );
    wlCreateDestinationFolder.setLayoutData( fdlCreateDestinationFolder );
    wCreateDestinationFolder = new Button( wDestinationFile, SWT.CHECK );
    props.setLook( wCreateDestinationFolder );
    wCreateDestinationFolder.setToolTipText( BaseMessages.getString(
      PKG, "JobPGPEncryptFiles.CreateDestinationFolder.Tooltip" ) );
    fdCreateDestinationFolder = new FormData();
    fdCreateDestinationFolder.left = new FormAttachment( middle, 0 );
    fdCreateDestinationFolder.top = new FormAttachment( 0, margin );
    fdCreateDestinationFolder.right = new FormAttachment( 100, 0 );
    wCreateDestinationFolder.setLayoutData( fdCreateDestinationFolder );
    wCreateDestinationFolder.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
      }
    } );

    // Destination is a file?
    wlDestinationIsAFile = new Label( wDestinationFile, SWT.RIGHT );
    wlDestinationIsAFile.setText( BaseMessages.getString( PKG, "JobPGPEncryptFiles.DestinationIsAFile.Label" ) );
    props.setLook( wlDestinationIsAFile );
    fdlDestinationIsAFile = new FormData();
    fdlDestinationIsAFile.left = new FormAttachment( 0, 0 );
    fdlDestinationIsAFile.top = new FormAttachment( wCreateDestinationFolder, margin );
    fdlDestinationIsAFile.right = new FormAttachment( middle, -margin );
    wlDestinationIsAFile.setLayoutData( fdlDestinationIsAFile );
    wDestinationIsAFile = new Button( wDestinationFile, SWT.CHECK );
    props.setLook( wDestinationIsAFile );
    wDestinationIsAFile.setToolTipText( BaseMessages.getString(
      PKG, "JobPGPEncryptFiles.DestinationIsAFile.Tooltip" ) );
    fdDestinationIsAFile = new FormData();
    fdDestinationIsAFile.left = new FormAttachment( middle, 0 );
    fdDestinationIsAFile.top = new FormAttachment( wCreateDestinationFolder, margin );
    fdDestinationIsAFile.right = new FormAttachment( 100, 0 );
    wDestinationIsAFile.setLayoutData( fdDestinationIsAFile );
    wDestinationIsAFile.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {

        action.setChanged();
      }
    } );

    // Do not keep folder structure?
    wlDoNotKeepFolderStructure = new Label( wDestinationFile, SWT.RIGHT );
    wlDoNotKeepFolderStructure.setText( BaseMessages.getString(
      PKG, "JobPGPEncryptFiles.DoNotKeepFolderStructure.Label" ) );
    props.setLook( wlDoNotKeepFolderStructure );
    fdlDoNotKeepFolderStructure = new FormData();
    fdlDoNotKeepFolderStructure.left = new FormAttachment( 0, 0 );
    fdlDoNotKeepFolderStructure.top = new FormAttachment( wDestinationIsAFile, margin );
    fdlDoNotKeepFolderStructure.right = new FormAttachment( middle, -margin );
    wlDoNotKeepFolderStructure.setLayoutData( fdlDoNotKeepFolderStructure );
    wDoNotKeepFolderStructure = new Button( wDestinationFile, SWT.CHECK );
    props.setLook( wDoNotKeepFolderStructure );
    wDoNotKeepFolderStructure.setToolTipText( BaseMessages.getString(
      PKG, "JobPGPEncryptFiles.DoNotKeepFolderStructure.Tooltip" ) );
    fdDoNotKeepFolderStructure = new FormData();
    fdDoNotKeepFolderStructure.left = new FormAttachment( middle, 0 );
    fdDoNotKeepFolderStructure.top = new FormAttachment( wDestinationIsAFile, margin );
    fdDoNotKeepFolderStructure.right = new FormAttachment( 100, 0 );
    wDoNotKeepFolderStructure.setLayoutData( fdDoNotKeepFolderStructure );
    wDoNotKeepFolderStructure.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
      }
    } );

    // Create multi-part file?
    wlAddDate = new Label( wDestinationFile, SWT.RIGHT );
    wlAddDate.setText( BaseMessages.getString( PKG, "JobPGPEncryptFiles.AddDate.Label" ) );
    props.setLook( wlAddDate );
    fdlAddDate = new FormData();
    fdlAddDate.left = new FormAttachment( 0, 0 );
    fdlAddDate.top = new FormAttachment( wDoNotKeepFolderStructure, margin );
    fdlAddDate.right = new FormAttachment( middle, -margin );
    wlAddDate.setLayoutData( fdlAddDate );
    wAddDate = new Button( wDestinationFile, SWT.CHECK );
    props.setLook( wAddDate );
    wAddDate.setToolTipText( BaseMessages.getString( PKG, "JobPGPEncryptFiles.AddDate.Tooltip" ) );
    fdAddDate = new FormData();
    fdAddDate.left = new FormAttachment( middle, 0 );
    fdAddDate.top = new FormAttachment( wDoNotKeepFolderStructure, margin );
    fdAddDate.right = new FormAttachment( 100, 0 );
    wAddDate.setLayoutData( fdAddDate );
    wAddDate.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
        setAddDateBeforeExtension();
      }
    } );
    // Create multi-part file?
    wlAddTime = new Label( wDestinationFile, SWT.RIGHT );
    wlAddTime.setText( BaseMessages.getString( PKG, "JobPGPEncryptFiles.AddTime.Label" ) );
    props.setLook( wlAddTime );
    fdlAddTime = new FormData();
    fdlAddTime.left = new FormAttachment( 0, 0 );
    fdlAddTime.top = new FormAttachment( wAddDate, margin );
    fdlAddTime.right = new FormAttachment( middle, -margin );
    wlAddTime.setLayoutData( fdlAddTime );
    wAddTime = new Button( wDestinationFile, SWT.CHECK );
    props.setLook( wAddTime );
    wAddTime.setToolTipText( BaseMessages.getString( PKG, "JobPGPEncryptFiles.AddTime.Tooltip" ) );
    fdAddTime = new FormData();
    fdAddTime.left = new FormAttachment( middle, 0 );
    fdAddTime.top = new FormAttachment( wAddDate, margin );
    fdAddTime.right = new FormAttachment( 100, 0 );
    wAddTime.setLayoutData( fdAddTime );
    wAddTime.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
        setAddDateBeforeExtension();
      }
    } );

    // Specify date time format?
    wlSpecifyFormat = new Label( wDestinationFile, SWT.RIGHT );
    wlSpecifyFormat.setText( BaseMessages.getString( PKG, "JobPGPEncryptFiles.SpecifyFormat.Label" ) );
    props.setLook( wlSpecifyFormat );
    fdlSpecifyFormat = new FormData();
    fdlSpecifyFormat.left = new FormAttachment( 0, 0 );
    fdlSpecifyFormat.top = new FormAttachment( wAddTime, margin );
    fdlSpecifyFormat.right = new FormAttachment( middle, -margin );
    wlSpecifyFormat.setLayoutData( fdlSpecifyFormat );
    wSpecifyFormat = new Button( wDestinationFile, SWT.CHECK );
    props.setLook( wSpecifyFormat );
    wSpecifyFormat.setToolTipText( BaseMessages.getString( PKG, "JobPGPEncryptFiles.SpecifyFormat.Tooltip" ) );
    fdSpecifyFormat = new FormData();
    fdSpecifyFormat.left = new FormAttachment( middle, 0 );
    fdSpecifyFormat.top = new FormAttachment( wAddTime, margin );
    fdSpecifyFormat.right = new FormAttachment( 100, 0 );
    wSpecifyFormat.setLayoutData( fdSpecifyFormat );
    wSpecifyFormat.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
        setDateTimeFormat();
        setAddDateBeforeExtension();
      }
    } );

    // DateTimeFormat
    wlDateTimeFormat = new Label( wDestinationFile, SWT.RIGHT );
    wlDateTimeFormat.setText( BaseMessages.getString( PKG, "JobPGPEncryptFiles.DateTimeFormat.Label" ) );
    props.setLook( wlDateTimeFormat );
    fdlDateTimeFormat = new FormData();
    fdlDateTimeFormat.left = new FormAttachment( 0, 0 );
    fdlDateTimeFormat.top = new FormAttachment( wSpecifyFormat, margin );
    fdlDateTimeFormat.right = new FormAttachment( middle, -margin );
    wlDateTimeFormat.setLayoutData( fdlDateTimeFormat );
    wDateTimeFormat = new CCombo( wDestinationFile, SWT.BORDER | SWT.READ_ONLY );
    wDateTimeFormat.setEditable( true );
    props.setLook( wDateTimeFormat );
    wDateTimeFormat.addModifyListener( lsMod );
    fdDateTimeFormat = new FormData();
    fdDateTimeFormat.left = new FormAttachment( middle, 0 );
    fdDateTimeFormat.top = new FormAttachment( wSpecifyFormat, margin );
    fdDateTimeFormat.right = new FormAttachment( 100, 0 );
    wDateTimeFormat.setLayoutData( fdDateTimeFormat );
    // Prepare a list of possible DateTimeFormats...
    String[] dats = Const.getDateFormats();
    for ( int x = 0; x < dats.length; x++ ) {
      wDateTimeFormat.add( dats[ x ] );
    }

    // Add Date before extension?
    wlAddDateBeforeExtension = new Label( wDestinationFile, SWT.RIGHT );
    wlAddDateBeforeExtension.setText( BaseMessages.getString(
      PKG, "JobPGPEncryptFiles.AddDateBeforeExtension.Label" ) );
    props.setLook( wlAddDateBeforeExtension );
    fdlAddDateBeforeExtension = new FormData();
    fdlAddDateBeforeExtension.left = new FormAttachment( 0, 0 );
    fdlAddDateBeforeExtension.top = new FormAttachment( wDateTimeFormat, margin );
    fdlAddDateBeforeExtension.right = new FormAttachment( middle, -margin );
    wlAddDateBeforeExtension.setLayoutData( fdlAddDateBeforeExtension );
    wAddDateBeforeExtension = new Button( wDestinationFile, SWT.CHECK );
    props.setLook( wAddDateBeforeExtension );
    wAddDateBeforeExtension.setToolTipText( BaseMessages.getString(
      PKG, "JobPGPEncryptFiles.AddDateBeforeExtension.Tooltip" ) );
    fdAddDateBeforeExtension = new FormData();
    fdAddDateBeforeExtension.left = new FormAttachment( middle, 0 );
    fdAddDateBeforeExtension.top = new FormAttachment( wDateTimeFormat, margin );
    fdAddDateBeforeExtension.right = new FormAttachment( 100, 0 );
    wAddDateBeforeExtension.setLayoutData( fdAddDateBeforeExtension );
    wAddDateBeforeExtension.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
      }
    } );

    // If File Exists
    wlIfFileExists = new Label( wDestinationFile, SWT.RIGHT );
    wlIfFileExists.setText( BaseMessages.getString( PKG, "JobPGPEncryptFiles.IfFileExists.Label" ) );
    props.setLook( wlIfFileExists );
    fdlIfFileExists = new FormData();
    fdlIfFileExists.left = new FormAttachment( 0, 0 );
    fdlIfFileExists.right = new FormAttachment( middle, 0 );
    fdlIfFileExists.top = new FormAttachment( wAddDateBeforeExtension, margin );
    wlIfFileExists.setLayoutData( fdlIfFileExists );
    wIfFileExists = new CCombo( wDestinationFile, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wIfFileExists.add( BaseMessages.getString( PKG, "JobPGPEncryptFiles.Do_Nothing_IfFileExists.Label" ) );
    wIfFileExists.add( BaseMessages.getString( PKG, "JobPGPEncryptFiles.Overwrite_File_IfFileExists.Label" ) );
    wIfFileExists.add( BaseMessages.getString( PKG, "JobPGPEncryptFiles.Unique_Name_IfFileExists.Label" ) );
    wIfFileExists.add( BaseMessages.getString( PKG, "JobPGPEncryptFiles.Delete_Source_File_IfFileExists.Label" ) );
    wIfFileExists.add( BaseMessages.getString( PKG, "JobPGPEncryptFiles.Move_To_Folder_IfFileExists.Label" ) );
    wIfFileExists.add( BaseMessages.getString( PKG, "JobPGPEncryptFiles.Fail_IfFileExists.Label" ) );
    wIfFileExists.select( 0 ); // +1: starts at -1

    props.setLook( wIfFileExists );
    fdIfFileExists = new FormData();
    fdIfFileExists.left = new FormAttachment( middle, 0 );
    fdIfFileExists.top = new FormAttachment( wAddDateBeforeExtension, margin );
    fdIfFileExists.right = new FormAttachment( 100, 0 );
    wIfFileExists.setLayoutData( fdIfFileExists );

    wIfFileExists.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {

        activeDestinationFolder();
        setMovedDateTimeFormat();
        // setAddDateBeforeExtension();
        setAddMovedDateBeforeExtension();

      }
    } );

    fdDestinationFile = new FormData();
    fdDestinationFile.left = new FormAttachment( 0, margin );
    fdDestinationFile.top = new FormAttachment( wName, margin );
    fdDestinationFile.right = new FormAttachment( 100, -margin );
    wDestinationFile.setLayoutData( fdDestinationFile );

    // ///////////////////////////////////////////////////////////
    // / END OF DestinationFile GROUP
    // ///////////////////////////////////////////////////////////

    // MoveTo grouping?
    // ////////////////////////
    // START OF MoveTo GROUP
    //

    wMoveToGroup = new Group( wDestinationFileComp, SWT.SHADOW_NONE );
    props.setLook( wMoveToGroup );
    wMoveToGroup.setText( BaseMessages.getString( PKG, "JobPGPEncryptFiles.GroupMoveToGroup.Label" ) );

    FormLayout MovetoLayoutFile = new FormLayout();
    MovetoLayoutFile.marginWidth = 10;
    MovetoLayoutFile.marginHeight = 10;
    wMoveToGroup.setLayout( MovetoLayoutFile );

    // DestinationFolder line
    wlDestinationFolder = new Label( wMoveToGroup, SWT.RIGHT );
    wlDestinationFolder.setText( BaseMessages.getString( PKG, "JobPGPEncryptFiles.DestinationFolder.Label" ) );
    props.setLook( wlDestinationFolder );
    fdlDestinationFolder = new FormData();
    fdlDestinationFolder.left = new FormAttachment( 0, 0 );
    fdlDestinationFolder.top = new FormAttachment( wDestinationFile, margin );
    fdlDestinationFolder.right = new FormAttachment( middle, -margin );
    wlDestinationFolder.setLayoutData( fdlDestinationFolder );

    wbDestinationFolder = new Button( wMoveToGroup, SWT.PUSH | SWT.CENTER );
    props.setLook( wbDestinationFolder );
    wbDestinationFolder.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    fdbDestinationFolder = new FormData();
    fdbDestinationFolder.right = new FormAttachment( 100, 0 );
    fdbDestinationFolder.top = new FormAttachment( wDestinationFile, 0 );
    wbDestinationFolder.setLayoutData( fdbDestinationFolder );

    wDestinationFolder = new TextVar( workflowMeta, wMoveToGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wDestinationFolder );
    wDestinationFolder.addModifyListener( lsMod );
    fdDestinationFolder = new FormData();
    fdDestinationFolder.left = new FormAttachment( middle, 0 );
    fdDestinationFolder.top = new FormAttachment( wDestinationFile, margin );
    fdDestinationFolder.right = new FormAttachment( wbDestinationFolder, -margin );
    wDestinationFolder.setLayoutData( fdDestinationFolder );

    // Whenever something changes, set the tooltip to the expanded version:
    wDestinationFolder.addModifyListener( e -> wDestinationFolder.setToolTipText( workflowMeta.environmentSubstitute( wDestinationFolder.getText() ) ) );
    wbDestinationFolder.addListener( SWT.Selection, e-> BaseDialog.presentDirectoryDialog( shell, wDestinationFolder, workflowMeta ) );

    // Create destination folder/parent folder
    wlCreateMoveToFolder = new Label( wMoveToGroup, SWT.RIGHT );
    wlCreateMoveToFolder.setText( BaseMessages.getString( PKG, "JobPGPEncryptFiles.CreateMoveToFolder.Label" ) );
    props.setLook( wlCreateMoveToFolder );
    fdlCreateMoveToFolder = new FormData();
    fdlCreateMoveToFolder.left = new FormAttachment( 0, 0 );
    fdlCreateMoveToFolder.top = new FormAttachment( wDestinationFolder, margin );
    fdlCreateMoveToFolder.right = new FormAttachment( middle, -margin );
    wlCreateMoveToFolder.setLayoutData( fdlCreateMoveToFolder );
    wCreateMoveToFolder = new Button( wMoveToGroup, SWT.CHECK );
    props.setLook( wCreateMoveToFolder );
    wCreateMoveToFolder.setToolTipText( BaseMessages.getString(
      PKG, "JobPGPEncryptFiles.CreateMoveToFolder.Tooltip" ) );
    fdCreateMoveToFolder = new FormData();
    fdCreateMoveToFolder.left = new FormAttachment( middle, 0 );
    fdCreateMoveToFolder.top = new FormAttachment( wDestinationFolder, margin );
    fdCreateMoveToFolder.right = new FormAttachment( 100, 0 );
    wCreateMoveToFolder.setLayoutData( fdCreateMoveToFolder );
    wCreateMoveToFolder.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
      }
    } );

    // Create multi-part file?
    wlAddMovedDate = new Label( wMoveToGroup, SWT.RIGHT );
    wlAddMovedDate.setText( BaseMessages.getString( PKG, "JobPGPEncryptFiles.AddMovedDate.Label" ) );
    props.setLook( wlAddMovedDate );
    fdlAddMovedDate = new FormData();
    fdlAddMovedDate.left = new FormAttachment( 0, 0 );
    fdlAddMovedDate.top = new FormAttachment( wCreateMoveToFolder, margin );
    fdlAddMovedDate.right = new FormAttachment( middle, -margin );
    wlAddMovedDate.setLayoutData( fdlAddMovedDate );
    wAddMovedDate = new Button( wMoveToGroup, SWT.CHECK );
    props.setLook( wAddMovedDate );
    wAddMovedDate.setToolTipText( BaseMessages.getString( PKG, "JobPGPEncryptFiles.AddMovedDate.Tooltip" ) );
    fdAddMovedDate = new FormData();
    fdAddMovedDate.left = new FormAttachment( middle, 0 );
    fdAddMovedDate.top = new FormAttachment( wCreateMoveToFolder, margin );
    fdAddMovedDate.right = new FormAttachment( 100, 0 );
    wAddMovedDate.setLayoutData( fdAddMovedDate );
    wAddMovedDate.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
        setAddMovedDateBeforeExtension();
      }
    } );
    // Create multi-part file?
    wlAddMovedTime = new Label( wMoveToGroup, SWT.RIGHT );
    wlAddMovedTime.setText( BaseMessages.getString( PKG, "JobPGPEncryptFiles.AddMovedTime.Label" ) );
    props.setLook( wlAddMovedTime );
    fdlAddMovedTime = new FormData();
    fdlAddMovedTime.left = new FormAttachment( 0, 0 );
    fdlAddMovedTime.top = new FormAttachment( wAddMovedDate, margin );
    fdlAddMovedTime.right = new FormAttachment( middle, -margin );
    wlAddMovedTime.setLayoutData( fdlAddMovedTime );
    wAddMovedTime = new Button( wMoveToGroup, SWT.CHECK );
    props.setLook( wAddMovedTime );
    wAddMovedTime.setToolTipText( BaseMessages.getString( PKG, "JobPGPEncryptFiles.AddMovedTime.Tooltip" ) );
    fdAddMovedTime = new FormData();
    fdAddMovedTime.left = new FormAttachment( middle, 0 );
    fdAddMovedTime.top = new FormAttachment( wAddMovedDate, margin );
    fdAddMovedTime.right = new FormAttachment( 100, 0 );
    wAddMovedTime.setLayoutData( fdAddMovedTime );
    wAddMovedTime.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
        setAddMovedDateBeforeExtension();
      }
    } );

    // Specify date time format?
    wlSpecifyMoveFormat = new Label( wMoveToGroup, SWT.RIGHT );
    wlSpecifyMoveFormat.setText( BaseMessages.getString( PKG, "JobPGPEncryptFiles.SpecifyMoveFormat.Label" ) );
    props.setLook( wlSpecifyMoveFormat );
    fdlSpecifyMoveFormat = new FormData();
    fdlSpecifyMoveFormat.left = new FormAttachment( 0, 0 );
    fdlSpecifyMoveFormat.top = new FormAttachment( wAddMovedTime, margin );
    fdlSpecifyMoveFormat.right = new FormAttachment( middle, -margin );
    wlSpecifyMoveFormat.setLayoutData( fdlSpecifyMoveFormat );
    wSpecifyMoveFormat = new Button( wMoveToGroup, SWT.CHECK );
    props.setLook( wSpecifyMoveFormat );
    wSpecifyMoveFormat.setToolTipText( BaseMessages
      .getString( PKG, "JobPGPEncryptFiles.SpecifyMoveFormat.Tooltip" ) );
    fdSpecifyMoveFormat = new FormData();
    fdSpecifyMoveFormat.left = new FormAttachment( middle, 0 );
    fdSpecifyMoveFormat.top = new FormAttachment( wAddMovedTime, margin );
    fdSpecifyMoveFormat.right = new FormAttachment( 100, 0 );
    wSpecifyMoveFormat.setLayoutData( fdSpecifyMoveFormat );
    wSpecifyMoveFormat.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
        setMovedDateTimeFormat();
        setAddMovedDateBeforeExtension();
      }
    } );

    // Moved DateTimeFormat
    wlMovedDateTimeFormat = new Label( wMoveToGroup, SWT.RIGHT );
    wlMovedDateTimeFormat.setText( BaseMessages.getString( PKG, "JobPGPEncryptFiles.MovedDateTimeFormat.Label" ) );
    props.setLook( wlMovedDateTimeFormat );
    fdlMovedDateTimeFormat = new FormData();
    fdlMovedDateTimeFormat.left = new FormAttachment( 0, 0 );
    fdlMovedDateTimeFormat.top = new FormAttachment( wSpecifyMoveFormat, margin );
    fdlMovedDateTimeFormat.right = new FormAttachment( middle, -margin );
    wlMovedDateTimeFormat.setLayoutData( fdlMovedDateTimeFormat );
    wMovedDateTimeFormat = new CCombo( wMoveToGroup, SWT.BORDER | SWT.READ_ONLY );
    wMovedDateTimeFormat.setEditable( true );
    props.setLook( wMovedDateTimeFormat );
    wMovedDateTimeFormat.addModifyListener( lsMod );
    fdMovedDateTimeFormat = new FormData();
    fdMovedDateTimeFormat.left = new FormAttachment( middle, 0 );
    fdMovedDateTimeFormat.top = new FormAttachment( wSpecifyMoveFormat, margin );
    fdMovedDateTimeFormat.right = new FormAttachment( 100, 0 );
    wMovedDateTimeFormat.setLayoutData( fdMovedDateTimeFormat );

    for ( int x = 0; x < dats.length; x++ ) {
      wMovedDateTimeFormat.add( dats[ x ] );
    }

    // Add Date before extension?
    wlAddMovedDateBeforeExtension = new Label( wMoveToGroup, SWT.RIGHT );
    wlAddMovedDateBeforeExtension.setText( BaseMessages.getString(
      PKG, "JobPGPEncryptFiles.AddMovedDateBeforeExtension.Label" ) );
    props.setLook( wlAddMovedDateBeforeExtension );
    fdlAddMovedDateBeforeExtension = new FormData();
    fdlAddMovedDateBeforeExtension.left = new FormAttachment( 0, 0 );
    fdlAddMovedDateBeforeExtension.top = new FormAttachment( wMovedDateTimeFormat, margin );
    fdlAddMovedDateBeforeExtension.right = new FormAttachment( middle, -margin );
    wlAddMovedDateBeforeExtension.setLayoutData( fdlAddMovedDateBeforeExtension );
    wAddMovedDateBeforeExtension = new Button( wMoveToGroup, SWT.CHECK );
    props.setLook( wAddMovedDateBeforeExtension );
    wAddMovedDateBeforeExtension.setToolTipText( BaseMessages.getString(
      PKG, "JobPGPEncryptFiles.AddMovedDateBeforeExtension.Tooltip" ) );
    fdAddMovedDateBeforeExtension = new FormData();
    fdAddMovedDateBeforeExtension.left = new FormAttachment( middle, 0 );
    fdAddMovedDateBeforeExtension.top = new FormAttachment( wMovedDateTimeFormat, margin );
    fdAddMovedDateBeforeExtension.right = new FormAttachment( 100, 0 );
    wAddMovedDateBeforeExtension.setLayoutData( fdAddMovedDateBeforeExtension );
    wAddMovedDateBeforeExtension.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
      }
    } );

    // If moved File Exists
    wlIfMovedFileExists = new Label( wMoveToGroup, SWT.RIGHT );
    wlIfMovedFileExists.setText( BaseMessages.getString( PKG, "JobPGPEncryptFiles.IfMovedFileExists.Label" ) );
    props.setLook( wlIfMovedFileExists );
    fdlIfMovedFileExists = new FormData();
    fdlIfMovedFileExists.left = new FormAttachment( 0, 0 );
    fdlIfMovedFileExists.right = new FormAttachment( middle, 0 );
    fdlIfMovedFileExists.top = new FormAttachment( wAddMovedDateBeforeExtension, margin );
    wlIfMovedFileExists.setLayoutData( fdlIfMovedFileExists );
    wIfMovedFileExists = new CCombo( wMoveToGroup, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wIfMovedFileExists
      .add( BaseMessages.getString( PKG, "JobPGPEncryptFiles.Do_Nothing_IfMovedFileExists.Label" ) );
    wIfMovedFileExists.add( BaseMessages.getString(
      PKG, "JobPGPEncryptFiles.Overwrite_Filename_IffMovedFileExists.Label" ) );
    wIfMovedFileExists
      .add( BaseMessages.getString( PKG, "JobPGPEncryptFiles.UniqueName_IfMovedFileExists.Label" ) );
    wIfMovedFileExists.add( BaseMessages.getString( PKG, "JobPGPEncryptFiles.Fail_IfMovedFileExists.Label" ) );
    wIfMovedFileExists.select( 0 ); // +1: starts at -1

    props.setLook( wIfMovedFileExists );
    fdIfMovedFileExists = new FormData();
    fdIfMovedFileExists.left = new FormAttachment( middle, 0 );
    fdIfMovedFileExists.top = new FormAttachment( wAddMovedDateBeforeExtension, margin );
    fdIfMovedFileExists.right = new FormAttachment( 100, 0 );
    wIfMovedFileExists.setLayoutData( fdIfMovedFileExists );

    fdIfMovedFileExists = new FormData();
    fdIfMovedFileExists.left = new FormAttachment( middle, 0 );
    fdIfMovedFileExists.top = new FormAttachment( wAddMovedDateBeforeExtension, margin );
    fdIfMovedFileExists.right = new FormAttachment( 100, 0 );
    wIfMovedFileExists.setLayoutData( fdIfMovedFileExists );

    fdMoveToGroup = new FormData();
    fdMoveToGroup.left = new FormAttachment( 0, margin );
    fdMoveToGroup.top = new FormAttachment( wDestinationFile, margin );
    fdMoveToGroup.right = new FormAttachment( 100, -margin );
    wMoveToGroup.setLayoutData( fdMoveToGroup );

    // ///////////////////////////////////////////////////////////
    // / END OF MoveToGroup GROUP
    // ///////////////////////////////////////////////////////////

    fdDestinationFileComp = new FormData();
    fdDestinationFileComp.left = new FormAttachment( 0, 0 );
    fdDestinationFileComp.top = new FormAttachment( 0, 0 );
    fdDestinationFileComp.right = new FormAttachment( 100, 0 );
    fdDestinationFileComp.bottom = new FormAttachment( 100, 0 );
    wDestinationFileComp.setLayoutData( wDestinationFileComp );

    wDestinationFileComp.layout();
    wDestinationFileTab.setControl( wDestinationFileComp );

    // ///////////////////////////////////////////////////////////
    // / END OF DESTINATION FILETAB
    // ///////////////////////////////////////////////////////////

    // ////////////////////////////////////
    // START OF ADVANCED TAB ///
    // ///////////////////////////////////

    wAdvancedTab = new CTabItem( wTabFolder, SWT.NONE );
    wAdvancedTab.setText( BaseMessages.getString( PKG, "JobPGPEncryptFiles.Tab.Advanced.Label" ) );

    FormLayout contentLayout = new FormLayout();
    contentLayout.marginWidth = 3;
    contentLayout.marginHeight = 3;

    wAdvancedComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wAdvancedComp );
    wAdvancedComp.setLayout( contentLayout );

    // SuccessOngrouping?
    // ////////////////////////
    // START OF SUCCESS ON GROUP///
    // /
    wSuccessOn = new Group( wAdvancedComp, SWT.SHADOW_NONE );
    props.setLook( wSuccessOn );
    wSuccessOn.setText( BaseMessages.getString( PKG, "JobPGPEncryptFiles.SuccessOn.Group.Label" ) );

    FormLayout successongroupLayout = new FormLayout();
    successongroupLayout.marginWidth = 10;
    successongroupLayout.marginHeight = 10;

    wSuccessOn.setLayout( successongroupLayout );

    // Success Condition
    wlSuccessCondition = new Label( wSuccessOn, SWT.RIGHT );
    wlSuccessCondition.setText( BaseMessages.getString( PKG, "JobPGPEncryptFiles.SuccessCondition.Label" ) );
    props.setLook( wlSuccessCondition );
    fdlSuccessCondition = new FormData();
    fdlSuccessCondition.left = new FormAttachment( 0, 0 );
    fdlSuccessCondition.right = new FormAttachment( middle, 0 );
    fdlSuccessCondition.top = new FormAttachment( 0, margin );
    wlSuccessCondition.setLayoutData( fdlSuccessCondition );
    wSuccessCondition = new CCombo( wSuccessOn, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wSuccessCondition.add( BaseMessages.getString( PKG, "JobPGPEncryptFiles.SuccessWhenAllWorksFine.Label" ) );
    wSuccessCondition.add( BaseMessages.getString( PKG, "JobPGPEncryptFiles.SuccessWhenAtLeat.Label" ) );
    wSuccessCondition.add( BaseMessages.getString( PKG, "JobPGPEncryptFiles.SuccessWhenErrorsLessThan.Label" ) );

    wSuccessCondition.select( 0 ); // +1: starts at -1

    props.setLook( wSuccessCondition );
    fdSuccessCondition = new FormData();
    fdSuccessCondition.left = new FormAttachment( middle, 0 );
    fdSuccessCondition.top = new FormAttachment( 0, margin );
    fdSuccessCondition.right = new FormAttachment( 100, 0 );
    wSuccessCondition.setLayoutData( fdSuccessCondition );
    wSuccessCondition.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        activeSuccessCondition();

      }
    } );

    // Success when number of errors less than
    wlNrErrorsLessThan = new Label( wSuccessOn, SWT.RIGHT );
    wlNrErrorsLessThan.setText( BaseMessages.getString( PKG, "JobPGPEncryptFiles.NrErrorsLessThan.Label" ) );
    props.setLook( wlNrErrorsLessThan );
    fdlNrErrorsLessThan = new FormData();
    fdlNrErrorsLessThan.left = new FormAttachment( 0, 0 );
    fdlNrErrorsLessThan.top = new FormAttachment( wSuccessCondition, margin );
    fdlNrErrorsLessThan.right = new FormAttachment( middle, -margin );
    wlNrErrorsLessThan.setLayoutData( fdlNrErrorsLessThan );

    wNrErrorsLessThan =
      new TextVar( workflowMeta, wSuccessOn, SWT.SINGLE | SWT.LEFT | SWT.BORDER, BaseMessages.getString(
        PKG, "JobPGPEncryptFiles.NrErrorsLessThan.Tooltip" ) );
    props.setLook( wNrErrorsLessThan );
    wNrErrorsLessThan.addModifyListener( lsMod );
    fdNrErrorsLessThan = new FormData();
    fdNrErrorsLessThan.left = new FormAttachment( middle, 0 );
    fdNrErrorsLessThan.top = new FormAttachment( wSuccessCondition, margin );
    fdNrErrorsLessThan.right = new FormAttachment( 100, -margin );
    wNrErrorsLessThan.setLayoutData( fdNrErrorsLessThan );

    fdSuccessOn = new FormData();
    fdSuccessOn.left = new FormAttachment( 0, margin );
    fdSuccessOn.top = new FormAttachment( wDestinationFile, margin );
    fdSuccessOn.right = new FormAttachment( 100, -margin );
    wSuccessOn.setLayoutData( fdSuccessOn );
    // ///////////////////////////////////////////////////////////
    // / END OF Success ON GROUP
    // ///////////////////////////////////////////////////////////

    // fileresult grouping?
    // ////////////////////////
    // START OF LOGGING GROUP///
    // /
    wFileResult = new Group( wAdvancedComp, SWT.SHADOW_NONE );
    props.setLook( wFileResult );
    wFileResult.setText( BaseMessages.getString( PKG, "JobPGPEncryptFiles.FileResult.Group.Label" ) );

    FormLayout fileresultgroupLayout = new FormLayout();
    fileresultgroupLayout.marginWidth = 10;
    fileresultgroupLayout.marginHeight = 10;

    wFileResult.setLayout( fileresultgroupLayout );

    // Add file to result
    wlAddFileToResult = new Label( wFileResult, SWT.RIGHT );
    wlAddFileToResult.setText( BaseMessages.getString( PKG, "JobPGPEncryptFiles.AddFileToResult.Label" ) );
    props.setLook( wlAddFileToResult );
    fdlAddFileToResult = new FormData();
    fdlAddFileToResult.left = new FormAttachment( 0, 0 );
    fdlAddFileToResult.top = new FormAttachment( wSuccessOn, margin );
    fdlAddFileToResult.right = new FormAttachment( middle, -margin );
    wlAddFileToResult.setLayoutData( fdlAddFileToResult );
    wAddFileToResult = new Button( wFileResult, SWT.CHECK );
    props.setLook( wAddFileToResult );
    wAddFileToResult.setToolTipText( BaseMessages.getString( PKG, "JobPGPEncryptFiles.AddFileToResult.Tooltip" ) );
    fdAddFileToResult = new FormData();
    fdAddFileToResult.left = new FormAttachment( middle, 0 );
    fdAddFileToResult.top = new FormAttachment( wSuccessOn, margin );
    fdAddFileToResult.right = new FormAttachment( 100, 0 );
    wAddFileToResult.setLayoutData( fdAddFileToResult );
    wAddFileToResult.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
      }
    } );

    fdFileResult = new FormData();
    fdFileResult.left = new FormAttachment( 0, margin );
    fdFileResult.top = new FormAttachment( wSuccessOn, margin );
    fdFileResult.right = new FormAttachment( 100, -margin );
    wFileResult.setLayoutData( fdFileResult );
    // ///////////////////////////////////////////////////////////
    // / END OF FilesResult GROUP
    // ///////////////////////////////////////////////////////////

    fdAdvancedComp = new FormData();
    fdAdvancedComp.left = new FormAttachment( 0, 0 );
    fdAdvancedComp.top = new FormAttachment( 0, 0 );
    fdAdvancedComp.right = new FormAttachment( 100, 0 );
    fdAdvancedComp.bottom = new FormAttachment( 100, 0 );
    wAdvancedComp.setLayoutData( wAdvancedComp );

    wAdvancedComp.layout();
    wAdvancedTab.setControl( wAdvancedComp );

    // ///////////////////////////////////////////////////////////
    // / END OF ADVANCED TAB
    // ///////////////////////////////////////////////////////////

    fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( wName, margin );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.bottom = new FormAttachment( 100, -50 );
    wTabFolder.setLayoutData( fdTabFolder );

    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    BaseTransformDialog.positionBottomButtons( shell, new Button[] { wOk, wCancel }, margin, wTabFolder );

    // Add listeners
    lsCancel = new Listener() {
      @Override
      public void handleEvent( Event e ) {
        cancel();
      }
    };
    lsOk = new Listener() {
      @Override
      public void handleEvent( Event e ) {
        ok();
      }
    };

    wCancel.addListener( SWT.Selection, lsCancel );
    wOk.addListener( SWT.Selection, lsOk );

    lsDef = new SelectionAdapter() {
      @Override
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wName.addSelectionListener( lsDef );
    wSourceFileFolder.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      @Override
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();
    CheckIncludeSubFolders();
    activeSuccessCondition();
    setDateTimeFormat();
    activeSuccessCondition();

    activeDestinationFolder();
    setMovedDateTimeFormat();
    setAddDateBeforeExtension();
    setAddMovedDateBeforeExtension();
    wTabFolder.setSelection( 0 );
    BaseTransformDialog.setSize( shell );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return action;
  }

  private void activeDestinationFolder() {

    wbDestinationFolder.setEnabled( wIfFileExists.getSelectionIndex() == 4 );
    wlDestinationFolder.setEnabled( wIfFileExists.getSelectionIndex() == 4 );
    wDestinationFolder.setEnabled( wIfFileExists.getSelectionIndex() == 4 );
    wlMovedDateTimeFormat.setEnabled( wIfFileExists.getSelectionIndex() == 4 );
    wMovedDateTimeFormat.setEnabled( wIfFileExists.getSelectionIndex() == 4 );
    wIfMovedFileExists.setEnabled( wIfFileExists.getSelectionIndex() == 4 );
    wlIfMovedFileExists.setEnabled( wIfFileExists.getSelectionIndex() == 4 );
    wlAddMovedDateBeforeExtension.setEnabled( wIfFileExists.getSelectionIndex() == 4 );
    wAddMovedDateBeforeExtension.setEnabled( wIfFileExists.getSelectionIndex() == 4 );
    wlAddMovedDate.setEnabled( wIfFileExists.getSelectionIndex() == 4 );
    wAddMovedDate.setEnabled( wIfFileExists.getSelectionIndex() == 4 );
    wlAddMovedTime.setEnabled( wIfFileExists.getSelectionIndex() == 4 );
    wAddMovedTime.setEnabled( wIfFileExists.getSelectionIndex() == 4 );
    wlSpecifyMoveFormat.setEnabled( wIfFileExists.getSelectionIndex() == 4 );
    wSpecifyMoveFormat.setEnabled( wIfFileExists.getSelectionIndex() == 4 );
    wlCreateMoveToFolder.setEnabled( wIfFileExists.getSelectionIndex() == 4 );
    wCreateMoveToFolder.setEnabled( wIfFileExists.getSelectionIndex() == 4 );
  }

  private void activeSuccessCondition() {
    wlNrErrorsLessThan.setEnabled( wSuccessCondition.getSelectionIndex() != 0 );
    wNrErrorsLessThan.setEnabled( wSuccessCondition.getSelectionIndex() != 0 );
  }

  private void setAddDateBeforeExtension() {
    wlAddDateBeforeExtension.setEnabled( wAddDate.getSelection()
      || wAddTime.getSelection() || wSpecifyFormat.getSelection() );
    wAddDateBeforeExtension.setEnabled( wAddDate.getSelection()
      || wAddTime.getSelection() || wSpecifyFormat.getSelection() );
    if ( !wAddDate.getSelection() && !wAddTime.getSelection() && !wSpecifyFormat.getSelection() ) {
      wAddDateBeforeExtension.setSelection( false );
    }
  }

  private void setAddMovedDateBeforeExtension() {
    wlAddMovedDateBeforeExtension.setEnabled( wAddMovedDate.getSelection()
      || wAddMovedTime.getSelection() || wSpecifyMoveFormat.getSelection() );
    wAddMovedDateBeforeExtension.setEnabled( wAddMovedDate.getSelection()
      || wAddMovedTime.getSelection() || wSpecifyMoveFormat.getSelection() );
    if ( !wAddMovedDate.getSelection() && !wAddMovedTime.getSelection() && !wSpecifyMoveFormat.getSelection() ) {
      wAddMovedDateBeforeExtension.setSelection( false );
    }
  }

  private void setDateTimeFormat() {
    if ( wSpecifyFormat.getSelection() ) {
      wAddDate.setSelection( false );
      wAddTime.setSelection( false );
    }

    wDateTimeFormat.setEnabled( wSpecifyFormat.getSelection() );
    wlDateTimeFormat.setEnabled( wSpecifyFormat.getSelection() );
    wAddDate.setEnabled( !wSpecifyFormat.getSelection() );
    wlAddDate.setEnabled( !wSpecifyFormat.getSelection() );
    wAddTime.setEnabled( !wSpecifyFormat.getSelection() );
    wlAddTime.setEnabled( !wSpecifyFormat.getSelection() );

  }

  private void setMovedDateTimeFormat() {
    if ( wSpecifyMoveFormat.getSelection() ) {
      wAddMovedDate.setSelection( false );
      wAddMovedTime.setSelection( false );
    }

    wlMovedDateTimeFormat.setEnabled( wSpecifyMoveFormat.getSelection() );
    wMovedDateTimeFormat.setEnabled( wSpecifyMoveFormat.getSelection() );
  }

  private void RefreshArgFromPrevious() {

    wlFields.setEnabled( !wPrevious.getSelection() );
    wFields.setEnabled( !wPrevious.getSelection() );
    wbdSourceFileFolder.setEnabled( !wPrevious.getSelection() );
    wbeSourceFileFolder.setEnabled( !wPrevious.getSelection() );
    wbSourceFileFolder.setEnabled( !wPrevious.getSelection() );
    wbaSourceFileFolder.setEnabled( !wPrevious.getSelection() );
    wbDestinationFileFolder.setEnabled( !wPrevious.getSelection() );
    wlDestinationFileFolder.setEnabled( !wPrevious.getSelection() );
    wDestinationFileFolder.setEnabled( !wPrevious.getSelection() );
    wlSourceFileFolder.setEnabled( !wPrevious.getSelection() );
    wSourceFileFolder.setEnabled( !wPrevious.getSelection() );

    wlWildcard.setEnabled( !wPrevious.getSelection() );
    wWildcard.setEnabled( !wPrevious.getSelection() );
    wbSourceDirectory.setEnabled( !wPrevious.getSelection() );
    wbDestinationDirectory.setEnabled( !wPrevious.getSelection() );
  }

  public void dispose() {
    WindowProperty winprop = new WindowProperty( shell );
    props.setScreen( winprop );
    shell.dispose();
  }

  private void CheckIncludeSubFolders() {
    wlDoNotKeepFolderStructure.setEnabled( wIncludeSubfolders.getSelection() );
    wDoNotKeepFolderStructure.setEnabled( wIncludeSubfolders.getSelection() );
    if ( !wIncludeSubfolders.getSelection() ) {
      wDoNotKeepFolderStructure.setSelection( false );
    }
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    wName.setText( Const.nullToEmpty( action.getName() ) );
    if ( action.sourceFileFolder != null ) {
      for ( int i = 0; i < action.sourceFileFolder.length; i++ ) {
        TableItem ti = wFields.table.getItem( i );
        ti.setText( 1, ActionPGPEncryptFiles.getActionTypeDesc( action.actionType[ i ] ) );
        if ( action.sourceFileFolder[ i ] != null ) {
          ti.setText( 2, action.sourceFileFolder[ i ] );
        }
        if ( action.wildcard[ i ] != null ) {
          ti.setText( 3, action.wildcard[ i ] );
        }
        if ( action.userId[ i ] != null ) {
          ti.setText( 4, action.userId[ i ] );
        }

        if ( action.destinationFileFolder[ i ] != null ) {
          ti.setText( 5, action.destinationFileFolder[ i ] );
        }
      }
      wFields.setRowNums();
      wFields.optWidth( true );

    }
    wasciiMode.setSelection( action.isAsciiMode() );
    wPrevious.setSelection( action.argFromPrevious );
    wIncludeSubfolders.setSelection( action.includeSubfolders );
    wDestinationIsAFile.setSelection( action.destinationIsAFile );
    wCreateDestinationFolder.setSelection( action.createDestinationFolder );

    wAddFileToResult.setSelection( action.addResultFilesname );

    wCreateMoveToFolder.setSelection( action.createMoveToFolder );

    if ( action.getNrErrorsLessThan() != null ) {
      wNrErrorsLessThan.setText( action.getNrErrorsLessThan() );
    } else {
      wNrErrorsLessThan.setText( "10" );
    }

    if ( action.getSuccessCondition() != null ) {
      if ( action.getSuccessCondition().equals( action.SUCCESS_IF_AT_LEAST_X_FILES_UN_ZIPPED ) ) {
        wSuccessCondition.select( 1 );
      } else if ( action.getSuccessCondition().equals( action.SUCCESS_IF_ERRORS_LESS ) ) {
        wSuccessCondition.select( 2 );
      } else {
        wSuccessCondition.select( 0 );
      }
    } else {
      wSuccessCondition.select( 0 );
    }

    if ( action.getIfFileExists() != null ) {
      if ( action.getIfFileExists().equals( "overwrite_file" ) ) {
        wIfFileExists.select( 1 );
      } else if ( action.getIfFileExists().equals( "unique_name" ) ) {
        wIfFileExists.select( 2 );
      } else if ( action.getIfFileExists().equals( "delete_file" ) ) {
        wIfFileExists.select( 3 );
      } else if ( action.getIfFileExists().equals( "move_file" ) ) {
        wIfFileExists.select( 4 );
      } else if ( action.getIfFileExists().equals( "fail" ) ) {
        wIfFileExists.select( 5 );
      } else {
        wIfFileExists.select( 0 );
      }

    } else {
      wIfFileExists.select( 0 );
    }

    if ( action.getDestinationFolder() != null ) {
      wDestinationFolder.setText( action.getDestinationFolder() );
    }

    if ( action.getIfMovedFileExists() != null ) {
      if ( action.getIfMovedFileExists().equals( "overwrite_file" ) ) {
        wIfMovedFileExists.select( 1 );
      } else if ( action.getIfMovedFileExists().equals( "unique_name" ) ) {
        wIfMovedFileExists.select( 2 );
      } else if ( action.getIfMovedFileExists().equals( "fail" ) ) {
        wIfMovedFileExists.select( 3 );
      } else {
        wIfMovedFileExists.select( 0 );
      }

    } else {
      wIfMovedFileExists.select( 0 );
    }
    wDoNotKeepFolderStructure.setSelection( action.isDoNotKeepFolderStructure() );
    wAddDateBeforeExtension.setSelection( action.isAddDateBeforeExtension() );

    wAddDate.setSelection( action.isAddDate() );
    wAddTime.setSelection( action.isAddTime() );
    wSpecifyFormat.setSelection( action.isSpecifyFormat() );
    if ( action.getDateTimeFormat() != null ) {
      wDateTimeFormat.setText( action.getDateTimeFormat() );
    }

    if ( action.getGPGLocation() != null ) {
      wGpgExe.setText( action.getGPGLocation() );
    }

    wAddMovedDate.setSelection( action.isAddMovedDate() );
    wAddMovedTime.setSelection( action.isAddMovedTime() );
    wSpecifyMoveFormat.setSelection( action.isSpecifyMoveFormat() );
    if ( action.getMovedDateTimeFormat() != null ) {
      wMovedDateTimeFormat.setText( action.getMovedDateTimeFormat() );
    }
    wAddMovedDateBeforeExtension.setSelection( action.isAddMovedDateBeforeExtension() );

    wName.selectAll();
    wName.setFocus();
  }

  private void cancel() {
    action.setChanged( changed );
    action = null;
    dispose();
  }

  private void ok() {
    if ( Utils.isEmpty( wName.getText() ) ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( "Please give this action a name!" );
      mb.setText( "Enter a name" );
      mb.open();
      return;
    }
    action.setName( wName.getText() );
    action.setIncludeSubfolders( wIncludeSubfolders.getSelection() );
    action.setAsciiMode( wasciiMode.getSelection() );
    action.setArgFromPrevious( wPrevious.getSelection() );
    action.setAddresultfilesname( wAddFileToResult.getSelection() );
    action.setDestinationIsAFile( wDestinationIsAFile.getSelection() );
    action.setCreateDestinationFolder( wCreateDestinationFolder.getSelection() );
    action.setNrErrorsLessThan( wNrErrorsLessThan.getText() );

    action.setCreateMoveToFolder( wCreateMoveToFolder.getSelection() );

    if ( wSuccessCondition.getSelectionIndex() == 1 ) {
      action.setSuccessCondition( action.SUCCESS_IF_AT_LEAST_X_FILES_UN_ZIPPED );
    } else if ( wSuccessCondition.getSelectionIndex() == 2 ) {
      action.setSuccessCondition( action.SUCCESS_IF_ERRORS_LESS );
    } else {
      action.setSuccessCondition( action.SUCCESS_IF_NO_ERRORS );
    }

    if ( wIfFileExists.getSelectionIndex() == 1 ) {
      action.setIfFileExists( "overwrite_file" );
    } else if ( wIfFileExists.getSelectionIndex() == 2 ) {
      action.setIfFileExists( "unique_name" );
    } else if ( wIfFileExists.getSelectionIndex() == 3 ) {
      action.setIfFileExists( "delete_file" );
    } else if ( wIfFileExists.getSelectionIndex() == 4 ) {
      action.setIfFileExists( "move_file" );
    } else if ( wIfFileExists.getSelectionIndex() == 5 ) {
      action.setIfFileExists( "fail" );
    } else {
      action.setIfFileExists( "do_nothing" );
    }

    action.setDestinationFolder( wDestinationFolder.getText() );

    action.setGPGLocation( wGpgExe.getText() );

    if ( wIfMovedFileExists.getSelectionIndex() == 1 ) {
      action.setIfMovedFileExists( "overwrite_file" );
    } else if ( wIfMovedFileExists.getSelectionIndex() == 2 ) {
      action.setIfMovedFileExists( "unique_name" );
    } else if ( wIfMovedFileExists.getSelectionIndex() == 3 ) {
      action.setIfMovedFileExists( "fail" );
    } else {
      action.setIfMovedFileExists( "do_nothing" );
    }

    action.setDoNotKeepFolderStructure( wDoNotKeepFolderStructure.getSelection() );

    action.setAddDate( wAddDate.getSelection() );
    action.setAddTime( wAddTime.getSelection() );
    action.setSpecifyFormat( wSpecifyFormat.getSelection() );
    action.setDateTimeFormat( wDateTimeFormat.getText() );
    action.setAddDateBeforeExtension( wAddDateBeforeExtension.getSelection() );

    action.setAddMovedDate( wAddMovedDate.getSelection() );
    action.setAddMovedTime( wAddMovedTime.getSelection() );
    action.setSpecifyMoveFormat( wSpecifyMoveFormat.getSelection() );
    action.setMovedDateTimeFormat( wMovedDateTimeFormat.getText() );
    action.setAddMovedDateBeforeExtension( wAddMovedDateBeforeExtension.getSelection() );

    int nritems = wFields.nrNonEmpty();
    int nr = 0;
    for ( int i = 0; i < nritems; i++ ) {
      String arg = wFields.getNonEmpty( i ).getText( 1 );
      if ( arg != null && arg.length() != 0 ) {
        nr++;
      }
    }
    action.actionType = new int[ nr ];
    action.sourceFileFolder = new String[ nr ];
    action.userId = new String[ nr ];
    action.destinationFileFolder = new String[ nr ];
    action.wildcard = new String[ nr ];
    nr = 0;
    for ( int i = 0; i < nritems; i++ ) {
      String actionName = wFields.getNonEmpty( i ).getText( 1 );
      String source = wFields.getNonEmpty( i ).getText( 2 );
      String wild = wFields.getNonEmpty( i ).getText( 3 );
      String userid = wFields.getNonEmpty( i ).getText( 4 );
      String dest = wFields.getNonEmpty( i ).getText( 5 );

      if ( source != null && source.length() != 0 ) {
        action.actionType[ nr ] = ActionPGPEncryptFiles.getActionTypeByDesc( actionName );
        action.sourceFileFolder[ nr ] = source;
        action.wildcard[ nr ] = wild;
        action.userId[ nr ] = userid;
        action.destinationFileFolder[ nr ] = dest;
        nr++;
      }
    }
    dispose();
  }

  @Override
  public String toString() {
    return this.getClass().getName();
  }

  public boolean evaluates() {
    return true;
  }

  public boolean isUnconditional() {
    return false;
  }
}
