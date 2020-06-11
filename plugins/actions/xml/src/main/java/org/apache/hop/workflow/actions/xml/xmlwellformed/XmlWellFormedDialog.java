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

package org.apache.hop.workflow.actions.xml.xmlwellformed;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.annotations.PluginDialog;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.IActionDialog;
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

/**
 * This dialog allows you to edit the XML valid job entry settings.
 * 
 * @author Samatar Hassan
 * @since 26-03-2008
 */

@PluginDialog(
        id = "XML_WELL_FORMED",
        image = "XFC.svg",
        pluginType = PluginDialog.PluginType.ACTION,
        documentationUrl = "https://www.project-hop.org/manual/latest/plugins/actions/xmlwellformed.html"
)
public class XmlWellFormedDialog extends ActionDialog implements IActionDialog {
  private static Class<?> PKG = XmlWellFormed.class; // for i18n purposes, needed by Translator2!!

  private static final String[] FILETYPES = new String[] {
    BaseMessages.getString( PKG, "JobXMLWellFormed.Filetype.Xml" ),
    BaseMessages.getString( PKG, "JobXMLWellFormed.Filetype.All" ) };

  private Label wlName;
  private Text wName;
  private FormData fdlName, fdName;

  private Label wlSourceFileFolder;
  private Button wbSourceFileFolder, wbSourceDirectory;

  private TextVar wSourceFileFolder;
  private FormData fdlSourceFileFolder, fdbSourceFileFolder, fdSourceFileFolder, fdbSourceDirectory;

  private Label wlIncludeSubfolders;
  private Button wIncludeSubfolders;
  private FormData fdlIncludeSubfolders, fdIncludeSubfolders;

  private Button wOK, wCancel;
  private Listener lsOK, lsCancel;

  private XmlWellFormed jobEntry;
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

  private Label wlWildcard;
  private TextVar wWildcard;
  private FormData fdlWildcard, fdWildcard;

  private Button wbdSourceFileFolder; // Delete
  private Button wbeSourceFileFolder; // Edit
  private Button wbaSourceFileFolder; // Add or change

  private CTabFolder wTabFolder;
  private Composite wGeneralComp, wAdvancedComp;
  private CTabItem wGeneralTab, wAdvancedTab;
  private FormData fdGeneralComp, fdAdvancedComp;
  private FormData fdTabFolder;

  // Add File to result

  private Group wFileResult;
  private FormData fdFileResult;

  private Group wSuccessOn;
  private FormData fdSuccessOn;

  private FormData fdbeSourceFileFolder, fdbaSourceFileFolder, fdbdSourceFileFolder;

  private Label wlSuccessCondition, wlAddFilenameToResult;
  private CCombo wSuccessCondition, wAddFilenameToResult;
  private FormData fdlSuccessCondition, fdSuccessCondition, fdlAddFilenameToResult, fdAddFilenameToResult;

  private Label wlNrErrorsLessThan;
  private TextVar wNrErrorsLessThan;
  private FormData fdlNrErrorsLessThan, fdNrErrorsLessThan;

  public XmlWellFormedDialog(Shell parent, IAction jobEntryInt, WorkflowMeta workflowMeta ) {
    super( parent, jobEntryInt, workflowMeta );
    jobEntry = (XmlWellFormed) jobEntryInt;

    if ( this.jobEntry.getName() == null ) {
      this.jobEntry.setName( BaseMessages.getString( PKG, "JobXMLWellFormed.Name.Default" ) );
    }
  }

  public IAction open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.MIN | SWT.MAX | SWT.RESIZE );
    props.setLook( shell );
    WorkflowDialog.setShellImage( shell, jobEntry );

    ModifyListener lsMod = new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        jobEntry.setChanged();
      }
    };
    changed = jobEntry.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "JobXMLWellFormed.Title" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Filename line
    wlName = new Label( shell, SWT.RIGHT );
    wlName.setText( BaseMessages.getString( PKG, "JobXMLWellFormed.Name.Label" ) );
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
    wGeneralTab.setText( BaseMessages.getString( PKG, "JobXMLWellFormed.Tab.General.Label" ) );

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
    wSettings.setText( BaseMessages.getString( PKG, "JobXMLWellFormed.Settings.Label" ) );

    FormLayout groupLayout = new FormLayout();
    groupLayout.marginWidth = 10;
    groupLayout.marginHeight = 10;
    wSettings.setLayout( groupLayout );

    wlIncludeSubfolders = new Label( wSettings, SWT.RIGHT );
    wlIncludeSubfolders.setText( BaseMessages.getString( PKG, "JobXMLWellFormed.IncludeSubfolders.Label" ) );
    props.setLook( wlIncludeSubfolders );
    fdlIncludeSubfolders = new FormData();
    fdlIncludeSubfolders.left = new FormAttachment( 0, 0 );
    fdlIncludeSubfolders.top = new FormAttachment( wName, margin );
    fdlIncludeSubfolders.right = new FormAttachment( middle, -margin );
    wlIncludeSubfolders.setLayoutData( fdlIncludeSubfolders );
    wIncludeSubfolders = new Button( wSettings, SWT.CHECK );
    props.setLook( wIncludeSubfolders );
    wIncludeSubfolders.setToolTipText( BaseMessages.getString( PKG, "JobXMLWellFormed.IncludeSubfolders.Tooltip" ) );
    fdIncludeSubfolders = new FormData();
    fdIncludeSubfolders.left = new FormAttachment( middle, 0 );
    fdIncludeSubfolders.top = new FormAttachment( wName, margin );
    fdIncludeSubfolders.right = new FormAttachment( 100, 0 );
    wIncludeSubfolders.setLayoutData( fdIncludeSubfolders );
    wIncludeSubfolders.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        jobEntry.setChanged();
      }
    } );

    // previous
    wlPrevious = new Label( wSettings, SWT.RIGHT );
    wlPrevious.setText( BaseMessages.getString( PKG, "JobXMLWellFormed.Previous.Label" ) );
    props.setLook( wlPrevious );
    fdlPrevious = new FormData();
    fdlPrevious.left = new FormAttachment( 0, 0 );
    fdlPrevious.top = new FormAttachment( wIncludeSubfolders, margin );
    fdlPrevious.right = new FormAttachment( middle, -margin );
    wlPrevious.setLayoutData( fdlPrevious );
    wPrevious = new Button( wSettings, SWT.CHECK );
    props.setLook( wPrevious );
    wPrevious.setSelection( jobEntry.isArgFromPrevious() );
    wPrevious.setToolTipText( BaseMessages.getString( PKG, "JobXMLWellFormed.Previous.Tooltip" ) );
    fdPrevious = new FormData();
    fdPrevious.left = new FormAttachment( middle, 0 );
    fdPrevious.top = new FormAttachment( wIncludeSubfolders, margin );
    fdPrevious.right = new FormAttachment( 100, 0 );
    wPrevious.setLayoutData( fdPrevious );
    wPrevious.addSelectionListener( new SelectionAdapter() {
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
    wlSourceFileFolder.setText( BaseMessages.getString( PKG, "JobXMLWellFormed.SourceFileFolder.Label" ) );
    props.setLook( wlSourceFileFolder );
    fdlSourceFileFolder = new FormData();
    fdlSourceFileFolder.left = new FormAttachment( 0, 0 );
    fdlSourceFileFolder.top = new FormAttachment( wSettings, 2 * margin );
    fdlSourceFileFolder.right = new FormAttachment( middle, -margin );
    wlSourceFileFolder.setLayoutData( fdlSourceFileFolder );

    // Browse Source folders button ...
    wbSourceDirectory = new Button( wGeneralComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbSourceDirectory );
    wbSourceDirectory.setText( BaseMessages.getString( PKG, "JobXMLWellFormed.BrowseFolders.Label" ) );
    fdbSourceDirectory = new FormData();
    fdbSourceDirectory.right = new FormAttachment( 100, 0 );
    fdbSourceDirectory.top = new FormAttachment( wSettings, margin );
    wbSourceDirectory.setLayoutData( fdbSourceDirectory );

    wbSourceDirectory.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        DirectoryDialog ddialog = new DirectoryDialog( shell, SWT.OPEN );
        if ( wSourceFileFolder.getText() != null ) {
          ddialog.setFilterPath( workflowMeta.environmentSubstitute( wSourceFileFolder.getText() ) );
        }

        // Calling open() will open and run the dialog.
        // It will return the selected directory, or
        // null if user cancels
        String dir = ddialog.open();
        if ( dir != null ) {
          // Set the text box to the new selection
          wSourceFileFolder.setText( dir );
        }

      }
    } );

    // Browse Source files button ...
    wbSourceFileFolder = new Button( wGeneralComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbSourceFileFolder );
    wbSourceFileFolder.setText( BaseMessages.getString( PKG, "JobXMLWellFormed.BrowseFiles.Label" ) );
    fdbSourceFileFolder = new FormData();
    fdbSourceFileFolder.right = new FormAttachment( wbSourceDirectory, -margin );
    fdbSourceFileFolder.top = new FormAttachment( wSettings, margin );
    wbSourceFileFolder.setLayoutData( fdbSourceFileFolder );

    // Browse Destination file add button ...
    wbaSourceFileFolder = new Button( wGeneralComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbaSourceFileFolder );
    wbaSourceFileFolder.setText( BaseMessages.getString( PKG, "JobXMLWellFormed.FilenameAdd.Button" ) );
    fdbaSourceFileFolder = new FormData();
    fdbaSourceFileFolder.right = new FormAttachment( wbSourceFileFolder, -margin );
    fdbaSourceFileFolder.top = new FormAttachment( wSettings, margin );
    wbaSourceFileFolder.setLayoutData( fdbaSourceFileFolder );

    wSourceFileFolder = new TextVar( workflowMeta, wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wSourceFileFolder.setToolTipText( BaseMessages.getString( PKG, "JobXMLWellFormed.SourceFileFolder.Tooltip" ) );

    props.setLook( wSourceFileFolder );
    wSourceFileFolder.addModifyListener( lsMod );
    fdSourceFileFolder = new FormData();
    fdSourceFileFolder.left = new FormAttachment( middle, 0 );
    fdSourceFileFolder.top = new FormAttachment( wSettings, 2 * margin );
    fdSourceFileFolder.right = new FormAttachment( wbSourceFileFolder, -55 );
    wSourceFileFolder.setLayoutData( fdSourceFileFolder );

    // Whenever something changes, set the tooltip to the expanded version:
    wSourceFileFolder.addModifyListener( new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        wSourceFileFolder.setToolTipText( workflowMeta.environmentSubstitute( wSourceFileFolder.getText() ) );
      }
    } );

    wbSourceFileFolder.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        FileDialog dialog = new FileDialog( shell, SWT.OPEN );
        dialog.setFilterExtensions( new String[] { "*.xml;*.XML", "*" } );
        if ( wSourceFileFolder.getText() != null ) {
          dialog.setFileName( workflowMeta.environmentSubstitute( wSourceFileFolder.getText() ) );
        }
        dialog.setFilterNames( FILETYPES );
        if ( dialog.open() != null ) {
          wSourceFileFolder.setText( dialog.getFilterPath() + Const.FILE_SEPARATOR + dialog.getFileName() );
        }
      }
    } );

    // Buttons to the right of the screen...
    wbdSourceFileFolder = new Button( wGeneralComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbdSourceFileFolder );
    wbdSourceFileFolder.setText( BaseMessages.getString( PKG, "JobXMLWellFormed.FilenameDelete.Button" ) );
    wbdSourceFileFolder.setToolTipText( BaseMessages.getString( PKG, "JobXMLWellFormed.FilenameDelete.Tooltip" ) );
    fdbdSourceFileFolder = new FormData();
    fdbdSourceFileFolder.right = new FormAttachment( 100, 0 );
    fdbdSourceFileFolder.top = new FormAttachment( wSourceFileFolder, 40 );
    wbdSourceFileFolder.setLayoutData( fdbdSourceFileFolder );

    wbeSourceFileFolder = new Button( wGeneralComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbeSourceFileFolder );
    wbeSourceFileFolder.setText( BaseMessages.getString( PKG, "JobXMLWellFormed.FilenameEdit.Button" ) );
    wbeSourceFileFolder.setToolTipText( BaseMessages.getString( PKG, "JobXMLWellFormed.FilenameEdit.Tooltip" ) );
    fdbeSourceFileFolder = new FormData();
    fdbeSourceFileFolder.right = new FormAttachment( 100, 0 );
    fdbeSourceFileFolder.left = new FormAttachment( wbdSourceFileFolder, 0, SWT.LEFT );
    fdbeSourceFileFolder.top = new FormAttachment( wbdSourceFileFolder, margin );
    wbeSourceFileFolder.setLayoutData( fdbeSourceFileFolder );

    // Wildcard
    wlWildcard = new Label( wGeneralComp, SWT.RIGHT );
    wlWildcard.setText( BaseMessages.getString( PKG, "JobXMLWellFormed.Wildcard.Label" ) );
    props.setLook( wlWildcard );
    fdlWildcard = new FormData();
    fdlWildcard.left = new FormAttachment( 0, 0 );
    fdlWildcard.top = new FormAttachment( wSourceFileFolder, margin );
    fdlWildcard.right = new FormAttachment( middle, -margin );
    wlWildcard.setLayoutData( fdlWildcard );

    wWildcard = new TextVar( workflowMeta, wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wWildcard.setToolTipText( BaseMessages.getString( PKG, "JobXMLWellFormed.Wildcard.Tooltip" ) );
    props.setLook( wWildcard );
    wWildcard.addModifyListener( lsMod );
    fdWildcard = new FormData();
    fdWildcard.left = new FormAttachment( middle, 0 );
    fdWildcard.top = new FormAttachment( wSourceFileFolder, margin );
    fdWildcard.right = new FormAttachment( wbSourceFileFolder, -55 );
    wWildcard.setLayoutData( fdWildcard );

    wlFields = new Label( wGeneralComp, SWT.NONE );
    wlFields.setText( BaseMessages.getString( PKG, "JobXMLWellFormed.Fields.Label" ) );
    props.setLook( wlFields );
    fdlFields = new FormData();
    fdlFields.left = new FormAttachment( 0, 0 );
    fdlFields.right = new FormAttachment( middle, -margin );
    fdlFields.top = new FormAttachment( wWildcard, margin );
    wlFields.setLayoutData( fdlFields );

    int rows =
        jobEntry.getSourceFileFolders() == null ? 1 : ( jobEntry.getSourceFileFolders().length == 0 ? 0 : jobEntry
            .getSourceFileFolders().length );
    final int FieldsRows = rows;

    ColumnInfo[] colinf =
        new ColumnInfo[] {
          new ColumnInfo( BaseMessages.getString( PKG, "JobXMLWellFormed.Fields.SourceFileFolder.Label" ),
              ColumnInfo.COLUMN_TYPE_TEXT, false ),
          new ColumnInfo( BaseMessages.getString( PKG, "JobXMLWellFormed.Fields.Wildcard.Label" ),
              ColumnInfo.COLUMN_TYPE_TEXT, false ), };

    colinf[0].setUsingVariables( true );
    colinf[0].setToolTip( BaseMessages.getString( PKG, "JobXMLWellFormed.Fields.SourceFileFolder.Tooltip" ) );
    colinf[1].setUsingVariables( true );
    colinf[1].setToolTip( BaseMessages.getString( PKG, "JobXMLWellFormed.Fields.Wildcard.Tooltip" ) );

    wFields =
        new TableView( workflowMeta, wGeneralComp, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, FieldsRows, lsMod,
            props );

    fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment( wlFields, margin );
    fdFields.right = new FormAttachment( 100, -75 );
    fdFields.bottom = new FormAttachment( 100, -margin );
    wFields.setLayoutData( fdFields );

    RefreshArgFromPrevious();

    // Add the file to the list of files...
    SelectionAdapter selA = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        wFields.add( new String[] { wSourceFileFolder.getText(), wWildcard.getText() } );
        wSourceFileFolder.setText( "" );

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
      public void widgetSelected( SelectionEvent arg0 ) {
        int[] idx = wFields.getSelectionIndices();
        wFields.remove( idx );
        wFields.removeEmptyRows();
        wFields.setRowNums();
      }
    } );

    // Edit the selected file & remove from the list...
    wbeSourceFileFolder.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        int idx = wFields.getSelectionIndex();
        if ( idx >= 0 ) {
          String[] string = wFields.getItem( idx );
          wSourceFileFolder.setText( string[0] );
          wWildcard.setText( string[1] );
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
    // START OF ADVANCED TAB ///
    // ///////////////////////////////////

    wAdvancedTab = new CTabItem( wTabFolder, SWT.NONE );
    wAdvancedTab.setText( BaseMessages.getString( PKG, "JobXMLWellFormed.Tab.Advanced.Label" ) );

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
    wSuccessOn.setText( BaseMessages.getString( PKG, "JobXMLWellFormed.SuccessOn.Group.Label" ) );

    FormLayout successongroupLayout = new FormLayout();
    successongroupLayout.marginWidth = 10;
    successongroupLayout.marginHeight = 10;

    wSuccessOn.setLayout( successongroupLayout );

    // Success Condition
    wlSuccessCondition = new Label( wSuccessOn, SWT.RIGHT );
    wlSuccessCondition.setText( BaseMessages.getString( PKG, "JobXMLWellFormed.SuccessCondition.Label" ) );
    props.setLook( wlSuccessCondition );
    fdlSuccessCondition = new FormData();
    fdlSuccessCondition.left = new FormAttachment( 0, 0 );
    fdlSuccessCondition.right = new FormAttachment( middle, 0 );
    fdlSuccessCondition.top = new FormAttachment( 0, margin );
    wlSuccessCondition.setLayoutData( fdlSuccessCondition );
    wSuccessCondition = new CCombo( wSuccessOn, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wSuccessCondition.add( BaseMessages.getString( PKG, "JobXMLWellFormed.SuccessWhenAllWorksFine.Label" ) );
    wSuccessCondition.add( BaseMessages.getString( PKG, "JobXMLWellFormed.SuccessWhenAtLeat.Label" ) );
    wSuccessCondition.add( BaseMessages.getString( PKG, "JobXMLWellFormed.SuccessWhenBadFormedLessThan.Label" ) );
    wSuccessCondition.select( 0 ); // +1: starts at -1

    props.setLook( wSuccessCondition );
    fdSuccessCondition = new FormData();
    fdSuccessCondition.left = new FormAttachment( middle, 0 );
    fdSuccessCondition.top = new FormAttachment( 0, margin );
    fdSuccessCondition.right = new FormAttachment( 100, 0 );
    wSuccessCondition.setLayoutData( fdSuccessCondition );
    wSuccessCondition.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        activeSuccessCondition();

      }
    } );

    // Success when number of errors less than
    wlNrErrorsLessThan = new Label( wSuccessOn, SWT.RIGHT );
    wlNrErrorsLessThan.setText( BaseMessages.getString( PKG, "JobXMLWellFormed.NrBadFormedLessThan.Label" ) );
    props.setLook( wlNrErrorsLessThan );
    fdlNrErrorsLessThan = new FormData();
    fdlNrErrorsLessThan.left = new FormAttachment( 0, 0 );
    fdlNrErrorsLessThan.top = new FormAttachment( wSuccessCondition, margin );
    fdlNrErrorsLessThan.right = new FormAttachment( middle, -margin );
    wlNrErrorsLessThan.setLayoutData( fdlNrErrorsLessThan );

    wNrErrorsLessThan =
        new TextVar( workflowMeta, wSuccessOn, SWT.SINGLE | SWT.LEFT | SWT.BORDER, BaseMessages.getString( PKG,
            "JobXMLWellFormed.NrBadFormedLessThan.Tooltip" ) );
    props.setLook( wNrErrorsLessThan );
    wNrErrorsLessThan.addModifyListener( lsMod );
    fdNrErrorsLessThan = new FormData();
    fdNrErrorsLessThan.left = new FormAttachment( middle, 0 );
    fdNrErrorsLessThan.top = new FormAttachment( wSuccessCondition, margin );
    fdNrErrorsLessThan.right = new FormAttachment( 100, -margin );
    wNrErrorsLessThan.setLayoutData( fdNrErrorsLessThan );

    fdSuccessOn = new FormData();
    fdSuccessOn.left = new FormAttachment( 0, margin );
    fdSuccessOn.top = new FormAttachment( 0, margin );
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
    wFileResult.setText( BaseMessages.getString( PKG, "JobXMLWellFormed.FileResult.Group.Label" ) );

    FormLayout fileresultgroupLayout = new FormLayout();
    fileresultgroupLayout.marginWidth = 10;
    fileresultgroupLayout.marginHeight = 10;

    wFileResult.setLayout( fileresultgroupLayout );

    // Add Filenames to result filenames?
    wlAddFilenameToResult = new Label( wFileResult, SWT.RIGHT );
    wlAddFilenameToResult.setText( BaseMessages.getString( PKG, "JobXMLWellFormed.AddFilenameToResult.Label" ) );
    props.setLook( wlAddFilenameToResult );
    fdlAddFilenameToResult = new FormData();
    fdlAddFilenameToResult.left = new FormAttachment( 0, 0 );
    fdlAddFilenameToResult.right = new FormAttachment( middle, 0 );
    fdlAddFilenameToResult.top = new FormAttachment( 0, margin );
    wlAddFilenameToResult.setLayoutData( fdlAddFilenameToResult );
    wAddFilenameToResult = new CCombo( wFileResult, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wAddFilenameToResult.add( BaseMessages.getString( PKG, "JobXMLWellFormed.AddAllFilenamesToResult.Label" ) );
    wAddFilenameToResult.add( BaseMessages.getString( PKG, "JobXMLWellFormed.AddOnlyWellFormedFilenames.Label" ) );
    wAddFilenameToResult.add( BaseMessages.getString( PKG, "JobXMLWellFormed.AddOnlyBadFormedFilenames.Label" ) );
    wAddFilenameToResult.select( 0 ); // +1: starts at -1

    props.setLook( wAddFilenameToResult );
    fdAddFilenameToResult = new FormData();
    fdAddFilenameToResult.left = new FormAttachment( middle, 0 );
    fdAddFilenameToResult.top = new FormAttachment( 0, margin );
    fdAddFilenameToResult.right = new FormAttachment( 100, 0 );
    wAddFilenameToResult.setLayoutData( fdAddFilenameToResult );
    wAddFilenameToResult.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {

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

    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    BaseTransformDialog.positionBottomButtons( shell, new Button[] { wOK, wCancel }, margin, wTabFolder );

    // Add listeners
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };
    lsOK = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };

    wCancel.addListener( SWT.Selection, lsCancel );
    wOK.addListener( SWT.Selection, lsOK );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wName.addSelectionListener( lsDef );
    wSourceFileFolder.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();
    activeSuccessCondition();

    activeSuccessCondition();

    wTabFolder.setSelection( 0 );
    BaseTransformDialog.setSize( shell );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return jobEntry;
  }

  private void activeSuccessCondition() {
    wlNrErrorsLessThan.setEnabled( wSuccessCondition.getSelectionIndex() != 0 );
    wNrErrorsLessThan.setEnabled( wSuccessCondition.getSelectionIndex() != 0 );
  }

  private void RefreshArgFromPrevious() {

    wlFields.setEnabled( !wPrevious.getSelection() );
    wFields.setEnabled( !wPrevious.getSelection() );
    wbdSourceFileFolder.setEnabled( !wPrevious.getSelection() );
    wbeSourceFileFolder.setEnabled( !wPrevious.getSelection() );
    wbSourceFileFolder.setEnabled( !wPrevious.getSelection() );
    wbaSourceFileFolder.setEnabled( !wPrevious.getSelection() );
    wlSourceFileFolder.setEnabled( !wPrevious.getSelection() );
    wSourceFileFolder.setEnabled( !wPrevious.getSelection() );

    wlWildcard.setEnabled( !wPrevious.getSelection() );
    wWildcard.setEnabled( !wPrevious.getSelection() );
    wbSourceDirectory.setEnabled( !wPrevious.getSelection() );
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
    wName.setText( Const.nullToEmpty( jobEntry.getName() ) );

    if ( jobEntry.getSourceFileFolders() != null ) {
      for ( int i = 0; i < jobEntry.getSourceFileFolders().length; i++ ) {
        TableItem ti = wFields.table.getItem( i );
        if ( jobEntry.getSourceFileFolders()[i] != null ) {
          ti.setText( 1, jobEntry.getSourceFileFolders()[i] );
        }

        if ( jobEntry.getSourceWildcards()[i] != null ) {
          ti.setText( 2, jobEntry.getSourceWildcards()[i] );
        }
      }
      wFields.setRowNums();
      wFields.optWidth( true );
    }
    wPrevious.setSelection( jobEntry.isArgFromPrevious() );
    wIncludeSubfolders.setSelection( jobEntry.isIncludeSubfolders() );

    wNrErrorsLessThan.setText( Const.NVL( jobEntry.getNrErrorsLessThan(), "10" ) );

    if ( jobEntry.getSuccessCondition() != null ) {
      if ( jobEntry.getSuccessCondition().equals( XmlWellFormed.SUCCESS_IF_AT_LEAST_X_FILES_WELL_FORMED ) ) {
        wSuccessCondition.select( 1 );
      } else if ( jobEntry.getSuccessCondition().equals( XmlWellFormed.SUCCESS_IF_BAD_FORMED_FILES_LESS ) ) {
        wSuccessCondition.select( 2 );
      } else {
        wSuccessCondition.select( 0 );
      }
    } else {
      wSuccessCondition.select( 0 );
    }

    if ( jobEntry.getResultFilenames() != null ) {
      if ( jobEntry.getResultFilenames().equals( XmlWellFormed.ADD_WELL_FORMED_FILES_ONLY ) ) {
        wAddFilenameToResult.select( 1 );
      } else if ( jobEntry.getResultFilenames().equals( XmlWellFormed.ADD_BAD_FORMED_FILES_ONLY ) ) {
        wAddFilenameToResult.select( 2 );
      } else {
        wAddFilenameToResult.select( 0 );
      }
    } else {
      wAddFilenameToResult.select( 0 );
    }

    wName.selectAll();
    wName.setFocus();
  }

  private void cancel() {
    jobEntry.setChanged( changed );
    jobEntry = null;
    dispose();
  }

  private void ok() {
    if ( Utils.isEmpty( wName.getText() ) ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setText( BaseMessages.getString( PKG, "System.StepJobEntryNameMissing.Title" ) );
      mb.setMessage( BaseMessages.getString( PKG, "System.JobEntryNameMissing.Msg" ) );
      mb.open();
      return;
    }
    jobEntry.setName( wName.getText() );

    jobEntry.setIncludeSubfolders( wIncludeSubfolders.getSelection() );
    jobEntry.setArgFromPrevious( wPrevious.getSelection() );

    jobEntry.setNrErrorsLessThan( wNrErrorsLessThan.getText() );

    if ( wSuccessCondition.getSelectionIndex() == 1 ) {
      jobEntry.setSuccessCondition( XmlWellFormed.SUCCESS_IF_AT_LEAST_X_FILES_WELL_FORMED );
    } else if ( wSuccessCondition.getSelectionIndex() == 2 ) {
      jobEntry.setSuccessCondition( XmlWellFormed.SUCCESS_IF_BAD_FORMED_FILES_LESS );
    } else {
      jobEntry.setSuccessCondition( XmlWellFormed.SUCCESS_IF_NO_ERRORS );
    }

    if ( wAddFilenameToResult.getSelectionIndex() == 1 ) {
      jobEntry.setResultFilenames( XmlWellFormed.ADD_WELL_FORMED_FILES_ONLY );
    } else if ( wAddFilenameToResult.getSelectionIndex() == 2 ) {
      jobEntry.setResultFilenames( XmlWellFormed.ADD_BAD_FORMED_FILES_ONLY );
    } else {
      jobEntry.setResultFilenames( XmlWellFormed.ADD_ALL_FILENAMES );
    }

    int nritems = wFields.nrNonEmpty();
    int nr = 0;
    for ( int i = 0; i < nritems; i++ ) {
      String arg = wFields.getNonEmpty( i ).getText( 1 );
      if ( arg != null && arg.length() != 0 ) {
        nr++;
      }
    }
    String[] source_filefolder = new String[nr];
    String[] wildcard = new String[nr];
    nr = 0;
    for ( int i = 0; i < nritems; i++ ) {
      String source = wFields.getNonEmpty( i ).getText( 1 );
      String wild = wFields.getNonEmpty( i ).getText( 2 );
      if ( source != null && source.length() != 0 ) {
        source_filefolder[nr] = source;
        wildcard[nr] = wild;
        nr++;
      }
    }
    jobEntry.setSourceFileFolders( source_filefolder );
    jobEntry.setSourceWildcards( wildcard );
    dispose();
  }

  public boolean evaluates() {
    return true;
  }

  public boolean isUnconditional() {
    return false;
  }
}
