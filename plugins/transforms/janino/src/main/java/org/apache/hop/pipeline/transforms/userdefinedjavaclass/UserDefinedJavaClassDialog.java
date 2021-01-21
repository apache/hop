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

package org.apache.hop.pipeline.transforms.userdefinedjavaclass;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.rowgenerator.RowGeneratorMeta;
import org.apache.hop.pipeline.transforms.userdefinedjavaclass.UserDefinedJavaClassCodeSnippits.Category;
import org.apache.hop.pipeline.transforms.userdefinedjavaclass.UserDefinedJavaClassCodeSnippits.Snippit;
import org.apache.hop.pipeline.transforms.userdefinedjavaclass.UserDefinedJavaClassDef.ClassType;
import org.apache.hop.pipeline.transforms.userdefinedjavaclass.UserDefinedJavaClassMeta.FieldInfo;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.dialog.EnterTextDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.PreviewRowsDialog;
import org.apache.hop.ui.core.dialog.ShowMessageDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.StyledTextComp;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.hopgui.TextSizeUtilFacade;
import org.apache.hop.ui.pipeline.dialog.PipelinePreviewProgressDialog;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.util.SwtSvgImageUtil;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.*;
import org.eclipse.swt.dnd.*;
import org.eclipse.swt.events.*;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.util.List;
import java.util.*;

public class UserDefinedJavaClassDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = UserDefinedJavaClassMeta.class; // For Translator

  private ModifyListener lsMod;
  private SashForm wSash;

  private TableView wFields;

  private Label wlPosition;

  private Button wClearResultFields;

  private Text wlHelpLabel;

  private Button wTest;
  private Listener lsTest;

  protected Button wCreatePlugin;
  protected Listener lsCreatePlugin;

  private Tree wTree;
  private TreeItem wTreeClassesItem;
  private Listener lsTree;

  private Image imageActiveScript, imageInactiveScript;
  

  private CTabFolder folder, wTabFolder;
  private Menu cMenu, tMenu;

  // Suport for Rename Tree
  private TreeItem[] lastItem;
  private TreeEditor editor;

  private enum TabActions {
    DELETE_ITEM, ADD_ITEM, RENAME_ITEM, SET_ACTIVE_ITEM
  }

  private enum TabAddActions {
    ADD_COPY, ADD_BLANK, ADD_DEFAULT
  }

  private String strActiveScript;

  private UserDefinedJavaClassMeta input;
  private UserDefinedJavaClassCodeSnippits snippitsHelper;

  private static GuiResource guiResource = GuiResource.getInstance();

  private TreeItem itemInput, itemInfo, itemOutput;

  private TreeItem itemWaitFieldsIn, itemWaitFieldsInfo, itemWaitFieldsOut;

  private IRowMeta inputRowMeta, infoRowMeta, outputRowMeta;

  private RowGeneratorMeta genMeta;

  private CTabItem fieldsTab;

  private int middle, margin;

  private CTabItem infoTab, targetTab, parametersTab;
  private TableView wInfoTransforms, wTargetTransforms, wParameters;

  private String[] prevTransformNames, nextTransformNames;

  public UserDefinedJavaClassDialog( Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, pipelineMeta, sname );
    input = (UserDefinedJavaClassMeta) in;
    genMeta = null;
    try {      
      imageActiveScript = SwtSvgImageUtil.getImage(parent.getDisplay(), getClass().getClassLoader(), "userdefinedjavaclass.svg", ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE);
      imageInactiveScript = SwtSvgImageUtil.getImage(parent.getDisplay(), getClass().getClassLoader(), "userdefinedjavaclass.svg", ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE);
    } catch ( Exception e ) {
      imageActiveScript = guiResource.getImageEmpty();
      imageInactiveScript = guiResource.getImageEmpty();
    }

    try {
      snippitsHelper = UserDefinedJavaClassCodeSnippits.getSnippitsHelper();
    } catch ( Exception e ) {
      new ErrorDialog(
        shell, "Unexpected error", "There was an unexpected error reading the code snippets file", e );
    }

  }

  public String open() {

    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, input );

    lsMod = new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        input.setChanged();
      }
    };
    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "UserDefinedJavaClassDialog.Shell.Title" ) );

    middle = props.getMiddlePct();
    margin = props.getMargin();

    // Filename line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "UserDefinedJavaClassDialog.TransformName.Label" ) );
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

    wSash = new SashForm( shell, SWT.VERTICAL );

    // Top sash form
    //
    Composite wTop = new Composite( wSash, SWT.NONE );
    props.setLook( wTop );

    FormLayout topLayout = new FormLayout();
    topLayout.marginWidth = Const.FORM_MARGIN;
    topLayout.marginHeight = Const.FORM_MARGIN;
    wTop.setLayout( topLayout );

    // Script line
    Label wlScriptFunctions = new Label( wTop, SWT.NONE );
    wlScriptFunctions
      .setText( BaseMessages.getString( PKG, "UserDefinedJavaClassDialog.ClassesAndSnippits.Label" ) );
    props.setLook( wlScriptFunctions );
    FormData fdlScriptFunctions = new FormData();
    fdlScriptFunctions.left = new FormAttachment( 0, 0 );
    fdlScriptFunctions.top = new FormAttachment( 0, 0 );
    wlScriptFunctions.setLayoutData( fdlScriptFunctions );

    // Tree View Test
    wTree = new Tree( wTop, SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL );
    props.setLook( wTree );
    FormData fdlTree = new FormData();
    fdlTree.left = new FormAttachment( 0, 0 );
    fdlTree.top = new FormAttachment( wlScriptFunctions, margin );
    fdlTree.right = new FormAttachment( 20, 0 );
    fdlTree.bottom = new FormAttachment( 100, -margin );
    wTree.setLayoutData( fdlTree );

    // Script line
    Label wlScript = new Label( wTop, SWT.NONE );
    wlScript.setText( BaseMessages.getString( PKG, "UserDefinedJavaClassDialog.Class.Label" ) );
    props.setLook( wlScript );
    FormData fdlScript = new FormData();
    fdlScript.left = new FormAttachment( wTree, margin );
    fdlScript.top = new FormAttachment( 0, 0 );
    wlScript.setLayoutData( fdlScript );

    folder = new CTabFolder( wTop, SWT.BORDER | SWT.RESIZE );
    folder.setUnselectedImageVisible( true );
    folder.setUnselectedCloseVisible( true );
    FormData fdScript = new FormData();
    fdScript.left = new FormAttachment( wTree, margin );
    fdScript.top = new FormAttachment( wlScript, margin );
    fdScript.right = new FormAttachment( 100, -5 );
    fdScript.bottom = new FormAttachment( 100, -50 );
    folder.setLayoutData( fdScript );

    wlPosition = new Label( wTop, SWT.NONE );
    wlPosition.setText( BaseMessages.getString( PKG, "UserDefinedJavaClassDialog.Position.Label" ) );
    props.setLook( wlPosition );
    FormData fdlPosition = new FormData();
    fdlPosition.left = new FormAttachment( wTree, margin );
    fdlPosition.right = new FormAttachment( 30, 0 );
    fdlPosition.top = new FormAttachment( folder, margin );
    wlPosition.setLayoutData( fdlPosition );

    wlHelpLabel = new Text( wTop, SWT.V_SCROLL | SWT.LEFT );
    wlHelpLabel.setEditable( false );
    wlHelpLabel.setText( "Hallo" );
    props.setLook( wlHelpLabel );
    FormData fdHelpLabel = new FormData();
    fdHelpLabel.left = new FormAttachment( wlPosition, margin );
    fdHelpLabel.top = new FormAttachment( folder, margin );
    fdHelpLabel.right = new FormAttachment( 100, -5 );
    fdHelpLabel.bottom = new FormAttachment( 100, 0 );
    wlHelpLabel.setLayoutData( fdHelpLabel );
    wlHelpLabel.setVisible( false );

    FormData fdTop = new FormData();
    fdTop.left = new FormAttachment( 0, 0 );
    fdTop.top = new FormAttachment( 0, 0 );
    fdTop.right = new FormAttachment( 100, 0 );
    fdTop.bottom = new FormAttachment( 100, 0 );
    wTop.setLayoutData( fdTop );

    //
    // Add a tab folder for the parameters and various input and output
    // streams
    //
    wTabFolder = new CTabFolder( wSash, SWT.BORDER );
    props.setLook( wTabFolder, Props.WIDGET_STYLE_TAB );
    wTabFolder.setUnselectedCloseVisible( false );

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.top = new FormAttachment( 0, 0 );
    fdTabFolder.bottom = new FormAttachment( 100, -75 );
    wTabFolder.setLayoutData( fdTabFolder );

    // The Fields tab...
    //
    addFieldsTab();

    // The parameters
    //
    addParametersTab();

    prevTransformNames = pipelineMeta.getPrevTransformNames( transformMeta );
    nextTransformNames = pipelineMeta.getNextTransformNames( transformMeta );

    // OK, add another tab for the input settings...
    //
    addInfoTab();
    addTargetTab();

    // Select the fields tab...
    //
    wTabFolder.setSelection( fieldsTab );

    FormData fdSash = new FormData();
    fdSash.left = new FormAttachment( 0, 0 );
    fdSash.top = new FormAttachment( wTransformName, 0 );
    fdSash.right = new FormAttachment( 100, 0 );
    fdSash.bottom = new FormAttachment( 100, -50 );
    wSash.setLayoutData( fdSash );

    wSash.setWeights( new int[] { 75, 25 } );

    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wTest = new Button( shell, SWT.PUSH );
    wTest.setText( BaseMessages.getString( PKG, "UserDefinedJavaClassDialog.TestClass.Button" ) );
    // wCreatePlugin = new Button(shell, SWT.PUSH);
    //wCreatePlugin.setText("Create Plug-in");
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOk, wCancel, wTest /* , wCreatePlugin */ }, margin, null );

    lsCancel = e -> cancel();
    lsTest = e -> test();
    lsOk = e -> ok();
    lsTree = e -> treeDblClick( e );
    /*
     * lsCreatePlugin = new Listener() { public void handleEvent(Event e) { createPlugin(); } };
     */

    wCancel.addListener( SWT.Selection, lsCancel );
    wTest.addListener( SWT.Selection, lsTest );
    wOk.addListener( SWT.Selection, lsOk );
    // wCreatePlugin.addListener(SWT.Selection, lsCreatePlugin);
    wTree.addListener( SWT.MouseDoubleClick, lsTree );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };
    wTransformName.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        if ( !cancel() ) {
          e.doit = false;
        }
      }
    } );

    folder.addCTabFolder2Listener( new CTabFolder2Adapter() {
      public void close( CTabFolderEvent event ) {
        CTabItem cItem = (CTabItem) event.item;
        event.doit = false;
        if ( cItem != null && folder.getItemCount() > 1 ) {
          MessageBox messageBox = new MessageBox( shell, SWT.ICON_QUESTION | SWT.NO | SWT.YES );
          messageBox.setText( BaseMessages.getString( PKG, "UserDefinedJavaClassDialog.DeleteItem.Label" ) );
          messageBox.setMessage( BaseMessages.getString(
            PKG, "UserDefinedJavaClassDialog.ConfirmDeleteItem.Label", cItem.getText() ) );
          switch ( messageBox.open() ) {
            case SWT.YES:
              modifyTabTree( cItem, TabActions.DELETE_ITEM );
              event.doit = true;
              break;
            default:
              break;
          }
        }
      }
    } );

    cMenu = new Menu( shell, SWT.POP_UP );
    buildingFolderMenu();
    tMenu = new Menu( shell, SWT.POP_UP );
    buildingTreeMenu();

    // Adding the Default Transform Class Item to the Tree
    wTreeClassesItem = new TreeItem( wTree, SWT.NULL );
    wTreeClassesItem.setImage( guiResource.getImageFolder() );
    wTreeClassesItem.setText( BaseMessages.getString( PKG, "UserDefinedJavaClassDialog.Classes.Label" ) );

    // Set the shell size, based upon previous time...
    setSize();
    getData();

    // Adding the Rest (Functions, InputItems, etc.) to the Tree
    buildSnippitsTree();

    // Input Fields
    itemInput = new TreeItem( wTree, SWT.NULL );
    itemInput.setImage( guiResource.getImageInput() );
    itemInput.setText( BaseMessages.getString( PKG, "UserDefinedJavaClassDialog.InputFields.Label" ) );
    itemInput.setData( "Field Helpers" );
    // Info Fields
    itemInfo = new TreeItem( wTree, SWT.NULL );
    itemInfo.setImage( guiResource.getImageInput() );
    itemInfo.setText( BaseMessages.getString( PKG, "UserDefinedJavaClassDialog.InfoFields.Label" ) );
    itemInfo.setData( "Field Helpers" );
    // Output Fields
    itemOutput = new TreeItem( wTree, SWT.NULL );
    itemOutput.setImage( guiResource.getImageOutput() );
    itemOutput.setText( BaseMessages.getString( PKG, "UserDefinedJavaClassDialog.OutputFields.Label" ) );
    itemOutput.setData( "Field Helpers" );

    // Display waiting message for input
    itemWaitFieldsIn = new TreeItem( itemInput, SWT.NULL );
    itemWaitFieldsIn.setText( BaseMessages.getString( PKG, "UserDefinedJavaClassDialog.GettingFields.Label" ) );
    itemWaitFieldsIn.setForeground( guiResource.getColorDirectory() );
    itemInput.setExpanded( true );

    // Display waiting message for info
    itemWaitFieldsInfo = new TreeItem( itemInfo, SWT.NULL );
    itemWaitFieldsInfo.setText( BaseMessages.getString( PKG, "UserDefinedJavaClassDialog.GettingFields.Label" ) );
    itemWaitFieldsInfo.setForeground( guiResource.getColorDirectory() );
    itemInfo.setExpanded( true );

    // Display waiting message for output
    itemWaitFieldsOut = new TreeItem( itemOutput, SWT.NULL );
    itemWaitFieldsOut.setText( BaseMessages.getString( PKG, "UserDefinedJavaClassDialog.GettingFields.Label" ) );
    itemWaitFieldsOut.setForeground( guiResource.getColorDirectory() );
    itemOutput.setExpanded( true );

    //
    // Search the fields in the background
    //

    final Runnable runnable = new Runnable() {
      public void run() {
        TransformMeta transformMeta = pipelineMeta.findTransform( transformName );
        if ( transformMeta != null ) {
          try {
            inputRowMeta = pipelineMeta.getPrevTransformFields( variables, transformMeta );
            infoRowMeta = pipelineMeta.getPrevInfoFields( variables,transformMeta );
            outputRowMeta = pipelineMeta.getThisTransformFields( variables,transformMeta, null, inputRowMeta.clone() );
            populateFieldsTree();
          } catch ( HopException e ) {
            log.logError( BaseMessages.getString( PKG, "System.Dialog.GetFieldsFailed.Message" ), e );
          }
        }
      }
    };
    new Thread( runnable ).start();

    addRenameToTreeScriptItems();
    input.setChanged( changed );

    // Create the drag source on the tree
    DragSource ds = new DragSource( wTree, DND.DROP_MOVE );
    ds.setTransfer( new Transfer[] { TextTransfer.getInstance() } );
    ds.addDragListener( new DragSourceAdapter() {

      public void dragStart( DragSourceEvent event ) {
        boolean doit = false;
        TreeItem item = wTree.getSelection()[ 0 ];

        // Allow dragging snippits and field helpers
        if ( item != null && item.getParentItem() != null ) {
          if ( "Snippits Category".equals( item.getParentItem().getData() )
            && !"Snippits Category".equals( item.getData() ) ) {
            doit = true;
          } else if ( "Field Helpers".equals( item.getParentItem().getData() ) ) {
            doit = true;
          } else if ( item.getParentItem().getParentItem() != null
            && "Field Helpers".equals( item.getParentItem().getParentItem().getData() ) ) {
            doit = true;
          }
        }
        event.doit = doit;
      }

      public void dragSetData( DragSourceEvent event ) {
        // Set the data to be the first selected item's data
        event.data = wTree.getSelection()[ 0 ].getData();
      }
    } );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return transformName;
  }

  protected boolean createPlugin() {

    // Create a transform with the information in this dialog
    UserDefinedJavaClassMeta udjcMeta = new UserDefinedJavaClassMeta();
    getInfo( udjcMeta );

    try {
      String pluginName = "Processor";
      for ( UserDefinedJavaClassDef def : udjcMeta.getDefinitions() ) {
        if ( def.isTransformClass() ) {
          pluginName = def.getClassName();
        }
      }
      File pluginFile = new File( String.format( "plugins/transforms/%s/%s.transform.xml", pluginName, pluginName ) );
      pluginFile.getParentFile().mkdirs();
      PrintWriter pw = new PrintWriter( new FileWriter( pluginFile ) );
      StringBuilder outXML = new StringBuilder( "<transform>\n" );
      outXML.append( String.format( "\t<name>%s</name>\n", transformName ) );
      outXML.append( "\t<type>UserDefinedJavaClass</type>\n" );
      outXML.append( "\t<description/>\n\t" );
      outXML.append( udjcMeta.getXml() );
      outXML.append( "</transform>" );
      pw.println( outXML.toString() );
      pw.flush();
      pw.close();
      ShowMessageDialog msgDialog = new ShowMessageDialog( shell, SWT.ICON_INFORMATION | SWT.OK,
        BaseMessages.getString( PKG, "UserDefinedJavaClassDialog.Plugin.CreateSuccess" ),
        BaseMessages.getString( PKG, "UserDefinedJavaClassDialog.Plugin.CreatedFile", pluginFile.getPath() ), false );
      msgDialog.open();

    } catch ( IOException e ) {
      e.printStackTrace();
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "UserDefinedJavaClassDialog.Plugin.CreateErrorTitle" ), BaseMessages
        .getString( PKG, "UserDefinedJavaClassDialog.Plugin.CreateErrorMessage", transformName ), e );
    }

    return true;
  }

  private void addFieldsTab() {
    fieldsTab = new CTabItem( wTabFolder, SWT.NONE );
    fieldsTab.setText( BaseMessages.getString( PKG, "UserDefinedJavaClassDialog.Tabs.Fields.Title" ) );
    fieldsTab.setToolTipText( BaseMessages.getString( PKG, "UserDefinedJavaClassDialog.Tabs.Fields.TooltipText" ) );

    Composite wBottom = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wBottom );
    fieldsTab.setControl( wBottom );
    FormLayout bottomLayout = new FormLayout();
    bottomLayout.marginWidth = Const.FORM_MARGIN;
    bottomLayout.marginHeight = Const.FORM_MARGIN;
    wBottom.setLayout( bottomLayout );

    Label wlFields = new Label( wBottom, SWT.NONE );
    wlFields.setText( BaseMessages.getString( PKG, "UserDefinedJavaClassDialog.Fields.Label" ) );
    props.setLook( wlFields );
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment( 0, 0 );
    fdlFields.top = new FormAttachment( 0, 0 );
    wlFields.setLayoutData( fdlFields );

    wClearResultFields = new Button( wBottom, SWT.CHECK );
    wClearResultFields
      .setText( BaseMessages.getString( PKG, "UserDefinedJavaClassDialog.ClearResultFields.Label" ) );
    props.setLook( wClearResultFields );
    FormData fdClearResultFields = new FormData();
    fdClearResultFields.right = new FormAttachment( 100, 0 );
    fdClearResultFields.top = new FormAttachment( 0, 0 );
    wClearResultFields.setLayoutData( fdClearResultFields );

    final int fieldsRows = input.getFieldInfo().size();

    ColumnInfo[] colinf =
      new ColumnInfo[] {
        new ColumnInfo(
          BaseMessages.getString( PKG, "UserDefinedJavaClassDialog.ColumnInfo.Filename" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "UserDefinedJavaClassDialog.ColumnInfo.Type" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, ValueMetaFactory.getValueMetaNames() ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "UserDefinedJavaClassDialog.ColumnInfo.Length" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "UserDefinedJavaClassDialog.ColumnInfo.Precision" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ), };

    wFields =
      new TableView(
        variables, wBottom, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, fieldsRows, lsMod, props );

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment( wlFields, margin );
    fdFields.right = new FormAttachment( 100, 0 );
    fdFields.bottom = new FormAttachment( 100, 0 );
    wFields.setLayoutData( fdFields );

    FormData fdBottom = new FormData();
    fdBottom.left = new FormAttachment( 0, 0 );
    fdBottom.top = new FormAttachment( 0, 0 );
    fdBottom.right = new FormAttachment( 100, 0 );
    fdBottom.bottom = new FormAttachment( 100, 0 );
    wBottom.setLayoutData( fdBottom );

  }

  private void addInfoTab() {
    infoTab = new CTabItem( wTabFolder, SWT.NONE );
    infoTab.setText( BaseMessages.getString( PKG, "UserDefinedJavaClassDialog.Tabs.Info.Title" ) );
    infoTab.setToolTipText( BaseMessages.getString( PKG, "UserDefinedJavaClassDialog.Tabs.Info.TooltipText" ) );

    Composite wBottom = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wBottom );
    infoTab.setControl( wBottom );
    FormLayout bottomLayout = new FormLayout();
    bottomLayout.marginWidth = Const.FORM_MARGIN;
    bottomLayout.marginHeight = Const.FORM_MARGIN;
    wBottom.setLayout( bottomLayout );

    Label wlFields = new Label( wBottom, SWT.NONE );
    wlFields.setText( BaseMessages.getString( PKG, "UserDefinedJavaClassDialog.InfoTransforms.Label" ) );
    props.setLook( wlFields );
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment( 0, 0 );
    fdlFields.top = new FormAttachment( 0, 0 );
    wlFields.setLayoutData( fdlFields );

    final int nrRows = input.getInfoTransformDefinitions().size();
    ColumnInfo[] colinf =
      new ColumnInfo[] {
        new ColumnInfo(
          BaseMessages.getString( PKG, "UserDefinedJavaClassDialog.ColumnInfo.TransformTag" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "UserDefinedJavaClassDialog.ColumnInfo.TransformName" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, prevTransformNames ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "UserDefinedJavaClassDialog.ColumnInfo.TransformDescription" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ), };

    wInfoTransforms =
      new TableView(
        variables, wBottom, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, nrRows, lsMod, props );

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment( wlFields, margin );
    fdFields.right = new FormAttachment( 100, 0 );
    fdFields.bottom = new FormAttachment( 100, 0 );
    wInfoTransforms.setLayoutData( fdFields );

    FormData fdBottom = new FormData();
    fdBottom.left = new FormAttachment( 0, 0 );
    fdBottom.top = new FormAttachment( 0, 0 );
    fdBottom.right = new FormAttachment( 100, 0 );
    fdBottom.bottom = new FormAttachment( 100, 0 );
    wBottom.setLayoutData( fdBottom );
  }

  private void addTargetTab() {
    targetTab = new CTabItem( wTabFolder, SWT.NONE );
    targetTab.setText( BaseMessages.getString( PKG, "UserDefinedJavaClassDialog.Tabs.Target.Title" ) );
    targetTab.setToolTipText( BaseMessages.getString( PKG, "UserDefinedJavaClassDialog.Tabs.Target.TooltipText" ) );

    Composite wBottom = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wBottom );
    targetTab.setControl( wBottom );
    FormLayout bottomLayout = new FormLayout();
    bottomLayout.marginWidth = Const.FORM_MARGIN;
    bottomLayout.marginHeight = Const.FORM_MARGIN;
    wBottom.setLayout( bottomLayout );

    Label wlFields = new Label( wBottom, SWT.NONE );
    wlFields.setText( BaseMessages.getString( PKG, "UserDefinedJavaClassDialog.TargetTransforms.Label" ) );
    props.setLook( wlFields );
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment( 0, 0 );
    fdlFields.top = new FormAttachment( 0, 0 );
    wlFields.setLayoutData( fdlFields );

    final int nrRows = input.getTargetTransformDefinitions().size();
    ColumnInfo[] colinf =
      new ColumnInfo[] {
        new ColumnInfo(
          BaseMessages.getString( PKG, "UserDefinedJavaClassDialog.ColumnInfo.TransformTag" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "UserDefinedJavaClassDialog.ColumnInfo.TransformName" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, nextTransformNames ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "UserDefinedJavaClassDialog.ColumnInfo.TransformDescription" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ), };

    wTargetTransforms =
      new TableView(
        variables, wBottom, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, nrRows, lsMod, props );

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment( wlFields, margin );
    fdFields.right = new FormAttachment( 100, 0 );
    fdFields.bottom = new FormAttachment( 100, 0 );
    wTargetTransforms.setLayoutData( fdFields );

    FormData fdBottom = new FormData();
    fdBottom.left = new FormAttachment( 0, 0 );
    fdBottom.top = new FormAttachment( 0, 0 );
    fdBottom.right = new FormAttachment( 100, 0 );
    fdBottom.bottom = new FormAttachment( 100, 0 );
    wBottom.setLayoutData( fdBottom );
  }

  private void addParametersTab() {
    parametersTab = new CTabItem( wTabFolder, SWT.NONE );
    parametersTab.setText( BaseMessages.getString( PKG, "UserDefinedJavaClassDialog.Tabs.Parameters.Title" ) );
    parametersTab.setToolTipText( BaseMessages.getString(
      PKG, "UserDefinedJavaClassDialog.Tabs.Parameters.TooltipText" ) );

    Composite wBottom = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wBottom );
    parametersTab.setControl( wBottom );
    FormLayout bottomLayout = new FormLayout();
    bottomLayout.marginWidth = Const.FORM_MARGIN;
    bottomLayout.marginHeight = Const.FORM_MARGIN;
    wBottom.setLayout( bottomLayout );

    Label wlFields = new Label( wBottom, SWT.NONE );
    wlFields.setText( BaseMessages.getString( PKG, "UserDefinedJavaClassDialog.Parameters.Label" ) );
    props.setLook( wlFields );
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment( 0, 0 );
    fdlFields.top = new FormAttachment( 0, 0 );
    wlFields.setLayoutData( fdlFields );

    final int nrRows = input.getUsageParameters().size();
    ColumnInfo[] colinf =
      new ColumnInfo[] {
        new ColumnInfo(
          BaseMessages.getString( PKG, "UserDefinedJavaClassDialog.ColumnInfo.ParameterTag" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "UserDefinedJavaClassDialog.ColumnInfo.ParameterValue" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "UserDefinedJavaClassDialog.ColumnInfo.ParameterDescription" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ), };
    colinf[ 1 ].setUsingVariables( true );

    wParameters =
      new TableView(
        variables, wBottom, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, nrRows, lsMod, props );

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment( wlFields, margin );
    fdFields.right = new FormAttachment( 100, 0 );
    fdFields.bottom = new FormAttachment( 100, 0 );
    wParameters.setLayoutData( fdFields );

    FormData fdBottom = new FormData();
    fdBottom.left = new FormAttachment( 0, 0 );
    fdBottom.top = new FormAttachment( 0, 0 );
    fdBottom.right = new FormAttachment( 100, 0 );
    fdBottom.bottom = new FormAttachment( 100, 0 );
    wBottom.setLayoutData( fdBottom );
  }

  private void setActiveCtab( String strName ) {
    if ( strName.length() == 0 ) {
      folder.setSelection( 0 );
    } else {
      folder.setSelection( getCTabPosition( strName ) );
    }
  }

  private void addCtab( String tabName, String tabCode, TabAddActions tabType ) {
    CTabItem item = new CTabItem( folder, SWT.CLOSE );

    switch ( tabType ) {
      case ADD_DEFAULT:
        item.setText( tabName );
        break;
      default:
        item.setText( getNextName( tabName ) );
        break;
    }
    StyledTextComp wScript =
      new StyledTextComp( variables, item.getParent(), SWT.MULTI | SWT.LEFT | SWT.H_SCROLL | SWT.V_SCROLL, item
        .getText(), false );
    if ( ( tabCode != null ) && tabCode.length() > 0 ) {
      wScript.setText( tabCode );
    } else {
      wScript.setText( snippitsHelper.getDefaultCode() );
    }
    item.setImage( imageInactiveScript );
    props.setLook( wScript, Props.WIDGET_STYLE_FIXED );

    wScript.addKeyListener( new KeyAdapter() {
      public void keyPressed( KeyEvent e ) {
        setPosition();
      }

      public void keyReleased( KeyEvent e ) {
        setPosition();
      }
    } );
    wScript.addFocusListener( new FocusAdapter() {
      public void focusGained( FocusEvent e ) {
        setPosition();
      }

      public void focusLost( FocusEvent e ) {
        setPosition();
      }
    } );
    wScript.addMouseListener( new MouseAdapter() {
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

    wScript.addModifyListener( lsMod );

    // Text Higlighting
    wScript.addLineStyleListener( new UserDefinedJavaClassHighlight() );
    item.setControl( wScript );

    // Adding new Item to Tree
    modifyTabTree( item, TabActions.ADD_ITEM );
  }

  private void modifyTabTree( CTabItem ctabitem, TabActions action ) {

    switch ( action ) {
      case DELETE_ITEM:
        TreeItem dItem = getTreeItemByName( ctabitem.getText() );
        if ( dItem != null ) {
          dItem.dispose();
          input.setChanged();
        }
        break;
      case ADD_ITEM:
        TreeItem item = new TreeItem( wTreeClassesItem, SWT.NULL );
        item.setImage( imageActiveScript );
        item.setText( ctabitem.getText() );
        input.setChanged();
        break;

      case RENAME_ITEM:
        input.setChanged();
        break;
      case SET_ACTIVE_ITEM:
        input.setChanged();
        break;
      default:
        break;
    }
  }

  private TreeItem getTreeItemByName( String strTabName ) {
    TreeItem[] tItems = wTreeClassesItem.getItems();
    for ( int i = 0; i < tItems.length; i++ ) {
      if ( tItems[ i ].getText().equals( strTabName ) ) {
        return tItems[ i ];
      }
    }
    return null;
  }

  private int getCTabPosition( String strTabName ) {
    CTabItem[] cItems = folder.getItems();
    for ( int i = 0; i < cItems.length; i++ ) {
      if ( cItems[ i ].getText().equals( strTabName ) ) {
        return i;
      }
    }
    return -1;
  }

  private CTabItem getCTabItemByName( String strTabName ) {
    CTabItem[] cItems = folder.getItems();
    for ( int i = 0; i < cItems.length; i++ ) {
      if ( cItems[ i ].getText().equals( strTabName ) ) {
        return cItems[ i ];
      }
    }
    return null;
  }

  private void modifyCTabItem( TreeItem tItem, TabActions iModType, String strOption ) {

    switch ( iModType ) {
      case DELETE_ITEM:
        CTabItem dItem = folder.getItem( getCTabPosition( tItem.getText() ) );
        if ( dItem != null ) {
          dItem.dispose();
          input.setChanged();
        }
        break;

      case RENAME_ITEM:
        CTabItem rItem = folder.getItem( getCTabPosition( tItem.getText() ) );
        if ( rItem != null ) {
          rItem.setText( strOption );
          input.setChanged();
          if ( rItem.getImage().equals( imageActiveScript ) ) {
            strActiveScript = strOption;
          }
        }
        break;
      case SET_ACTIVE_ITEM:
        CTabItem aItem = folder.getItem( getCTabPosition( tItem.getText() ) );
        if ( aItem != null ) {
          input.setChanged();
          strActiveScript = tItem.getText();
          for ( int i = 0; i < folder.getItemCount(); i++ ) {
            if ( folder.getItem( i ).equals( aItem ) ) {
              aItem.setImage( imageActiveScript );
            } else {
              folder.getItem( i ).setImage( imageInactiveScript );
            }
          }
        }
        break;
      default:
        break;
    }

  }

  private StyledTextComp getStyledTextComp() {
    CTabItem item = folder.getSelection();
    if ( item.getControl().isDisposed() ) {
      return null;
    } else {
      return (StyledTextComp) item.getControl();
    }
  }

  private StyledTextComp getStyledTextComp( CTabItem item ) {
    return (StyledTextComp) item.getControl();
  }

  private String getNextName( String strActualName ) {
    String strRC = "";
    if ( strActualName.length() == 0 ) {
      strActualName = "ExtraClass";
    }

    int i = 0;
    strRC = strActualName + "_" + i;
    while ( getCTabItemByName( strRC ) != null ) {
      i++;
      strRC = strActualName + "_" + i;
    }
    return strRC;
  }

  public void setPosition() {

    StyledTextComp wScript = getStyledTextComp();
    String scr = wScript.getText();
    int linenr = wScript.getLineAtOffset( wScript.getCaretOffset() ) + 1;
    int posnr = wScript.getCaretOffset();

    // Go back from position to last CR: how many positions?
    int colnr = 0;
    while ( posnr > 0 && scr.charAt( posnr - 1 ) != '\n' && scr.charAt( posnr - 1 ) != '\r' ) {
      posnr--;
      colnr++;
    }
    wlPosition.setText( BaseMessages.getString( PKG, "UserDefinedJavaClassDialog.Position.Label2" )
      + linenr + ", " + colnr );
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    int i = 0;
    for ( FieldInfo fi : input.getFieldInfo() ) {
      TableItem item = wFields.table.getItem( i );
      i++;
      item.setText( 1, fi.name );
      item.setText( 2, ValueMetaFactory.getValueMetaName( fi.type ) );
      if ( fi.length >= 0 ) {
        item.setText( 3, "" + fi.length );
      }
      if ( fi.precision >= 0 ) {
        item.setText( 4, "" + fi.precision );
      }
    }

    List<UserDefinedJavaClassDef> definitions = input.getDefinitions();
    if ( definitions.size() == 0 ) {
      try {
        definitions = new ArrayList<UserDefinedJavaClassDef>();
        // Note the tab name isn't i18n because it is a Java Class name and i18n characters might make it choke.
        definitions.add( new UserDefinedJavaClassDef(
          ClassType.TRANSFORM_CLASS, "Processor", UserDefinedJavaClassCodeSnippits
          .getSnippitsHelper().getDefaultCode() ) );
        input.replaceDefinitions( definitions );
      } catch ( HopXmlException e ) {
        e.printStackTrace();
        new ErrorDialog(
          shell, BaseMessages.getString( PKG, "UserDefinedJavaClassDialog.Plugin.CreateErrorTitle" ),
          BaseMessages.getString( PKG, "UserDefinedJavaClassDialog.Plugin.CreateErrorMessage", transformName ), e );
      }
    }
    for ( UserDefinedJavaClassDef def : definitions ) {
      if ( def.isTransformClass() ) {
        strActiveScript = def.getClassName();
      }
      addCtab( def.getClassName(), def.getSource(), TabAddActions.ADD_DEFAULT );
    }

    setActiveCtab( strActiveScript );
    refresh();

    wClearResultFields.setSelection( input.isClearingResultFields() );

    wFields.setRowNums();
    wFields.optWidth( true );

    int rowNr = 0;
    for ( InfoTransformDefinition transformDefinition : input.getInfoTransformDefinitions() ) {
      TableItem item = wInfoTransforms.table.getItem( rowNr++ );
      int colNr = 1;
      item.setText( colNr++, Const.NVL( transformDefinition.tag, "" ) );
      item.setText( colNr++, transformDefinition.transformMeta != null ? transformDefinition.transformMeta.getName() : "" );
      item.setText( colNr++, Const.NVL( transformDefinition.description, "" ) );
    }
    wInfoTransforms.setRowNums();
    wInfoTransforms.optWidth( true );

    rowNr = 0;
    for ( TargetTransformDefinition transformDefinition : input.getTargetTransformDefinitions() ) {
      TableItem item = wTargetTransforms.table.getItem( rowNr++ );
      int colNr = 1;
      item.setText( colNr++, Const.NVL( transformDefinition.tag, "" ) );
      item.setText( colNr++, transformDefinition.transformMeta != null ? transformDefinition.transformMeta.getName() : "" );
      item.setText( colNr++, Const.NVL( transformDefinition.description, "" ) );
    }
    wTargetTransforms.setRowNums();
    wTargetTransforms.optWidth( true );

    rowNr = 0;
    for ( UsageParameter usageParameter : input.getUsageParameters() ) {
      TableItem item = wParameters.table.getItem( rowNr++ );
      int colNr = 1;
      item.setText( colNr++, Const.NVL( usageParameter.tag, "" ) );
      item.setText( colNr++, Const.NVL( usageParameter.value, "" ) );
      item.setText( colNr++, Const.NVL( usageParameter.description, "" ) );
    }
    wParameters.setRowNums();
    wParameters.optWidth( true );

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void refresh() {
    for ( int i = 0; i < folder.getItemCount(); i++ ) {
      CTabItem item = folder.getItem( i );
      if ( item.getText().equals( strActiveScript ) ) {
        item.setImage( imageActiveScript );
      } else {
        item.setImage( imageInactiveScript );
      }
    }
  }

  private boolean cancel() {
    if ( input.hasChanged() ) {
      MessageBox box = new MessageBox( shell, SWT.YES | SWT.NO | SWT.APPLICATION_MODAL );
      box.setText( BaseMessages.getString( PKG, "UserDefinedJavaClassDialog.WarningDialogChanged.Title" ) );
      box.setMessage( BaseMessages.getString(
        PKG, "UserDefinedJavaClassDialog.WarningDialogChanged.Message", Const.CR ) );
      int answer = box.open();

      if ( answer == SWT.NO ) {
        return false;
      }
    }
    transformName = null;
    input.setChanged( changed );
    dispose();
    return true;
  }

  private void getInfo( UserDefinedJavaClassMeta meta ) {
    int nrFields = wFields.nrNonEmpty();
    List<FieldInfo> newFields = new ArrayList<FieldInfo>( nrFields );
    for ( int i = 0; i < nrFields; i++ ) {
      TableItem item = wFields.getNonEmpty( i );
      newFields.add( new FieldInfo( item.getText( 1 ), ValueMetaFactory.getIdForValueMeta( item.getText( 2 ) ),
        Const.toInt( item.getText( 3 ), -1 ), Const.toInt( item.getText( 4 ), -1 ) ) );
    }
    meta.replaceFields( newFields );

    CTabItem[] cTabs = folder.getItems();
    if ( cTabs.length > 0 ) {
      List<UserDefinedJavaClassDef> definitions = new ArrayList<UserDefinedJavaClassDef>( cTabs.length );
      for ( int i = 0; i < cTabs.length; i++ ) {
        UserDefinedJavaClassDef def =
          new UserDefinedJavaClassDef( ClassType.NORMAL_CLASS, cTabs[ i ].getText(), getStyledTextComp( cTabs[ i ] )
            .getText() );
        if ( cTabs[ i ].getImage().equals( imageActiveScript ) ) {
          def.setClassType( ClassType.TRANSFORM_CLASS );
        }
        definitions.add( def );
      }
      meta.replaceDefinitions( definitions );
    }
    meta.setClearingResultFields( wClearResultFields.getSelection() );

    int nrInfos = wInfoTransforms.nrNonEmpty();
    meta.getInfoTransformDefinitions().clear();
    for ( int i = 0; i < nrInfos; i++ ) {
      TableItem item = wInfoTransforms.getNonEmpty( i );
      InfoTransformDefinition transformDefinition = new InfoTransformDefinition();
      int colNr = 1;
      transformDefinition.tag = item.getText( colNr++ );
      transformDefinition.transformMeta = pipelineMeta.findTransform( item.getText( colNr++ ) );
      transformDefinition.description = item.getText( colNr++ );
      meta.getInfoTransformDefinitions().add( transformDefinition );
    }

    int nrTargets = wTargetTransforms.nrNonEmpty();
    meta.getTargetTransformDefinitions().clear();
    for ( int i = 0; i < nrTargets; i++ ) {
      TableItem item = wTargetTransforms.getNonEmpty( i );
      TargetTransformDefinition transformDefinition = new TargetTransformDefinition();
      int colNr = 1;
      transformDefinition.tag = item.getText( colNr++ );
      transformDefinition.transformMeta = pipelineMeta.findTransform( item.getText( colNr++ ) );
      transformDefinition.description = item.getText( colNr++ );
      meta.getTargetTransformDefinitions().add( transformDefinition );
    }

    int nrParameters = wParameters.nrNonEmpty();
    meta.getUsageParameters().clear();
    for ( int i = 0; i < nrParameters; i++ ) {
      TableItem item = wParameters.getNonEmpty( i );
      UsageParameter usageParameter = new UsageParameter();
      int colNr = 1;
      usageParameter.tag = item.getText( colNr++ );
      usageParameter.value = item.getText( colNr++ );
      usageParameter.description = item.getText( colNr++ );
      meta.getUsageParameters().add( usageParameter );
    }
  }

  private void ok() {
    if ( Utils.isEmpty( wTransformName.getText() ) ) {
      return;
    }

    transformName = wTransformName.getText(); // return value

    boolean bInputOK = false;

    bInputOK = checkForTransformClass();

    if ( bInputOK ) {
      getInfo( input );
      dispose();
    }
  }

  private boolean checkForTransformClass() {
    boolean hasTransformClass = true;
    // Check if Active Script has set, otherwise Ask
    if ( getCTabItemByName( strActiveScript ) == null ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.CANCEL | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString( PKG, "UserDefinedJavaClassDialog.NoTransformClassSet" ) );
      mb.setText( BaseMessages.getString( PKG, "UserDefinedJavaClassDialog.ERROR.Label" ) );
      switch ( mb.open() ) {
        case SWT.OK:
          strActiveScript = folder.getItem( 0 ).getText();
          refresh();
          hasTransformClass = true;
          break;
        case SWT.CANCEL:
          hasTransformClass = false;
          break;
        default:
          break;
      }
    }
    return hasTransformClass;
  }

  private boolean test() {
    PluginRegistry registry = PluginRegistry.getInstance();
    String scriptTransformName = wTransformName.getText();

    if ( !checkForTransformClass() ) {
      return false;
    }

    // Create a transform with the information in this dialog
    UserDefinedJavaClassMeta udjcMeta = new UserDefinedJavaClassMeta();
    getInfo( udjcMeta );

    try {
      // First, before we get into the trial run, just see if the classes
      // all compile.
      udjcMeta.cookClasses();
      if ( udjcMeta.cookErrors.size() == 1 ) {
        Exception e = udjcMeta.cookErrors.get( 0 );
        new ErrorDialog( shell, "Error during class compilation", e.toString(), e );
        return false;
      } else if ( udjcMeta.cookErrors.size() > 1 ) {
        Exception e = udjcMeta.cookErrors.get( 0 );
        new ErrorDialog( shell, "Errors during class compilation", String.format(
          "Multiple errors during class compilation. First error:\n%s", e.toString() ), e );
        return false;
      }

      // What fields are coming into the transform?
      IRowMeta rowMeta = pipelineMeta.getPrevTransformFields( variables, transformName ).clone();
      if ( rowMeta != null ) {
        // Create a new RowGenerator transform to generate rows for the test
        // data...
        // Only create a new instance the first time to help the user.
        // Otherwise he/she has to key in the same test data all the
        // time
        if ( genMeta == null ) {
          genMeta = new RowGeneratorMeta();
          genMeta.setRowLimit( "10" );
          genMeta.allocate( rowMeta.size() );
          //CHECKSTYLE:Indentation:OFF
          for ( int i = 0; i < rowMeta.size(); i++ ) {
            IValueMeta valueMeta = rowMeta.getValueMeta( i );
            if ( valueMeta.isStorageBinaryString() ) {
              valueMeta.setStorageType( IValueMeta.STORAGE_TYPE_NORMAL );
            }
            genMeta.getFieldName()[ i ] = valueMeta.getName();
            genMeta.getFieldType()[ i ] = valueMeta.getTypeDesc();
            genMeta.getFieldLength()[ i ] = valueMeta.getLength();
            genMeta.getFieldPrecision()[ i ] = valueMeta.getPrecision();
            genMeta.getCurrency()[ i ] = valueMeta.getCurrencySymbol();
            genMeta.getDecimal()[ i ] = valueMeta.getDecimalSymbol();
            genMeta.getGroup()[ i ] = valueMeta.getGroupingSymbol();

            String string = null;
            switch ( valueMeta.getType() ) {
              case IValueMeta.TYPE_DATE:
                genMeta.getFieldFormat()[ i ] = "yyyy/MM/dd HH:mm:ss";
                valueMeta.setConversionMask( genMeta.getFieldFormat()[ i ] );
                string = valueMeta.getString( new Date() );
                break;
              case IValueMeta.TYPE_STRING:
                string = "test value test value";
                break;
              case IValueMeta.TYPE_INTEGER:
                genMeta.getFieldFormat()[ i ] = "#";
                valueMeta.setConversionMask( genMeta.getFieldFormat()[ i ] );
                string = valueMeta.getString( Long.valueOf( 0L ) );
                break;
              case IValueMeta.TYPE_NUMBER:
                genMeta.getFieldFormat()[ i ] = "#.#";
                valueMeta.setConversionMask( genMeta.getFieldFormat()[ i ] );
                string = valueMeta.getString( Double.valueOf( 0.0D ) );
                break;
              case IValueMeta.TYPE_BIGNUMBER:
                genMeta.getFieldFormat()[ i ] = "#.#";
                valueMeta.setConversionMask( genMeta.getFieldFormat()[ i ] );
                string = valueMeta.getString( BigDecimal.ZERO );
                break;
              case IValueMeta.TYPE_BOOLEAN:
                string = valueMeta.getString( Boolean.TRUE );
                break;
              case IValueMeta.TYPE_BINARY:
                string = valueMeta.getString( new byte[] { 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, } );
                break;
              default:
                break;
            }

            genMeta.getValue()[ i ] = string;
          }
        }
        TransformMeta genTransform =
          new TransformMeta( registry.getPluginId( TransformPluginType.class, genMeta ), "## TEST DATA ##", genMeta );
        genTransform.setLocation( 50, 50 );

        TransformMeta scriptTransform =
          new TransformMeta( registry.getPluginId( TransformPluginType.class, udjcMeta ), Const.NVL(
            scriptTransformName, "## SCRIPT ##" ), udjcMeta );
        scriptTransformName = scriptTransform.getName();
        scriptTransform.setLocation( 150, 50 );

        // Create a hop between both transforms...
        //
        PipelineHopMeta hop = new PipelineHopMeta( genTransform, scriptTransform );

        // Generate a new test pipeline...
        //
        PipelineMeta pipelineMeta = new PipelineMeta();
        pipelineMeta.setName( wTransformName.getText() + " - PREVIEW" );
        pipelineMeta.addTransform( genTransform );
        pipelineMeta.addTransform( scriptTransform );
        pipelineMeta.addPipelineHop( hop );

        // OK, now we ask the user to edit this dialog...
        //
          // Now run this pipeline and grab the results...
          //
          PipelinePreviewProgressDialog progressDialog =
            new PipelinePreviewProgressDialog(
              shell, variables, pipelineMeta, new String[] { scriptTransformName, }, new int[] { Const.toInt( genMeta
              .getRowLimit(), 10 ), } );
          progressDialog.open();

          Pipeline pipeline = progressDialog.getPipeline();
          String loggingText = progressDialog.getLoggingText();

          if ( !progressDialog.isCancelled() ) {
            if ( pipeline.getResult() != null && pipeline.getResult().getNrErrors() > 0 ) {
              EnterTextDialog etd =
                new EnterTextDialog(
                  shell, BaseMessages.getString( "System.Dialog.PreviewError.Title" ), BaseMessages
                  .getString( "System.Dialog.PreviewError.Message" ), loggingText, true );
              etd.setReadOnly();
              etd.open();
            }
          }

          IRowMeta previewRowsMeta = progressDialog.getPreviewRowsMeta( wTransformName.getText() );
          List<Object[]> previewRows = progressDialog.getPreviewRows( wTransformName.getText() );

          if ( previewRowsMeta != null && previewRows != null && previewRows.size() > 0 ) {
            PreviewRowsDialog prd =
              new PreviewRowsDialog(
                shell, variables, SWT.NONE, wTransformName.getText(), previewRowsMeta, previewRows, loggingText );
            prd.open();
          }


        return true;
      } else {
        throw new HopException( BaseMessages.getString(
          PKG, "UserDefinedJavaClassDialog.Exception.CouldNotGetFields" ) );
      }
    } catch ( Exception e ) {
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "UserDefinedJavaClassDialog.TestFailed.DialogTitle" ), BaseMessages
        .getString( PKG, "UserDefinedJavaClassDialog.TestFailed.DialogMessage" ), e );
      return false;
    }

  }

  private void buildSnippitsTree() {

    TreeItem item = new TreeItem( wTree, SWT.NULL );
    item.setImage( guiResource.getImageFolder() );
    item.setText( BaseMessages.getString( PKG, "UserDefinedJavaClassDialog.Snippits.Label" ) );

    Map<Category, TreeItem> categoryTreeItems = new EnumMap<Category, TreeItem>( Category.class );
    for ( Category cat : Category.values() ) {
      TreeItem itemGroup = new TreeItem( item, SWT.NULL );
      itemGroup.setImage( guiResource.getImageFolder() );
      itemGroup.setText( cat.getDescription() );
      itemGroup.setData( "Snippits Category" );
      categoryTreeItems.put( cat, itemGroup );
    }

    Collection<Snippit> snippits = snippitsHelper.getSnippits();
    for ( Snippit snippit : snippits ) {
      TreeItem itemGroup = categoryTreeItems.get( snippit.category );
      TreeItem itemSnippit = new TreeItem( itemGroup, SWT.NULL );
      itemSnippit.setText( snippit.name );
      itemSnippit.setImage( GuiResource.getInstance().getImageLabel() );
      itemSnippit.setData( snippit.code );
    }
  }

  public boolean TreeItemExist( TreeItem itemToCheck, String strItemName ) {
    boolean bRC = false;
    if ( itemToCheck.getItemCount() > 0 ) {
      TreeItem[] items = itemToCheck.getItems();
      for ( int i = 0; i < items.length; i++ ) {
        if ( items[ i ].getText().equals( strItemName ) ) {
          return true;
        }
      }
    }
    return bRC;
  }

  private void populateFieldsTree() {
    shell.getDisplay().syncExec( new Runnable() {
      public void run() {
        itemInput.removeAll();
        itemInfo.removeAll();
        itemOutput.removeAll();

        if ( inputRowMeta != null ) {
          for ( int i = 0; i < inputRowMeta.size(); i++ ) {
            IValueMeta v = inputRowMeta.getValueMeta( i );
            String itemName = v.getName();
            String itemData = FieldHelper.getAccessor( true, itemName );
            TreeItem itemField = new TreeItem( itemInput, SWT.NULL );
            itemField.setImage( guiResource.getImage(v) );
            itemField.setText( itemName );
            itemField.setData( itemData );
            TreeItem itemFieldGet = new TreeItem( itemField, SWT.NULL );
            itemFieldGet.setText( String.format( "get%s()", FieldHelper.getNativeDataTypeSimpleName( v ) ) );
            itemFieldGet.setData( FieldHelper.getGetSignature( itemData, v ) );
            TreeItem itemFieldSet = new TreeItem( itemField, SWT.NULL );
            itemFieldSet.setText( "setValue()" );
            itemFieldSet.setData( itemData + ".setValue(r, value);" );
          }
        }
        if ( infoRowMeta != null ) {
          for ( int i = 0; i < infoRowMeta.size(); i++ ) {
            IValueMeta v = infoRowMeta.getValueMeta( i );
            String itemName = v.getName();
            String itemData = FieldHelper.getAccessor( true, itemName );
            TreeItem itemField = new TreeItem( itemInfo, SWT.NULL );
            itemField.setImage( guiResource.getImage(v) );
            itemField.setText( itemName );
            itemField.setData( itemData );
            TreeItem itemFieldGet = new TreeItem( itemField, SWT.NULL );
            itemFieldGet.setText( String.format( "get%s()", FieldHelper.getNativeDataTypeSimpleName( v ) ) );
            itemFieldGet.setData( FieldHelper.getGetSignature( itemData, v ) );
            TreeItem itemFieldSet = new TreeItem( itemField, SWT.NULL );
            itemFieldSet.setText( "setValue()" );
            itemFieldSet.setData( itemData + ".setValue(r, value);" );
          }
        }
        if ( outputRowMeta != null ) {
          for ( int i = 0; i < outputRowMeta.size(); i++ ) {
            IValueMeta v = outputRowMeta.getValueMeta( i );
            String itemName = v.getName();
            String itemData = FieldHelper.getAccessor( false, itemName );
            TreeItem itemField = new TreeItem( itemOutput, SWT.NULL );
            itemField.setImage( guiResource.getImage(v) );
            itemField.setText( itemName );
            itemField.setData( itemData );
            TreeItem itemFieldGet = new TreeItem( itemField, SWT.NULL );
            itemFieldGet.setText( String.format( "get%s()", FieldHelper.getNativeDataTypeSimpleName( v ) ) );
            itemFieldGet.setData( FieldHelper.getGetSignature( itemData, v ) );
            TreeItem itemFieldSet = new TreeItem( itemField, SWT.NULL );
            itemFieldSet.setText( "setValue()" );
            itemFieldSet.setData( itemData + ".setValue(r, value);" );
          }
        }
      }
    } );
  }

  // Adds the Current item to the current Position
  private void treeDblClick( Event event ) {
    StyledTextComp wScript = getStyledTextComp();
    Point point = new Point( event.x, event.y );
    TreeItem item = wTree.getItem( point );

    // Qualification where the Click comes from
    if ( item != null && item.getParentItem() != null ) {
      if ( item.getParentItem().equals( wTreeClassesItem ) ) {
        setActiveCtab( item.getText() );
      } else if ( !item.getData().equals( "Snippit" ) ) {
        int iStart = wScript.getCaretOffset();
        int selCount = wScript.getSelectionCount(); // this selection
        // will be replaced
        // by wScript.insert
        iStart = iStart - selCount; // when a selection is already there
        // we need to subtract the position
        if ( iStart < 0 ) {
          iStart = 0; // just safety
        }
        String strInsert = (String) item.getData();
        wScript.insert( strInsert );
        wScript.setSelection( iStart, iStart + strInsert.length() );
      }
    }
  }

  private void buildingFolderMenu() {
    // styledTextPopupmenu = new Menu(, SWT.POP_UP);
    MenuItem addNewItem = new MenuItem( cMenu, SWT.PUSH );
    addNewItem.setText( BaseMessages.getString( PKG, "UserDefinedJavaClassDialog.AddNewTab" ) );
    addNewItem.addListener( SWT.Selection, new Listener() {
      public void handleEvent( Event e ) {
        addCtab( "", "", TabAddActions.ADD_BLANK );
      }
    } );

    MenuItem copyItem = new MenuItem( cMenu, SWT.PUSH );
    copyItem.setText( BaseMessages.getString( PKG, "UserDefinedJavaClassDialog.AddCopy" ) );
    copyItem.addListener( SWT.Selection, new Listener() {
      public void handleEvent( Event e ) {
        CTabItem item = folder.getSelection();
        StyledTextComp st = (StyledTextComp) item.getControl();
        addCtab( item.getText(), st.getText(), TabAddActions.ADD_COPY );
      }
    } );
    new MenuItem( cMenu, SWT.SEPARATOR );

    MenuItem setActiveScriptItem = new MenuItem( cMenu, SWT.PUSH );
    setActiveScriptItem.setText( BaseMessages.getString( PKG, "UserDefinedJavaClassDialog.SetTransformClass" ) );
    setActiveScriptItem.addListener( SWT.Selection, new Listener() {
      public void handleEvent( Event e ) {
        CTabItem item = folder.getSelection();
        for ( int i = 0; i < folder.getItemCount(); i++ ) {
          if ( folder.getItem( i ).equals( item ) ) {
            if ( item.getImage().equals( imageActiveScript ) ) {
              strActiveScript = "";
            }
            item.setImage( imageActiveScript );
            strActiveScript = item.getText();
          } else if ( folder.getItem( i ).getImage().equals( imageActiveScript ) ) {
            folder.getItem( i ).setImage( imageInactiveScript );
          }
        }
        modifyTabTree( item, TabActions.SET_ACTIVE_ITEM );
      }
    } );

    new MenuItem( cMenu, SWT.SEPARATOR );
    MenuItem setRemoveScriptItem = new MenuItem( cMenu, SWT.PUSH );
    setRemoveScriptItem.setText( BaseMessages.getString( PKG, "UserDefinedJavaClassDialog.RemoveClassType" ) );
    setRemoveScriptItem.addListener( SWT.Selection, new Listener() {
      public void handleEvent( Event e ) {
        CTabItem item = folder.getSelection();
        input.setChanged( true );
        if ( item.getImage().equals( imageActiveScript ) ) {
          strActiveScript = "";
        }
        item.setImage( imageInactiveScript );
      }
    } );

    folder.setMenu( cMenu );
  }

  private void buildingTreeMenu() {
    // styledTextPopupmenu = new Menu(, SWT.POP_UP);
    MenuItem addDeleteItem = new MenuItem( tMenu, SWT.PUSH );
    addDeleteItem.setText( BaseMessages.getString( PKG, "UserDefinedJavaClassDialog.Delete.Label" ) );
    addDeleteItem.addListener( SWT.Selection, new Listener() {
      public void handleEvent( Event e ) {
        if ( wTree.getSelectionCount() <= 0 ) {
          return;
        }

        TreeItem tItem = wTree.getSelection()[ 0 ];
        if ( tItem != null ) {
          MessageBox messageBox = new MessageBox( shell, SWT.ICON_QUESTION | SWT.NO | SWT.YES );
          messageBox.setText( BaseMessages.getString( PKG, "UserDefinedJavaClassDialog.DeleteItem.Label" ) );
          messageBox.setMessage( BaseMessages.getString(
            PKG, "UserDefinedJavaClassDialog.ConfirmDeleteItem.Label", tItem.getText() ) );
          switch ( messageBox.open() ) {
            case SWT.YES:
              modifyCTabItem( tItem, TabActions.DELETE_ITEM, "" );
              tItem.dispose();
              input.setChanged();
              break;
            default:
              break;
          }
        }
      }
    } );

    MenuItem renItem = new MenuItem( tMenu, SWT.PUSH );
    renItem.setText( BaseMessages.getString( PKG, "UserDefinedJavaClassDialog.Rename.Label" ) );
    renItem.addListener( SWT.Selection, new Listener() {
      public void handleEvent( Event e ) {
        renameFunction( wTree.getSelection()[ 0 ] );
      }
    } );

    new MenuItem( tMenu, SWT.SEPARATOR );
    MenuItem helpItem = new MenuItem( tMenu, SWT.PUSH );
    helpItem.setText( BaseMessages.getString( PKG, "UserDefinedJavaClassDialog.Sample.Label" ) );
    helpItem.addListener( SWT.Selection, new Listener() {
      public void handleEvent( Event e ) {
        String snippitFullName = wTree.getSelection()[ 0 ].getText();
        String sampleTabName = snippitFullName.replace( "Implement ", "" ).replace( ' ', '_' ) + "_Sample";

        if ( getCTabPosition( sampleTabName ) == -1 ) {
          addCtab( sampleTabName, snippitsHelper.getSample( snippitFullName ), TabAddActions.ADD_DEFAULT );
        }

        if ( getCTabPosition( sampleTabName ) != -1 ) {
          setActiveCtab( sampleTabName );
        }
      }
    } );

    wTree.addListener( SWT.MouseDown, new Listener() {
      public void handleEvent( Event e ) {
        if ( wTree.getSelectionCount() <= 0 ) {
          return;
        }

        TreeItem tItem = wTree.getSelection()[ 0 ];
        if ( tItem != null ) {
          TreeItem pItem = tItem.getParentItem();

          if ( pItem != null && pItem.equals( wTreeClassesItem ) ) {
            if ( folder.getItemCount() > 1 ) {
              tMenu.getItem( 0 ).setEnabled( true );
            } else {
              tMenu.getItem( 0 ).setEnabled( false );
            }
            tMenu.getItem( 1 ).setEnabled( true );
            tMenu.getItem( 3 ).setEnabled( false );
          } else if ( tItem.equals( wTreeClassesItem ) ) {
            tMenu.getItem( 0 ).setEnabled( false );
            tMenu.getItem( 1 ).setEnabled( false );
            tMenu.getItem( 3 ).setEnabled( false );
          } else if ( pItem != null && pItem.getData() != null && pItem.getData().equals( "Snippits Category" ) ) {
            tMenu.getItem( 0 ).setEnabled( false );
            tMenu.getItem( 1 ).setEnabled( false );
            tMenu.getItem( 3 ).setEnabled( true );
          } else {
            tMenu.getItem( 0 ).setEnabled( false );
            tMenu.getItem( 1 ).setEnabled( false );
            tMenu.getItem( 3 ).setEnabled( false );
          }
        }
      }
    } );
    wTree.setMenu( tMenu );
  }

  private void addRenameToTreeScriptItems() {
    lastItem = new TreeItem[ 1 ];
    editor = new TreeEditor( wTree );
    wTree.addListener( SWT.Selection, new Listener() {
      public void handleEvent( Event event ) {
        final TreeItem item = (TreeItem) event.item;
        renameFunction( item );
      }
    } );
  }

  // This function is for a Windows Like renaming inside the tree
  private void renameFunction( TreeItem tItem ) {
    final TreeItem item = tItem;
    if ( item.getParentItem() != null && item.getParentItem().equals( wTreeClassesItem ) ) {
      if ( item != null && item == lastItem[ 0 ] ) {
        boolean isCarbon = SWT.getPlatform().equals( "carbon" );
        final Composite composite = new Composite( wTree, SWT.NONE );
        if ( !isCarbon ) {
          composite.setBackground( shell.getDisplay().getSystemColor( SWT.COLOR_BLACK ) );
        }
        final Text text = new Text( composite, SWT.NONE );
        final int inset = isCarbon ? 0 : 1;
        composite.addListener( SWT.Resize, new Listener() {
          public void handleEvent( Event e ) {
            Rectangle rect = composite.getClientArea();
            text.setBounds( rect.x + inset, rect.y + inset, rect.width - inset * 2, rect.height - inset * 2 );
          }
        } );
        Listener textListener = new Listener() {
          @SuppressWarnings( "fallthrough" )
          public void handleEvent( final Event e ) {
            switch ( e.type ) {
              case SWT.FocusOut:
                if ( text.getText().length() > 0 ) {
                  // Check if the field_name Exists
                  if ( getCTabItemByName( text.getText() ) == null ) {
                    modifyCTabItem( item, TabActions.RENAME_ITEM, text.getText() );
                    item.setText( cleanClassName( text.getText() ) );
                  }
                }
                composite.dispose();
                break;
              case SWT.Verify:
                String newText = text.getText();
                String leftText = newText.substring( 0, e.start );
                String rightText = newText.substring( e.end, newText.length() );
                Point size = TextSizeUtilFacade.textExtent( leftText + e.text + rightText );
                size = text.computeSize( size.x, SWT.DEFAULT );
                editor.horizontalAlignment = SWT.LEFT;
                Rectangle itemRect = item.getBounds(),
                  rect = wTree.getClientArea();
                editor.minimumWidth = Math.max( size.x, itemRect.width ) + inset * 2;
                int left = itemRect.x,
                  right = rect.x + rect.width;
                editor.minimumWidth = Math.min( editor.minimumWidth, right - left );
                editor.minimumHeight = size.y + inset * 2;
                editor.layout();
                break;
              case SWT.Traverse:
                switch ( e.detail ) {
                  case SWT.TRAVERSE_RETURN:
                    if ( text.getText().length() > 0 ) {
                      // Check if the field_name Exists
                      if ( getCTabItemByName( text.getText() ) == null ) {
                        modifyCTabItem( item, TabActions.RENAME_ITEM, text.getText() );
                        item.setText( cleanClassName( text.getText() ) );
                      }
                    }
                  case SWT.TRAVERSE_ESCAPE:
                    composite.dispose();
                    e.doit = false;
                    break;
                  default:
                    break;
                }
                break;
              default:
                break;
            }
          }
        };
        text.addListener( SWT.FocusOut, textListener );
        text.addListener( SWT.Traverse, textListener );
        text.addListener( SWT.Verify, textListener );
        editor.setEditor( composite, item );
        text.setText( item.getText() );
        text.selectAll();
        text.setFocus();

      }
    }
    lastItem[ 0 ] = item;
  }

  private String cleanClassName( String unsafeName ) {
    return unsafeName.replaceAll( "(?:^[^\\p{Alpha}])|[^\\w]", "" );
  }
}
