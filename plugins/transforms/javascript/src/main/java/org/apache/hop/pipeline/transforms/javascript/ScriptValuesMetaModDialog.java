//CHECKSTYLE:FileLength:OFF
/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.pipeline.transforms.javascript;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.rowgenerator.RowGeneratorMeta;
import org.apache.hop.ui.core.dialog.EnterTextDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.PreviewRowsDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.StyledTextComp;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.dialog.PipelinePreviewProgressDialog;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.*;
import org.eclipse.swt.dnd.*;
import org.eclipse.swt.events.*;
import org.eclipse.swt.graphics.GC;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;
import org.mozilla.javascript.*;
import org.mozilla.javascript.ast.ScriptNode;
import org.mozilla.javascript.tools.ToolErrorReporter;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.Hashtable;
import java.util.List;
import java.util.*;

public class ScriptValuesMetaModDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = ScriptValuesMetaMod.class; // for i18n purposes, needed by Translator!!

  private static final String[] YES_NO_COMBO = new String[] {
    BaseMessages.getString( PKG, "System.Combo.No" ), BaseMessages.getString( PKG, "System.Combo.Yes" ) };

  private ModifyListener lsMod;

  private TableView wFields;

  private Label wlPosition;

  // private Button wHelp;

  private Tree wTree;
  private TreeItem wTreeScriptsItem;
  private TreeItem wTreeClassesitem;

  private Image imageActiveScript = null;
  private Image imageInactiveScript = null;
  private Image imageActiveStartScript = null;
  private Image imageActiveEndScript = null;
  private Image imageInputFields = null;
  private Image imageOutputFields = null;
  private Image imageArrowOrange = null;
  private Image imageArrowGreen = null;
  private Image imageUnderGreen = null;
  private Image imageAddScript = null;
  private Image imageDeleteScript = null;
  private Image imageDuplicateScript = null;

  private CTabFolder folder;
  private Menu cMenu;
  private Menu tMenu;

  // Suport for Rename Tree
  private TreeItem[] lastItem;
  private TreeEditor editor;

  private static final int DELETE_ITEM = 0;
  private static final int ADD_ITEM = 1;
  private static final int RENAME_ITEM = 2;
  private static final int SET_ACTIVE_ITEM = 3;

  private static final int ADD_COPY = 2;
  private static final int ADD_BLANK = 1;
  private static final int ADD_DEFAULT = 0;

  private String strActiveScript;
  private String strActiveStartScript;
  private String strActiveEndScript;

  private static final String[] jsFunctionList = ScriptValuesAddedFunctions.jsFunctionList;

  public static final int SKIP_PIPELINE = 1;
  private static final int ABORT_PIPELINE = -1;
  private static final int ERROR_PIPELINE = -2;
  private static final int CONTINUE_PIPELINE = 0;

  private final ScriptValuesMetaMod input;
  private ScriptValuesHelp scVHelp;
  private TextVar wOptimizationLevel;

  private TreeItem iteminput;

  private TreeItem itemoutput;

  private static final GuiResource guiresource = GuiResource.getInstance();

  private IRowMeta rowPrevTransformFields;

  private RowGeneratorMeta genMeta;

  public ScriptValuesMetaModDialog(Shell parent, Object in, PipelineMeta pipelineMeta, String sname ) {

    super( parent, (BaseTransformMeta) in, pipelineMeta, sname );
    input = (ScriptValuesMetaMod) in;
    genMeta = null;
    try {
      // ImageLoader xl = new ImageLoader();
      imageUnderGreen = guiresource.getImage("ui/images/add_all.svg");
      imageArrowGreen = guiresource.getImage("ui/images/add_all.svg");
      imageArrowOrange = guiresource.getImage("ui/images/add_all.svg");
      imageInputFields = guiresource.getImage("ui/images/hop-input.svg");
      imageOutputFields = guiresource.getImage("ui/images/hop-output.svg");
      imageActiveScript = guiresource.getImage("ui/images/active-script.svg");
      imageInactiveScript = guiresource.getImage("ui/images/inactive-script.svg");
      imageActiveStartScript = guiresource.getImage("ui/images/start-script.svg");
      imageActiveEndScript = guiresource.getImage("ui/images/end-script.svg");
      imageDeleteScript = guiresource.getImage("ui/images/generic-delete.svg");
      imageAddScript = guiresource.getImage("ui/images/addSmall.svg");
      imageDuplicateScript = guiresource.getImage("ui/images/copy-hop.svg");
    } catch ( Exception e ) {
      imageActiveScript = guiresource.getImageEmpty16x16();
      imageInactiveScript = guiresource.getImageEmpty16x16();
      imageActiveStartScript = guiresource.getImageEmpty16x16();
      imageActiveEndScript = guiresource.getImageEmpty16x16();
      imageInputFields = guiresource.getImageEmpty16x16();
      imageOutputFields = guiresource.getImageEmpty16x16();
      imageArrowOrange = guiresource.getImageEmpty16x16();
      imageArrowGreen = guiresource.getImageEmpty16x16();
      imageUnderGreen = guiresource.getImageEmpty16x16();
      imageDeleteScript = guiresource.getImageEmpty16x16();
      imageAddScript = guiresource.getImageEmpty16x16();
      imageDuplicateScript = guiresource.getImageEmpty16x16();
    }

    try {
      scVHelp = new ScriptValuesHelp("org/apache/hop/pipeline/transforms/javascript/jsFunctionHelp.xml");
    } catch ( Exception e ) {
      new ErrorDialog(
        shell, "Unexpected error", "There was an unexpected error reading the javascript functions help", e );
    }

  }

  public String open() {

    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, input );

    lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "ScriptValuesDialogMod.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // Filename line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "ScriptValuesDialogMod.TransformName.Label" ) );
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

    SashForm wSash = new SashForm(shell, SWT.VERTICAL);

    // Top sash form
    //
    Composite wTop = new Composite(wSash, SWT.NONE);
    props.setLook(wTop);

    FormLayout topLayout = new FormLayout();
    topLayout.marginWidth = Const.FORM_MARGIN;
    topLayout.marginHeight = Const.FORM_MARGIN;
    wTop.setLayout( topLayout );

    // Script line
    Label wlScriptFunctions = new Label(wTop, SWT.NONE);
    wlScriptFunctions.setText( BaseMessages.getString( PKG, "ScriptValuesDialogMod.JavascriptFunctions.Label" ) );
    props.setLook(wlScriptFunctions);
    FormData fdlScriptFunctions = new FormData();
    fdlScriptFunctions.left = new FormAttachment( 0, 0 );
    fdlScriptFunctions.top = new FormAttachment( 0, 0 );
    wlScriptFunctions.setLayoutData(fdlScriptFunctions);

    // Tree View Test
    wTree = new Tree(wTop, SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL );
    props.setLook( wTree );
    FormData fdlTree = new FormData();
    fdlTree.left = new FormAttachment( 0, 0 );
    fdlTree.top = new FormAttachment(wlScriptFunctions, margin );
    fdlTree.right = new FormAttachment( 20, 0 );
    fdlTree.bottom = new FormAttachment( 100, -margin );
    wTree.setLayoutData(fdlTree);

    // Script line
    Label wlScript = new Label(wTop, SWT.NONE);
    wlScript.setText( BaseMessages.getString( PKG, "ScriptValuesDialogMod.Javascript.Label" ) );
    props.setLook(wlScript);
    FormData fdlScript = new FormData();
    fdlScript.left = new FormAttachment( wTree, margin );
    fdlScript.top = new FormAttachment( 0, 0 );
    wlScript.setLayoutData(fdlScript);

    folder = new CTabFolder(wTop, SWT.BORDER | SWT.RESIZE );
    folder.setSimple( false );
    folder.setUnselectedImageVisible( true );
    folder.setUnselectedCloseVisible( true );
    FormData fdScript = new FormData();
    fdScript.left = new FormAttachment( wTree, margin );
    fdScript.top = new FormAttachment(wlScript, margin );
    fdScript.right = new FormAttachment( 100, -5 );
    fdScript.bottom = new FormAttachment( 100, -50 );
    folder.setLayoutData(fdScript);

    wlPosition = new Label(wTop, SWT.NONE );
    wlPosition.setText( BaseMessages.getString( PKG, "ScriptValuesDialogMod.Position.Label" ) );
    props.setLook( wlPosition );
    FormData fdlPosition = new FormData();
    fdlPosition.left = new FormAttachment( wTree, margin );
    fdlPosition.right = new FormAttachment( 30, 0 );
    fdlPosition.top = new FormAttachment( folder, margin );
    wlPosition.setLayoutData(fdlPosition);


    Label wlOptimizationLevel = new Label(wTop, SWT.NONE );
    wlOptimizationLevel.setText( BaseMessages.getString( PKG, "ScriptValuesDialogMod.OptimizationLevel.Label" ) );
    props.setLook( wlOptimizationLevel );
    FormData fdlOptimizationLevel = new FormData();
    fdlOptimizationLevel.left = new FormAttachment( wTree, margin * 2 );
    fdlOptimizationLevel.top = new FormAttachment( wlPosition, margin );
    wlOptimizationLevel.setLayoutData( fdlOptimizationLevel );

    wOptimizationLevel = new TextVar( pipelineMeta, wTop, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wOptimizationLevel.setToolTipText( BaseMessages.getString(
      PKG, "ScriptValuesDialogMod.OptimizationLevel.Tooltip" ) );
    props.setLook( wOptimizationLevel );
    FormData fdOptimizationLevel = new FormData();
    fdOptimizationLevel.left = new FormAttachment( wlOptimizationLevel, margin );
    fdOptimizationLevel.top = new FormAttachment( wlPosition, margin );
    fdOptimizationLevel.right = new FormAttachment( 100, margin );
    wOptimizationLevel.setLayoutData( fdOptimizationLevel );
    wOptimizationLevel.addModifyListener( lsMod );

    Text wlHelpLabel = new Text(wTop, SWT.V_SCROLL | SWT.LEFT);
    wlHelpLabel.setEditable( false );
    wlHelpLabel.setText( "Hallo" );
    props.setLook(wlHelpLabel);
    // private Listener lsHelp;
    FormData fdHelpLabel = new FormData();
    fdHelpLabel.left = new FormAttachment( wlPosition, margin );
    fdHelpLabel.top = new FormAttachment( folder, margin );
    fdHelpLabel.right = new FormAttachment( 100, -5 );
    fdHelpLabel.bottom = new FormAttachment( 100, 0 );
    wlHelpLabel.setLayoutData(fdHelpLabel);
    wlHelpLabel.setVisible( false );

    FormData fdTop = new FormData();
    fdTop.left = new FormAttachment( 0, 0 );
    fdTop.top = new FormAttachment( 0, 0 );
    fdTop.right = new FormAttachment( 100, 0 );
    fdTop.bottom = new FormAttachment( 100, 0 );
    wTop.setLayoutData(fdTop);

    Composite wBottom = new Composite(wSash, SWT.NONE);
    props.setLook(wBottom);

    FormLayout bottomLayout = new FormLayout();
    bottomLayout.marginWidth = Const.FORM_MARGIN;
    bottomLayout.marginHeight = Const.FORM_MARGIN;
    wBottom.setLayout( bottomLayout );

    Label wSeparator = new Label(wBottom, SWT.SEPARATOR | SWT.HORIZONTAL);
    FormData fdSeparator = new FormData();
    fdSeparator.left = new FormAttachment( 0, 0 );
    fdSeparator.right = new FormAttachment( 100, 0 );
    fdSeparator.top = new FormAttachment( 0, -margin + 2 );
    wSeparator.setLayoutData(fdSeparator);

    Label wlFields = new Label(wBottom, SWT.NONE);
    wlFields.setText( BaseMessages.getString( PKG, "ScriptValuesDialogMod.Fields.Label" ) );
    props.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment( 0, 0 );
    fdlFields.top = new FormAttachment(wSeparator, 0 );
    wlFields.setLayoutData(fdlFields);

    final int FieldsRows = input.getFieldname().length;

    ColumnInfo[] colinf =
      new ColumnInfo[] {
        new ColumnInfo(
          BaseMessages.getString( PKG, "ScriptValuesDialogMod.ColumnInfo.Filename" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "ScriptValuesDialogMod.ColumnInfo.RenameTo" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "ScriptValuesDialogMod.ColumnInfo.Type" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, ValueMetaFactory.getValueMetaNames() ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "ScriptValuesDialogMod.ColumnInfo.Length" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "ScriptValuesDialogMod.ColumnInfo.Precision" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "ScriptValuesDialogMod.ColumnInfo.Replace" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, YES_NO_COMBO ), };

    wFields =
      new TableView(
        pipelineMeta, wBottom, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, FieldsRows, lsMod, props );

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment(wlFields, margin );
    fdFields.right = new FormAttachment( 100, 0 );
    fdFields.bottom = new FormAttachment( 100, 0 );
    wFields.setLayoutData(fdFields);

    FormData fdBottom = new FormData();
    fdBottom.left = new FormAttachment( 0, 0 );
    fdBottom.top = new FormAttachment( 0, 0 );
    fdBottom.right = new FormAttachment( 100, 0 );
    fdBottom.bottom = new FormAttachment( 100, 0 );
    wBottom.setLayoutData(fdBottom);

    FormData fdSash = new FormData();
    fdSash.left = new FormAttachment( 0, 0 );
    fdSash.top = new FormAttachment( wTransformName, 0 );
    fdSash.right = new FormAttachment( 100, 0 );
    fdSash.bottom = new FormAttachment( 100, -50 );
    wSash.setLayoutData(fdSash);

    wSash.setWeights( new int[] { 75, 25 } );

    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    Button wVars = new Button(shell, SWT.PUSH);
    wVars.setText( BaseMessages.getString( PKG, "ScriptValuesDialogMod.GetVariables.Button" ) );
    Button wTest = new Button(shell, SWT.PUSH);
    wTest.setText( BaseMessages.getString( PKG, "ScriptValuesDialogMod.TestScript.Button" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOk, wCancel, wVars, wTest}, margin, null );

    // Add listeners
    lsCancel = e -> cancel();
    // lsGet = new Listener() { public void handleEvent(Event e) { get(); } };
    Listener lsTest = e -> newTest();
    Listener lsVars = e -> test(true, true);
    lsOk = e -> ok();
    Listener lsTree = e -> treeDblClick(e);
    // lsHelp = new Listener(){public void handleEvent(Event e){ wlHelpLabel.setVisible(true); }};

    wCancel.addListener( SWT.Selection, lsCancel );
    // wGet.addListener (SWT.Selection, lsGet );
    wTest.addListener( SWT.Selection, lsTest);
    wVars.addListener( SWT.Selection, lsVars);
    wOk.addListener( SWT.Selection, lsOk );
    wTree.addListener( SWT.MouseDoubleClick, lsTree);

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
          messageBox.setText( BaseMessages.getString( PKG, "ScriptValuesDialogMod.DeleteItem.Label" ) );
          messageBox.setMessage( BaseMessages.getString(
            PKG, "ScriptValuesDialogMod.ConfirmDeleteItem.Label", cItem.getText() ) );
          switch ( messageBox.open() ) {
            case SWT.YES:
              modifyScriptTree( cItem, DELETE_ITEM );
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

    // Adding the Default Transform Scripts Item to the Tree
    wTreeScriptsItem = new TreeItem( wTree, SWT.NULL );
    wTreeScriptsItem.setImage( guiresource.getImageBol() );
    wTreeScriptsItem.setText( BaseMessages.getString( PKG, "ScriptValuesDialogMod.TransformScript.Label" ) );

    // Set the shell size, based upon previous time...
    setSize();
    getData();

    // Adding the Rest (Functions, InputItems, etc.) to the Tree
    buildSpecialFunctionsTree();

    // Input Fields
    iteminput = new TreeItem( wTree, SWT.NULL );
    iteminput.setImage( imageInputFields );
    iteminput.setText( BaseMessages.getString( PKG, "ScriptValuesDialogMod.InputFields.Label" ) );
    // Output Fields
    itemoutput = new TreeItem( wTree, SWT.NULL );
    itemoutput.setImage( imageOutputFields );
    itemoutput.setText( BaseMessages.getString( PKG, "ScriptValuesDialogMod.OutputFields.Label" ) );

    // Display waiting message for input
    TreeItem itemWaitFieldsIn = new TreeItem(iteminput, SWT.NULL);
    itemWaitFieldsIn.setText( BaseMessages.getString( PKG, "ScriptValuesDialogMod.GettingFields.Label" ) );
    itemWaitFieldsIn.setForeground( guiresource.getColorDirectory() );
    iteminput.setExpanded( true );

    // Display waiting message for output
    TreeItem itemWaitFieldsOut = new TreeItem(itemoutput, SWT.NULL);
    itemWaitFieldsOut.setText( BaseMessages.getString( PKG, "ScriptValuesDialogMod.GettingFields.Label" ) );
    itemWaitFieldsOut.setForeground( guiresource.getColorDirectory() );
    itemoutput.setExpanded( true );

    //
    // Search the fields in the background
    //

    final Runnable runnable = () -> {
      TransformMeta transformMeta = pipelineMeta.findTransform( transformName );
      if ( transformMeta != null ) {
        try {
          rowPrevTransformFields = pipelineMeta.getPrevTransformFields( transformMeta );
          if ( rowPrevTransformFields != null ) {
            setInputOutputFields();
          } else {
            // Can not get fields...end of wait message
            iteminput.removeAll();
            itemoutput.removeAll();
          }
        } catch ( HopException e ) {
          logError( BaseMessages.getString( PKG, "System.Dialog.GetFieldsFailed.Message" ) );
        }
      }
    };
    new Thread( runnable ).start();

    // rebuildInputFieldsTree();
    // buildOutputFieldsTree();
    buildAddClassesListTree();
    addRenameTowTreeScriptItems();
    input.setChanged( changed );

    // Create the drag source on the tree
    DragSource ds = new DragSource( wTree, DND.DROP_MOVE );
    ds.setTransfer( new Transfer[] { TextTransfer.getInstance() } );
    ds.addDragListener( new DragSourceAdapter() {

      public void dragStart( DragSourceEvent event ) {
        TreeItem item = wTree.getSelection()[ 0 ];

        // Qualifikation where the Drag Request Comes from
        if ( item != null && item.getParentItem() != null ) {
          if ( item.getParentItem().equals( wTreeScriptsItem ) ) {
            event.doit = false;
          } else if ( !item.getData().equals( "Function" ) ) {
            String strInsert = (String) item.getData();
            if ( strInsert.equals( "jsFunction" ) ) {
              event.doit = true;
            } else {
              event.doit = false;
            }
          } else {
            event.doit = false;
          }
        } else {
          event.doit = false;
        }

      }

      public void dragSetData( DragSourceEvent event ) {
        // Set the data to be the first selected item's text
        event.data = wTree.getSelection()[ 0 ].getText();
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

  private void setActiveCtab( String strName ) {
    if ( strName.length() == 0 ) {
      folder.setSelection( 0 );
    } else {
      folder.setSelection( getCTabPosition( strName ) );
    }
  }

  private void addCtab( String cScriptName, String strScript, int iType ) {
    CTabItem item = new CTabItem( folder, SWT.CLOSE );

    switch ( iType ) {
      case ADD_DEFAULT:
        item.setText( cScriptName );
        break;
      default:
        item.setText( getNextName( cScriptName ) );
        break;
    }
    StyledTextComp wScript =
      new StyledTextComp( pipelineMeta, item.getParent(), SWT.MULTI | SWT.LEFT | SWT.H_SCROLL | SWT.V_SCROLL, item
        .getText(), false );
    if ( ( strScript != null ) && strScript.length() > 0 ) {
      wScript.setText( strScript );
    } else {
      wScript.setText( BaseMessages.getString( PKG, "ScriptValuesDialogMod.ScriptHere.Label" )
        + Const.CR + Const.CR );
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
    ScriptValuesHighlight lineStyler = new ScriptValuesHighlight(ScriptValuesAddedFunctions.jsFunctionList);
    wScript.addLineStyleListener(lineStyler);
    item.setControl( wScript );

    // Adding new Item to Tree
    modifyScriptTree( item, ADD_ITEM );
  }

  private void modifyScriptTree( CTabItem ctabitem, int iModType ) {

    switch ( iModType ) {
      case DELETE_ITEM:
        TreeItem dItem = getTreeItemByName( ctabitem.getText() );
        if ( dItem != null ) {
          dItem.dispose();
          input.setChanged();
        }
        break;
      case ADD_ITEM:
        TreeItem item = new TreeItem( wTreeScriptsItem, SWT.NULL );
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
    TreeItem[] tItems = wTreeScriptsItem.getItems();
    for (TreeItem tItem : tItems) {
      if (tItem.getText().equals(strTabName)) {
        return tItem;
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
    for (CTabItem cItem : cItems) {
      if (cItem.getText().equals(strTabName)) {
        return cItem;
      }
    }
    return null;
  }

  private void modifyCTabItem( TreeItem tItem, int iModType, String strOption ) {

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
          } else if ( rItem.getImage().equals( imageActiveStartScript ) ) {
            strActiveStartScript = strOption;
          } else if ( rItem.getImage().equals( imageActiveEndScript ) ) {
            strActiveEndScript = strOption;
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

  /*
   * private void setStyledTextComp(String strText){ CTabItem item = folder.getSelection();
   * ((StyledTextComp)item.getControl()).setText(strText); }
   *
   * private void setStyledTextComp(String strText, CTabItem item){
   * ((StyledTextComp)item.getControl()).setText(strText); }
   */

  private String getNextName( String strActualName ) {
    String strRC = "";
    if ( strActualName.length() == 0 ) {
      strActualName = "Item";
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
    wlPosition.setText( BaseMessages.getString( PKG, "ScriptValuesDialogMod.Position.Label2" )
      + linenr + ", " + colnr );
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
//    wCompatible.setSelection( input.isCompatible() );
    if ( !Utils.isEmpty( Const.trim( input.getOptimizationLevel() ) ) ) {
      wOptimizationLevel.setText( input.getOptimizationLevel().trim() );
    } else {
      wOptimizationLevel.setText( ScriptValuesMetaMod.OPTIMIZATION_LEVEL_DEFAULT );
    }

    for ( int i = 0; i < input.getFieldname().length; i++ ) {
      if ( input.getFieldname()[ i ] != null && input.getFieldname()[ i ].length() > 0 ) {
        TableItem item = wFields.table.getItem( i );
        item.setText( 1, input.getFieldname()[ i ] );
        if ( input.getRename()[ i ] != null && !input.getFieldname()[ i ].equals( input.getRename()[ i ] ) ) {
          item.setText( 2, input.getRename()[ i ] );
        }
        item.setText( 3, ValueMetaFactory.getValueMetaName( input.getType()[ i ] ) );
        if ( input.getLength()[ i ] >= 0 ) {
          item.setText( 4, "" + input.getLength()[ i ] );
        }
        if ( input.getPrecision()[ i ] >= 0 ) {
          item.setText( 5, "" + input.getPrecision()[ i ] );
        }
        item.setText( 6, input.getReplace()[ i ] ? YES_NO_COMBO[ 1 ] : YES_NO_COMBO[ 0 ] );
      }
    }

    ScriptValuesScript[] jsScripts = input.getJSScripts();
    if ( jsScripts.length > 0 ) {
      for (ScriptValuesScript jsScript : jsScripts) {
        if (jsScript.isTransformScript()) {
          strActiveScript = jsScript.getScriptName();
        } else if (jsScript.isStartScript()) {
          strActiveStartScript = jsScript.getScriptName();
        } else if (jsScript.isEndScript()) {
          strActiveEndScript = jsScript.getScriptName();
        }
        addCtab(jsScript.getScriptName(), jsScript.getScript(), ADD_DEFAULT);
      }
    } else {
      addCtab( "", "", ADD_DEFAULT );
    }
    setActiveCtab( strActiveScript );
    refresh();

    wFields.setRowNums();
    wFields.optWidth( true );

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  // Setting default active Script
  private void refresh() {
    // CTabItem item = getCTabItemByName(strActiveScript);
    for ( int i = 0; i < folder.getItemCount(); i++ ) {
      CTabItem item = folder.getItem( i );
      if ( item.getText().equals( strActiveScript ) ) {
        item.setImage( imageActiveScript );
      } else if ( item.getText().equals( strActiveStartScript ) ) {
        item.setImage( imageActiveStartScript );
      } else if ( item.getText().equals( strActiveEndScript ) ) {
        item.setImage( imageActiveEndScript );
      } else {
        item.setImage( imageInactiveScript );
      }
    }
    // modifyScriptTree(null, SET_ACTIVE_ITEM);
  }

  private void refreshScripts() {
    CTabItem[] cTabs = folder.getItems();
    for (CTabItem cTab : cTabs) {
      if (cTab.getImage().equals(imageActiveStartScript)) {
        strActiveStartScript = cTab.getText();
      } else if (cTab.getImage().equals(imageActiveEndScript)) {
        strActiveEndScript = cTab.getText();
      }
    }
  }

  private boolean cancel() {
    if ( input.hasChanged() ) {
      MessageBox box = new MessageBox( shell, SWT.YES | SWT.NO | SWT.APPLICATION_MODAL | SWT.SHEET );
      box.setText( BaseMessages.getString( PKG, "ScriptValuesModDialog.WarningDialogChanged.Title" ) );
      box
        .setMessage( BaseMessages
          .getString( PKG, "ScriptValuesModDialog.WarningDialogChanged.Message", Const.CR ) );
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

  private void getInfo( ScriptValuesMetaMod meta ) {
//    meta.setCompatible( wCompatible.getSelection() );
    meta.setOptimizationLevel( wOptimizationLevel.getText() );
    int nrFields = wFields.nrNonEmpty();
    meta.allocate( nrFields );
    //CHECKSTYLE:Indentation:OFF
    for ( int i = 0; i < nrFields; i++ ) {
      TableItem item = wFields.getNonEmpty( i );
      meta.getFieldname()[ i ] = item.getText( 1 );
      meta.getRename()[ i ] = item.getText( 2 );
      if ( meta.getRename()[ i ] == null
        || meta.getRename()[ i ].length() == 0 || meta.getRename()[ i ].equalsIgnoreCase( meta.getFieldname()[ i ] ) ) {
        meta.getRename()[ i ] = meta.getFieldname()[ i ];
      }
      meta.getType()[ i ] = ValueMetaFactory.getIdForValueMeta( item.getText( 3 ) );
      String slen = item.getText( 4 );
      String sprc = item.getText( 5 );
      meta.getLength()[ i ] = Const.toInt( slen, -1 );
      meta.getPrecision()[ i ] = Const.toInt( sprc, -1 );
      meta.getReplace()[ i ] = YES_NO_COMBO[ 1 ].equalsIgnoreCase( item.getText( 6 ) );
    }

    // input.setActiveJSScript(strActiveScript);
    CTabItem[] cTabs = folder.getItems();
    if ( cTabs.length > 0 ) {
      ScriptValuesScript[] jsScripts = new ScriptValuesScript[ cTabs.length ];
      for ( int i = 0; i < cTabs.length; i++ ) {
        ScriptValuesScript jsScript =
          new ScriptValuesScript( ScriptValuesScript.NORMAL_SCRIPT, cTabs[ i ].getText(), getStyledTextComp(
            cTabs[ i ] ).getText() );
        if ( cTabs[ i ].getImage().equals( imageActiveScript ) ) {
          jsScript.setScriptType( ScriptValuesScript.TRANSFORM_SCRIPT );
        } else if ( cTabs[ i ].getImage().equals( imageActiveStartScript ) ) {
          jsScript.setScriptType( ScriptValuesScript.START_SCRIPT );
        } else if ( cTabs[ i ].getImage().equals( imageActiveEndScript ) ) {
          jsScript.setScriptType( ScriptValuesScript.END_SCRIPT );
        }
        jsScripts[ i ] = jsScript;
      }
      meta.setJSScripts( jsScripts );
    }
  }

  private void ok() {
    if ( Utils.isEmpty( wTransformName.getText() ) ) {
      return;
    }

    transformName = wTransformName.getText(); // return value

    boolean bInputOK = false;

    // Check if Active Script has set, otherwise Ask
    if ( getCTabItemByName( strActiveScript ) == null ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.CANCEL | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString( PKG, "ScriptValuesDialogMod.NoActiveScriptSet" ) );
      mb.setText( BaseMessages.getString( PKG, "ScriptValuesDialogMod.ERROR.Label" ) );
      switch ( mb.open() ) {
        case SWT.OK:
          strActiveScript = folder.getItem( 0 ).getText();
          refresh();
          bInputOK = true;
          break;
        case SWT.CANCEL:
          bInputOK = false;
          break;
        default:
          break;
      }
    } else {
      bInputOK = true;
    }

/*
    if ( bInputOK && wCompatible.getSelection() ) {
      // If in compatibility mode the "replace" column must not be "Yes"
      int nrFields = wFields.nrNonEmpty();
      for ( int i = 0; i < nrFields; i++ ) {
        TableItem item = wFields.getNonEmpty( i );
        if ( YES_NO_COMBO[ 1 ].equalsIgnoreCase( item.getText( 6 ) ) ) {
          bInputOK = false;
        }
      }
      if ( !bInputOK ) {
        MessageBox mb = new MessageBox( shell, SWT.OK | SWT.CANCEL | SWT.ICON_ERROR );
        mb
          .setMessage( BaseMessages
            .getString( PKG, "ScriptValuesDialogMod.ReplaceNotAllowedInCompatibilityMode" ) );
        mb.setText( BaseMessages.getString( PKG, "ScriptValuesDialogMod.ERROR.Label" ) );
        mb.open();
      }
    }
*/

    if ( bInputOK ) {
      getInfo( input );
      dispose();
    }
  }

  public boolean test() {
    return test( false, false );
  }

  private boolean newTest() {

    PluginRegistry registry = PluginRegistry.getInstance();
    String scriptTransformName = wTransformName.getText();

    try {
      // What fields are coming into the transform?
      //
      IRowMeta rowMeta = pipelineMeta.getPrevTransformFields( transformName ).clone();
      if ( rowMeta != null ) {
        // Create a new RowGenerator transform to generate rows for the test data...
        // Only create a new instance the first time to help the user.
        // Otherwise he/she has to key in the same test data all the time
        //
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
                string = valueMeta.getString(0L);
                break;
              case IValueMeta.TYPE_NUMBER:
                genMeta.getFieldFormat()[ i ] = "#.#";
                valueMeta.setConversionMask( genMeta.getFieldFormat()[ i ] );
                string = valueMeta.getString(0.0D);
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

        // Now create a JavaScript transform with the information in this dialog
        //
        ScriptValuesMetaMod scriptMeta = new ScriptValuesMetaMod();
        getInfo( scriptMeta );
        TransformMeta scriptTransform =
          new TransformMeta( registry.getPluginId( TransformPluginType.class, scriptMeta ), Const.NVL(
            scriptTransformName, "## SCRIPT ##" ), scriptMeta );
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
//        if ( HopGui.getInstance().editTransform( pipelineMeta, genTransform ) != null ) {
          // Now run this pipeline and grab the results...
          //
          PipelinePreviewProgressDialog progressDialog =
            new PipelinePreviewProgressDialog(
              shell, pipelineMeta, new String[] { scriptTransformName, }, new int[] { Const.toInt( genMeta
              .getRowLimit(), 10 ), } );
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

          IRowMeta previewRowsMeta = progressDialog.getPreviewRowsMeta( wTransformName.getText() );
          List<Object[]> previewRows = progressDialog.getPreviewRows( wTransformName.getText() );

          if ( previewRowsMeta != null && previewRows != null && previewRows.size() > 0 ) {
            PreviewRowsDialog prd =
              new PreviewRowsDialog(
                shell, pipelineMeta, SWT.NONE, wTransformName.getText(), previewRowsMeta, previewRows, loggingText );
            prd.open();
          }
        }

        return true;
//      } else {
//        throw new HopException( BaseMessages.getString(
//          PKG, "ScriptValuesDialogMod.Exception.CouldNotGetFields" ) );
//      }
    } catch ( Exception e ) {
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "ScriptValuesDialogMod.TestFailed.DialogTitle" ), BaseMessages
        .getString( PKG, "ScriptValuesDialogMod.TestFailed.DialogMessage" ), e );
      return false;
    }

  }

  private boolean test( boolean getvars, boolean popup ) {
    boolean retval = true;
    StyledTextComp wScript = getStyledTextComp();
    String scr = wScript.getText();
    HopException testException = null;

    Context jscx;
    Scriptable jsscope;
    // Script jsscript;

    // Making Refresh to get Active Script State
    refreshScripts();

    jscx = ContextFactory.getGlobal().enterContext();
    jscx.setOptimizationLevel( -1 );
    jsscope = jscx.initStandardObjects( null, false );

    // Adding the existing Scripts to the Context
    for ( int i = 0; i < folder.getItemCount(); i++ ) {
      StyledTextComp sItem = getStyledTextComp( folder.getItem( i ) );
      Scriptable jsR = Context.toObject( sItem.getText(), jsscope );
      jsscope.put( folder.getItem( i ).getText(), jsscope, jsR );
    }

    // Adding the Name of the Pipeline to the Context
    jsscope.put( "_PipelineName_", jsscope, this.transformName );

    try {
      IRowMeta rowMeta = pipelineMeta.getPrevTransformFields( transformName );
      if ( rowMeta != null ) {

        ScriptValuesModDummy dummyTransform = new ScriptValuesModDummy( rowMeta, pipelineMeta.getTransformFields( transformName ) );
        Scriptable jsvalue = Context.toObject( dummyTransform, jsscope );
        jsscope.put( "_transform_", jsscope, jsvalue );

        // Modification for Additional Script parsing
        try {
          if ( input.getAddClasses() != null ) {
            for ( int i = 0; i < input.getAddClasses().length; i++ ) {
              Object jsOut = Context.javaToJS( input.getAddClasses()[ i ].getAddObject(), jsscope );
              ScriptableObject.putProperty( jsscope, input.getAddClasses()[ i ].getJSName(), jsOut );
            }
          }
        } catch ( Exception e ) {
          testException =
            new HopException( BaseMessages.getString( PKG, "ScriptValuesDialogMod.CouldNotAddToContext", e
              .toString() ) );
          retval = false;
        }

        // Adding some default JavaScriptFunctions to the System
        try {
          Context.javaToJS( ScriptValuesAddedFunctions.class, jsscope );
          ( (ScriptableObject) jsscope ).defineFunctionProperties(
            jsFunctionList, ScriptValuesAddedFunctions.class, ScriptableObject.DONTENUM );
        } catch ( Exception ex ) {
          testException =
            new HopException( BaseMessages.getString(
              PKG, "ScriptValuesDialogMod.CouldNotAddDefaultFunctions", ex.toString() ) );
          retval = false;
        }

        // Adding some Constants to the JavaScript
        try {
          jsscope.put( "SKIP_PIPELINE", jsscope, SKIP_PIPELINE);
          jsscope.put( "ABORT_PIPELINE", jsscope, ABORT_PIPELINE);
          jsscope.put( "ERROR_PIPELINE", jsscope, ERROR_PIPELINE);
          jsscope.put( "CONTINUE_PIPELINE", jsscope, CONTINUE_PIPELINE);
        } catch ( Exception ex ) {
          testException =
            new HopException( BaseMessages.getString(
              PKG, "ScriptValuesDialogMod.CouldNotAddPipelineConstants", ex.toString() ) );
          retval = false;
        }

        try {
          Object[] row = new Object[ rowMeta.size() ];
          Scriptable jsRowMeta = Context.toObject( rowMeta, jsscope );
          jsscope.put( "rowMeta", jsscope, jsRowMeta );
          for ( int i = 0; i < rowMeta.size(); i++ ) {
            IValueMeta valueMeta = rowMeta.getValueMeta( i );
            Object valueData = null;

            // Set date and string values to something to simulate real thing
            //
            if ( valueMeta.isDate() ) {
              valueData = new Date();
            }
            if ( valueMeta.isString() ) {
              valueData = "test value test value test value test value test value "
                + "test value test value test value test value test value";
            }
            if ( valueMeta.isInteger() ) {
              valueData = 0L;
            }
            if ( valueMeta.isNumber() ) {
              valueData = 0.0;
            }
            if ( valueMeta.isBigNumber() ) {
              valueData = BigDecimal.ZERO;
            }
            if ( valueMeta.isBoolean() ) {
              valueData = Boolean.TRUE;
            }
            if ( valueMeta.isBinary() ) {
              valueData = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, };
            }

            if ( valueMeta.isStorageBinaryString() ) {
              valueMeta.setStorageType( IValueMeta.STORAGE_TYPE_NORMAL );
            }

            row[ i ] = valueData;

            Scriptable jsarg = Context.toObject( valueData, jsscope );
            jsscope.put( valueMeta.getName(), jsscope, jsarg );
          }

          // OK, for these input values, we're going to allow the user to edit the default values...
          // We are displaying a
          // 2)

          // Add support for Value class (new Value())
//          Scriptable jsval = Context.toObject( Value.class, jsscope );
//          jsscope.put( "Value", jsscope, jsval );

          Scriptable jsRow = Context.toObject( row, jsscope );
          jsscope.put( "row", jsscope, jsRow );

        } catch ( Exception ev ) {
          testException =
            new HopException( BaseMessages.getString( PKG, "ScriptValuesDialogMod.CouldNotAddInputFields", ev
              .toString() ) );
          retval = false;
        }

        try {
          // Checking for StartScript
          if ( strActiveStartScript != null
            && !folder.getSelection().getText().equals( strActiveStartScript )
            && strActiveStartScript.length() > 0 ) {
            String strStartScript =
              getStyledTextComp( folder.getItem( getCTabPosition( strActiveStartScript ) ) ).getText();
            /* Object startScript = */
            jscx.evaluateString( jsscope, strStartScript, "pipeline_Start", 1, null );
          }
        } catch ( Exception e ) {
          testException =
            new HopException( BaseMessages.getString( PKG, "ScriptValuesDialogMod.CouldProcessStartScript", e
              .toString() ) );
          retval = false;
        }

        try {

          Script evalScript = jscx.compileString( scr, "script", 1, null );
          evalScript.exec( jscx, jsscope );

          if ( getvars ) {
            ScriptNode tree = parseVariables( jscx, jsscope, scr, "script", 1, null );
            for ( int i = 0; i < tree.getParamAndVarCount(); i++ ) {
              String varname = tree.getParamOrVarName( i );
              if ( !varname.equalsIgnoreCase( "row" ) && !varname.equalsIgnoreCase( "pipeline_Status" ) ) {
                int type = IValueMeta.TYPE_STRING;
                int length = -1, precision = -1;
                Object result = jsscope.get( varname, jsscope );
                if ( result != null ) {
                  String classname = result.getClass().getName();
                  if ( classname.equalsIgnoreCase( "java.lang.Byte" ) ) {
                    // MAX = 127
                    type = IValueMeta.TYPE_INTEGER;
                    length = 3;
                    precision = 0;
                  } else if ( classname.equalsIgnoreCase( "java.lang.Integer" ) ) {
                    // MAX = 2147483647
                    type = IValueMeta.TYPE_INTEGER;
                    length = 9;
                    precision = 0;
                  } else if ( classname.equalsIgnoreCase( "java.lang.Long" ) ) {
                    // MAX = 9223372036854775807
                    type = IValueMeta.TYPE_INTEGER;
                    length = 18;
                    precision = 0;
                  } else if ( classname.equalsIgnoreCase( "java.lang.Double" ) ) {
                    type = IValueMeta.TYPE_NUMBER;
                    length = 16;
                    precision = 2;

                  } else if ( classname.equalsIgnoreCase( "org.mozilla.javascript.NativeDate" )
                    || classname.equalsIgnoreCase( "java.util.Date" ) ) {
                    type = IValueMeta.TYPE_DATE;
                  } else if ( classname.equalsIgnoreCase( "java.lang.Boolean" ) ) {
                    type = IValueMeta.TYPE_BOOLEAN;
                  }
                }
                TableItem ti = new TableItem( wFields.table, SWT.NONE );
                ti.setText( 1, varname );
                ti.setText( 2, "" );
                ti.setText( 3, ValueMetaFactory.getValueMetaName( type ) );
                ti.setText( 4, length >= 0 ? ( "" + length ) : "" );
                ti.setText( 5, precision >= 0 ? ( "" + precision ) : "" );

                // If the variable name exists in the input, suggest to replace the value
                //
                ti.setText( 6, ( rowMeta.indexOfValue( varname ) >= 0 ) ? YES_NO_COMBO[ 1 ] : YES_NO_COMBO[ 0 ] );
              }
            }
            wFields.removeEmptyRows();
            wFields.setRowNums();
            wFields.optWidth( true );
          }

          // End Script!
        } catch ( EvaluatorException e ) {
          String position = "(" + e.lineNumber() + ":" + e.columnNumber() + ")";
          String message =
            BaseMessages.getString( PKG, "ScriptValuesDialogMod.Exception.CouldNotExecuteScript", position );
          testException = new HopException( message, e );
          retval = false;
        } catch ( JavaScriptException e ) {
          String position = "(" + e.lineNumber() + ":" + e.columnNumber() + ")";
          String message =
            BaseMessages.getString( PKG, "ScriptValuesDialogMod.Exception.CouldNotExecuteScript", position );
          testException = new HopException( message, e );
          retval = false;
        } catch ( Exception e ) {
          testException =
            new HopException( BaseMessages.getString(
              PKG, "ScriptValuesDialogMod.Exception.CouldNotExecuteScript2" ), e );
          retval = false;
        }
      } else {
        testException =
          new HopException( BaseMessages.getString( PKG, "ScriptValuesDialogMod.Exception.CouldNotGetFields" ) );
        retval = false;
      }

      if ( popup ) {
        if ( retval ) {
          if ( !getvars ) {
            MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_INFORMATION );
            mb.setMessage( BaseMessages.getString( PKG, "ScriptValuesDialogMod.ScriptCompilationOK" ) + Const.CR );
            mb.setText( "OK" );
            mb.open();
          }
        } else {
          new ErrorDialog(
            shell, BaseMessages.getString( PKG, "ScriptValuesDialogMod.TestFailed.DialogTitle" ), BaseMessages
            .getString( PKG, "ScriptValuesDialogMod.TestFailed.DialogMessage" ), testException );
        }
      }
    } catch ( HopException ke ) {
      retval = false;
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "ScriptValuesDialogMod.TestFailed.DialogTitle" ), BaseMessages
        .getString( PKG, "ScriptValuesDialogMod.TestFailed.DialogMessage" ), ke );
    } finally {
      if ( jscx != null ) {
        Context.exit();
      }
    }
    return retval;
  }

  private void buildSpecialFunctionsTree() {

    TreeItem item = new TreeItem( wTree, SWT.NULL );
    item.setImage( guiresource.getImageBol() );
    item.setText( BaseMessages.getString( PKG, "ScriptValuesDialogMod.TansformConstant.Label" ) );
    TreeItem itemT = new TreeItem( item, SWT.NULL );
    itemT.setImage( imageArrowGreen );
    itemT.setText( "SKIP_PIPELINE" );
    itemT.setData( "SKIP_PIPELINE" );
    // itemT = new TreeItem(item, SWT.NULL);
    // itemT.setText("ABORT_PIPELINE");
    // itemT.setData("ABORT_PIPELINE");
    itemT = new TreeItem( item, SWT.NULL );
    itemT.setImage( imageArrowGreen );
    itemT.setText( "ERROR_PIPELINE" );
    itemT.setData( "ERROR_PIPELINE" );
    itemT = new TreeItem( item, SWT.NULL );
    itemT.setImage( imageArrowGreen );
    itemT.setText( "CONTINUE_PIPELINE" );
    itemT.setData( "CONTINUE_PIPELINE" );

    item = new TreeItem( wTree, SWT.NULL );
    item.setImage( guiresource.getImageBol() );
    item.setText( BaseMessages.getString( PKG, "ScriptValuesDialogMod.TransformFunctions.Label" ) );
    String strData = "";

    // Adding the Grouping Items to the Tree
    TreeItem itemStringFunctionsGroup = new TreeItem( item, SWT.NULL );
    itemStringFunctionsGroup.setImage( imageUnderGreen );
    itemStringFunctionsGroup
      .setText( BaseMessages.getString( PKG, "ScriptValuesDialogMod.StringFunctions.Label" ) );
    itemStringFunctionsGroup.setData( "Function" );
    TreeItem itemNumericFunctionsGroup = new TreeItem( item, SWT.NULL );
    itemNumericFunctionsGroup.setImage( imageUnderGreen );
    itemNumericFunctionsGroup.setText( BaseMessages
      .getString( PKG, "ScriptValuesDialogMod.NumericFunctions.Label" ) );
    itemNumericFunctionsGroup.setData( "Function" );
    TreeItem itemDateFunctionsGroup = new TreeItem( item, SWT.NULL );
    itemDateFunctionsGroup.setImage( imageUnderGreen );
    itemDateFunctionsGroup.setText( BaseMessages.getString( PKG, "ScriptValuesDialogMod.DateFunctions.Label" ) );
    itemDateFunctionsGroup.setData( "Function" );
    TreeItem itemLogicFunctionsGroup = new TreeItem( item, SWT.NULL );
    itemLogicFunctionsGroup.setImage( imageUnderGreen );
    itemLogicFunctionsGroup.setText( BaseMessages.getString( PKG, "ScriptValuesDialogMod.LogicFunctions.Label" ) );
    itemLogicFunctionsGroup.setData( "Function" );
    TreeItem itemSpecialFunctionsGroup = new TreeItem( item, SWT.NULL );
    itemSpecialFunctionsGroup.setImage( imageUnderGreen );
    itemSpecialFunctionsGroup.setText( BaseMessages
      .getString( PKG, "ScriptValuesDialogMod.SpecialFunctions.Label" ) );
    itemSpecialFunctionsGroup.setData( "Function" );
    TreeItem itemFileFunctionsGroup = new TreeItem( item, SWT.NULL );
    itemFileFunctionsGroup.setImage( imageUnderGreen );
    itemFileFunctionsGroup.setText( BaseMessages.getString( PKG, "ScriptValuesDialogMod.FileFunctions.Label" ) );
    itemFileFunctionsGroup.setData( "Function" );

    // Loading the Default delivered JScript Functions
    // Method[] methods = ScriptValuesAddedFunctions.class.getMethods();
    // String strClassType = ScriptValuesAddedFunctions.class.toString();

    Hashtable<String, String> hatFunctions = scVHelp.getFunctionList();

    Vector<String> v = new Vector<>(hatFunctions.keySet());
    Collections.sort( v );

    for ( String strFunction : v ) {
      String strFunctionType = hatFunctions.get( strFunction );
      int iFunctionType = Integer.valueOf(strFunctionType);

      TreeItem itemFunction = null;
      switch ( iFunctionType ) {
        case ScriptValuesAddedFunctions.STRING_FUNCTION:
          itemFunction = new TreeItem( itemStringFunctionsGroup, SWT.NULL );
          break;
        case ScriptValuesAddedFunctions.NUMERIC_FUNCTION:
          itemFunction = new TreeItem( itemNumericFunctionsGroup, SWT.NULL );
          break;
        case ScriptValuesAddedFunctions.DATE_FUNCTION:
          itemFunction = new TreeItem( itemDateFunctionsGroup, SWT.NULL );
          break;
        case ScriptValuesAddedFunctions.LOGIC_FUNCTION:
          itemFunction = new TreeItem( itemLogicFunctionsGroup, SWT.NULL );
          break;
        case ScriptValuesAddedFunctions.SPECIAL_FUNCTION:
          itemFunction = new TreeItem( itemSpecialFunctionsGroup, SWT.NULL );
          break;
        case ScriptValuesAddedFunctions.FILE_FUNCTION:
          itemFunction = new TreeItem( itemFileFunctionsGroup, SWT.NULL );
          break;
        default:
          break;
      }
      if ( itemFunction != null ) {
        itemFunction.setText( strFunction );
        itemFunction.setImage( imageArrowGreen );
        strData = "jsFunction";
        itemFunction.setData( strData );
      }
    }
  }

  public boolean TreeItemExist( TreeItem itemToCheck, String strItemName ) {
    boolean bRC = false;
    if ( itemToCheck.getItemCount() > 0 ) {
      TreeItem[] items = itemToCheck.getItems();
      for (TreeItem item : items) {
        if (item.getText().equals(strItemName)) {
          return true;
        }
      }
    }
    return bRC;
  }

  private void setInputOutputFields() {
    shell.getDisplay().syncExec( () -> {
      // fields are got...end of wait message
      iteminput.removeAll();
      itemoutput.removeAll();

      String strItemInToAdd = "";
      String strItemToAddOut = "";

      // try{

      // IRowMeta r = pipelineMeta.getPrevTransformFields(transformName);
      if ( rowPrevTransformFields != null ) {
        // TreeItem item = new TreeItem(wTree, SWT.NULL);
        // item.setText(BaseMessages.getString(PKG, "ScriptValuesDialogMod.OutputFields.Label"));
        // String strItemToAdd="";

        for ( int i = 0; i < rowPrevTransformFields.size(); i++ ) {
          IValueMeta v = rowPrevTransformFields.getValueMeta( i );
          strItemToAddOut = v.getName() + ".setValue(var)";
          strItemInToAdd = v.getName();
          TreeItem itemFields = new TreeItem( iteminput, SWT.NULL );
          itemFields.setImage( imageArrowOrange );
          itemFields.setText( strItemInToAdd );
          itemFields.setData( strItemInToAdd );

          /*
           * switch(v.getType()){ case IValueMeta.TYPE_STRING : case IValueMeta.TYPE_NUMBER : case
           * IValueMeta.TYPE_INTEGER: case IValueMeta.TYPE_DATE : case
           * IValueMeta.TYPE_BOOLEAN: strItemToAdd=v.getName()+".setValue(var)"; break; default:
           * strItemToAdd=v.getName(); break; }
           */
        }
        TreeItem itemFields = new TreeItem( itemoutput, SWT.NULL );
        itemFields.setData( "" );
        itemFields
          .setText( BaseMessages.getString( PKG, "ScriptValuesDialogMod.OutputFiels.CompatibilityOff" ) );
      }
      /*
       * }catch(HopException ke){ new ErrorDialog(shell, BaseMessages.getString(PKG,
       * "ScriptValuesDialogMod.FailedToGetFields.DialogTitle"), BaseMessages.getString(PKG,
       * "ScriptValuesDialogMod.FailedToGetFields.DialogMessage"), ke); }
       */
    } );
  }

  // Adds the Current item to the current Position
  private void treeDblClick( Event event ) {
    StyledTextComp wScript = getStyledTextComp();
    Point point = new Point( event.x, event.y );
    TreeItem item = wTree.getItem( point );

    // Qualification where the Click comes from
    if ( item != null && item.getParentItem() != null ) {
      if ( item.getParentItem().equals( wTreeScriptsItem ) ) {
        setActiveCtab( item.getText() );
      } else if ( !item.getData().equals( "Function" ) ) {
        int iStart = wScript.getCaretOffset();
        int selCount = wScript.getSelectionCount(); // this selection will be replaced by wScript.insert
        iStart = iStart - selCount; // when a selection is already there we need to subtract the position
        if ( iStart < 0 ) {
          iStart = 0; // just safety
        }
        String strInsert = (String) item.getData();
        if ( strInsert.equals( "jsFunction" ) ) {
          strInsert = item.getText();
        }
        wScript.insert( strInsert );
        wScript.setSelection( iStart, iStart + strInsert.length() );
      }
    }
    /*
     * if (item != null && item.getParentItem()!=null && !item.getData().equals("Function")) { int iStart =
     * wScript.getCaretOffset(); String strInsert =(String)item.getData(); if(strInsert.equals("jsFunction")) strInsert
     * = (String)item.getText(); wScript.insert(strInsert); wScript.setSelection(iStart,iStart+strInsert.length()); }
     */
  }

  // Building the Tree for Additional Classes
  private void buildAddClassesListTree() {
    if ( wTreeClassesitem != null ) {
      wTreeClassesitem.dispose();
    }
    if ( input.getAddClasses() != null ) {
      for ( int i = 0; i < input.getAddClasses().length; i++ ) {
        try {
          Method[] methods = input.getAddClasses()[ i ].getAddClass().getMethods();
          String strClassType = input.getAddClasses()[ i ].getAddClass().toString();
          String strParams;
          wTreeClassesitem = new TreeItem( wTree, SWT.NULL );
          wTreeClassesitem.setText( input.getAddClasses()[ i ].getJSName() );
          for (Method method : methods) {
            String strDeclaringClass = method.getDeclaringClass().toString();
            if (strClassType.equals(strDeclaringClass)) {
              TreeItem item2 = new TreeItem(wTreeClassesitem, SWT.NULL);
              strParams = buildAddClassFunctionName(method);
              item2.setText(method.getName() + "(" + strParams + ")");
              String strData =
                      input.getAddClasses()[i].getJSName() + "." + method.getName() + "(" + strParams + ")";
              item2.setData(strData);
            }
          }
        } catch ( Exception e ) {
          // Ignore errors
        }
      }
    }
  }

  private String buildAddClassFunctionName( Method metForParams ) {
    StringBuilder sbRC = new StringBuilder();
    String strRC = "";
    Class<?>[] clsParamType = metForParams.getParameterTypes();
    String strParam;

    for (Class<?> aClass : clsParamType) {
      strParam = aClass.getName();
      if (strParam.toLowerCase().indexOf("javascript") <= 0) {
        if (strParam.toLowerCase().indexOf("object") > 0) {
          sbRC.append("var");
          sbRC.append(", ");
        } else if (strParam.equals("java.lang.String")) {
          sbRC.append("String");
          sbRC.append(", ");
        } else {
          sbRC.append(strParam);
          sbRC.append(", ");
        }
      }

    }
    strRC = sbRC.toString();
    if ( strRC.length() > 0 ) {
      strRC = strRC.substring( 0, sbRC.length() - 2 );
    }
    return strRC;
  }

  private void buildingFolderMenu() {
    // styledTextPopupmenu = new Menu(, SWT.POP_UP);
    MenuItem addNewItem = new MenuItem( cMenu, SWT.PUSH );
    addNewItem.setText( BaseMessages.getString( PKG, "ScriptValuesDialogMod.AddNewTab" ) );
    addNewItem.setImage( imageAddScript );
    addNewItem.addListener( SWT.Selection, e -> addCtab( "", "", ADD_BLANK ) );

    MenuItem copyItem = new MenuItem( cMenu, SWT.PUSH );
    copyItem.setText( BaseMessages.getString( PKG, "ScriptValuesDialogMod.AddCopy" ) );
    copyItem.setImage( imageDuplicateScript );
    copyItem.addListener( SWT.Selection, e -> {
      CTabItem item = folder.getSelection();
      StyledTextComp st = (StyledTextComp) item.getControl();
      addCtab( item.getText(), st.getText(), ADD_COPY );
    } );
    new MenuItem( cMenu, SWT.SEPARATOR );

    MenuItem setActiveScriptItem = new MenuItem( cMenu, SWT.PUSH );
    setActiveScriptItem.setText( BaseMessages.getString( PKG, "ScriptValuesDialogMod.SetTransformScript" ) );
    setActiveScriptItem.setImage( imageActiveScript );
    setActiveScriptItem.addListener( SWT.Selection, e -> {
      CTabItem item = folder.getSelection();
      for ( int i = 0; i < folder.getItemCount(); i++ ) {
        if ( folder.getItem( i ).equals( item ) ) {
          if ( item.getImage().equals( imageActiveScript ) ) {
            strActiveScript = "";
          } else if ( item.getImage().equals( imageActiveStartScript ) ) {
            strActiveStartScript = "";
          } else if ( item.getImage().equals( imageActiveEndScript ) ) {
            strActiveEndScript = "";
          }
          item.setImage( imageActiveScript );
          strActiveScript = item.getText();
        } else if ( folder.getItem( i ).getImage().equals( imageActiveScript ) ) {
          folder.getItem( i ).setImage( imageInactiveScript );
        }
      }
      modifyScriptTree( item, SET_ACTIVE_ITEM );
    } );

    MenuItem setStartScriptItem = new MenuItem( cMenu, SWT.PUSH );
    setStartScriptItem.setText( BaseMessages.getString( PKG, "ScriptValuesDialogMod.SetStartScript" ) );
    setStartScriptItem.setImage( imageActiveStartScript );
    setStartScriptItem.addListener( SWT.Selection, e -> {
      CTabItem item = folder.getSelection();
      for ( int i = 0; i < folder.getItemCount(); i++ ) {
        if ( folder.getItem( i ).equals( item ) ) {
          if ( item.getImage().equals( imageActiveScript ) ) {
            strActiveScript = "";
          } else if ( item.getImage().equals( imageActiveStartScript ) ) {
            strActiveStartScript = "";
          } else if ( item.getImage().equals( imageActiveEndScript ) ) {
            strActiveEndScript = "";
          }
          item.setImage( imageActiveStartScript );
          strActiveStartScript = item.getText();
        } else if ( folder.getItem( i ).getImage().equals( imageActiveStartScript ) ) {
          folder.getItem( i ).setImage( imageInactiveScript );
        }
      }
      modifyScriptTree( item, SET_ACTIVE_ITEM );
    } );

    MenuItem setEndScriptItem = new MenuItem( cMenu, SWT.PUSH );
    setEndScriptItem.setText( BaseMessages.getString( PKG, "ScriptValuesDialogMod.SetEndScript" ) );
    setEndScriptItem.setImage( imageActiveEndScript );
    setEndScriptItem.addListener( SWT.Selection, e -> {
      CTabItem item = folder.getSelection();
      for ( int i = 0; i < folder.getItemCount(); i++ ) {
        if ( folder.getItem( i ).equals( item ) ) {
          if ( item.getImage().equals( imageActiveScript ) ) {
            strActiveScript = "";
          } else if ( item.getImage().equals( imageActiveStartScript ) ) {
            strActiveStartScript = "";
          } else if ( item.getImage().equals( imageActiveEndScript ) ) {
            strActiveEndScript = "";
          }
          item.setImage( imageActiveEndScript );
          strActiveEndScript = item.getText();
        } else if ( folder.getItem( i ).getImage().equals( imageActiveEndScript ) ) {
          folder.getItem( i ).setImage( imageInactiveScript );
        }
      }
      modifyScriptTree( item, SET_ACTIVE_ITEM );
    } );
    new MenuItem( cMenu, SWT.SEPARATOR );
    MenuItem setRemoveScriptItem = new MenuItem( cMenu, SWT.PUSH );
    setRemoveScriptItem.setText( BaseMessages.getString( PKG, "ScriptValuesDialogMod.RemoveScriptType" ) );
    setRemoveScriptItem.setImage( imageInactiveScript );
    setRemoveScriptItem.addListener( SWT.Selection, e -> {
      CTabItem item = folder.getSelection();
      input.setChanged( true );
      if ( item.getImage().equals( imageActiveScript ) ) {
        strActiveScript = "";
      } else if ( item.getImage().equals( imageActiveStartScript ) ) {
        strActiveStartScript = "";
      } else if ( item.getImage().equals( imageActiveEndScript ) ) {
        strActiveEndScript = "";
      }
      item.setImage( imageInactiveScript );
    } );

    folder.setMenu( cMenu );
  }

  private void buildingTreeMenu() {
    // styledTextPopupmenu = new Menu(, SWT.POP_UP);
    MenuItem addDeleteItem = new MenuItem( tMenu, SWT.PUSH );
    addDeleteItem.setText( BaseMessages.getString( PKG, "ScriptValuesDialogMod.Delete.Label" ) );
    addDeleteItem.setImage( imageDeleteScript );
    addDeleteItem.addListener( SWT.Selection, e -> {
      if ( wTree.getSelectionCount() <= 0 ) {
        return;
      }

      TreeItem tItem = wTree.getSelection()[ 0 ];
      if ( tItem != null ) {
        MessageBox messageBox = new MessageBox( shell, SWT.ICON_QUESTION | SWT.NO | SWT.YES );
        messageBox.setText( BaseMessages.getString( PKG, "ScriptValuesDialogMod.DeleteItem.Label" ) );
        messageBox.setMessage( BaseMessages.getString(
          PKG, "ScriptValuesDialogMod.ConfirmDeleteItem.Label", tItem.getText() ) );
        switch ( messageBox.open() ) {
          case SWT.YES:
            modifyCTabItem( tItem, DELETE_ITEM, "" );
            tItem.dispose();
            input.setChanged();
            break;
          default:
            break;
        }
      }
    } );

    MenuItem renItem = new MenuItem( tMenu, SWT.PUSH );
    renItem.setText( BaseMessages.getString( PKG, "ScriptValuesDialogMod.Rename.Label" ) );
    renItem.addListener( SWT.Selection, e -> renameFunction( wTree.getSelection()[ 0 ] ) );

    new MenuItem( tMenu, SWT.SEPARATOR );
    MenuItem helpItem = new MenuItem( tMenu, SWT.PUSH );
    helpItem.setText( BaseMessages.getString( PKG, "ScriptValuesDialogMod.Sample.Label" ) );
    helpItem.addListener( SWT.Selection, e -> {
      String strFunctionName = wTree.getSelection()[ 0 ].getText();
      String strFunctionNameWithArgs = strFunctionName;
      strFunctionName = strFunctionName.substring( 0, strFunctionName.indexOf( '(' ) );
      String strHelpTabName = strFunctionName + "_Sample";

      if ( getCTabPosition( strHelpTabName ) == -1 ) {
        addCtab( strHelpTabName, scVHelp.getSample( strFunctionName, strFunctionNameWithArgs ), 0 );
      }

      if ( getCTabPosition( strHelpTabName ) != -1 ) {
        setActiveCtab( strHelpTabName );
      }
    } );

    wTree.addListener( SWT.MouseDown, e -> {
      if ( wTree.getSelectionCount() <= 0 ) {
        return;
      }

      TreeItem tItem = wTree.getSelection()[ 0 ];
      if ( tItem != null ) {
        TreeItem pItem = tItem.getParentItem();

        if ( pItem != null && pItem.equals( wTreeScriptsItem ) ) {
          if ( folder.getItemCount() > 1 ) {
            tMenu.getItem( 0 ).setEnabled( true );
          } else {
            tMenu.getItem( 0 ).setEnabled( false );
          }
          tMenu.getItem( 1 ).setEnabled( true );
          tMenu.getItem( 3 ).setEnabled( false );
        } else if ( tItem.equals( wTreeClassesitem ) ) {
          tMenu.getItem( 0 ).setEnabled( false );
          tMenu.getItem( 1 ).setEnabled( false );
          tMenu.getItem( 3 ).setEnabled( false );
        } else if ( tItem.getData() != null && tItem.getData().equals( "jsFunction" ) ) {
          tMenu.getItem( 0 ).setEnabled( false );
          tMenu.getItem( 1 ).setEnabled( false );
          tMenu.getItem( 3 ).setEnabled( true );
        } else {
          tMenu.getItem( 0 ).setEnabled( false );
          tMenu.getItem( 1 ).setEnabled( false );
          tMenu.getItem( 3 ).setEnabled( false );
        }
      }
    } );
    wTree.setMenu( tMenu );
  }

  private void addRenameTowTreeScriptItems() {
    lastItem = new TreeItem[ 1 ];
    editor = new TreeEditor( wTree );
    wTree.addListener( SWT.Selection, event -> {
      final TreeItem item = (TreeItem) event.item;
      renameFunction( item );
    } );
  }

  // This function is for a Windows Like renaming inside the tree
  private void renameFunction( TreeItem tItem ) {
    final TreeItem item = tItem;
    if ( item.getParentItem() != null && item.getParentItem().equals( wTreeScriptsItem ) ) {
      if ( item != null && item == lastItem[ 0 ] ) {
        boolean isCarbon = SWT.getPlatform().equals( "carbon" );
        final Composite composite = new Composite( wTree, SWT.NONE );
        if ( !isCarbon ) {
          composite.setBackground( shell.getDisplay().getSystemColor( SWT.COLOR_BLACK ) );
        }
        final Text text = new Text( composite, SWT.NONE );
        final int inset = isCarbon ? 0 : 1;
        composite.addListener( SWT.Resize, e -> {
          Rectangle rect = composite.getClientArea();
          text.setBounds( rect.x + inset, rect.y + inset, rect.width - inset * 2, rect.height - inset * 2 );
        } );
        Listener textListener = e -> {
          switch ( e.type ) {
            case SWT.FocusOut:
              if ( text.getText().length() > 0 ) {
                // Check if the name Exists
                if ( getCTabItemByName( text.getText() ) == null ) {
                  modifyCTabItem( item, RENAME_ITEM, text.getText() );
                  item.setText( text.getText() );
                }
              }
              composite.dispose();
              break;
            case SWT.Verify:
              String newText = text.getText();
              String leftText = newText.substring( 0, e.start );
              String rightText = newText.substring( e.end, newText.length() );
              GC gc = new GC( text );
              Point size = gc.textExtent( leftText + e.text + rightText );
              gc.dispose();
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
                    // Check if the name Exists
                    if ( getCTabItemByName( text.getText() ) == null ) {
                      modifyCTabItem( item, RENAME_ITEM, text.getText() );
                      item.setText( text.getText() );
                    }
                  }
                  break;
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

  // This could be useful for further improvements
  public static ScriptNode parseVariables( Context cx, Scriptable scope, String source, String sourceName,
                                           int lineno, Object securityDomain ) {
    // Interpreter compiler = new Interpreter();
    CompilerEnvirons evn = new CompilerEnvirons();
    // evn.setLanguageVersion(Context.VERSION_1_5);
    evn.setOptimizationLevel( -1 );
    evn.setGeneratingSource( true );
    evn.setGenerateDebugInfo( true );
    ErrorReporter errorReporter = new ToolErrorReporter( false );
    Parser p = new Parser( evn, errorReporter );
    ScriptNode tree = p.parse( source, "", 0 ); // IOException
    new NodeTransformer().transform( tree, evn );
    // Script result = (Script)compiler.compile(scope, evn, tree, p.getEncodedSource(),false, null);
    return tree;
  }
}
