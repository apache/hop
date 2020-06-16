package org.apache.hop.ui.core.vfs;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElementType;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.history.AuditList;
import org.apache.hop.history.AuditManager;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.EnterStringDialog;
import org.apache.hop.ui.core.dialog.IFileDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.GuiToolbarWidgets;
import org.apache.hop.ui.core.gui.HopNamespace;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.util.SwtSvgImageUtil;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.List;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeColumn;
import org.eclipse.swt.widgets.TreeItem;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@GuiPlugin
public class HopVfsFileDialog implements IFileDialog {

  private static Class<?> PKG = HopVfsFileDialog.class; // for i18n purposes, needed by Translator!!

  public static final String BOOKMARKS_AUDIT_TYPE = "vfs-bookmarks";

  public static final String BOOKMARKS_TOOLBAR_PARENT_ID = "HopVfsFileDialog-BookmarksToolbar";
  private static final String BOOKMARKS_ITEM_ID_BOOKMARKS = "0000-bookmarks";
  private static final String BOOKMARKS_ITEM_ID_BOOKMARK_GOTO = "0010-bookmark-goto";
  private static final String BOOKMARKS_ITEM_ID_BOOKMARK_REMOVE = "0030-bookmark-remove";

  public static final String BROWSER_TOOLBAR_PARENT_ID = "HopVfsFileDialog-BrowserToolbar";
  private static final String BROWSER_ITEM_ID_NAVIGATE_HOME = "0000-navigate-home";
  private static final String BROWSER_ITEM_ID_NAVIGATE_UP = "0010-navigate-up";
  private static final String BROWSER_ITEM_ID_CREATE_FOLDER = "0020-create-folder";
  private static final String BROWSER_ITEM_ID_BOOKMARK_ADD = "0030-bookmark-add";
  private static final String BROWSER_ITEM_ID_BOOKMARK_SHOW_HIDE = "0040-bookmark-show-hide";
  private static final String BROWSER_ITEM_ID_NAVIGATE_PREVIOUS = "0100-navigation-previous";
  private static final String BROWSER_ITEM_ID_NAVIGATE_NEXT = "0110-navigation-next";
  private static final String BROWSER_ITEM_ID_SHOW_HIDDEN = "0200-show-hidden";

  private Shell parent;
  private IVariables variables;
  private String text;
  private String fileName;
  private String filterPath;
  private String[] filterExtensions;
  private String[] filterNames;

  private PropsUi props;

  private List wBookmarks;
  private TextVar wFilename;

  private Text wDetails;
  private Tree wBrowser;

  private boolean showingHiddenFiles;

  private Shell shell;

  Map<String, FileObject> fileObjectsMap;

  private Map<String, String> bookmarks;
  private FileObject activeFileObject;
  private FileObject activeFolder;

  private Image fileImage;

  private static HopVfsFileDialog instance;
  private FileObject selectedFile;

  private java.util.List<String> navigationHistory;
  private int navigationIndex;

  private GuiToolbarWidgets browserToolbarWidgets;
  private GuiToolbarWidgets bookmarksToolbarWidgets;
  private Button wOk;
  private SashForm sashForm;
  private Combo wFilters;

  public HopVfsFileDialog( Shell parent, IVariables variables, FileObject fileObject ) {
    this.parent = parent;
    this.variables = variables;
    this.fileName = fileName == null ? null : HopVfs.getFilename( fileObject );

    if ( this.variables == null ) {
      this.variables = HopGui.getInstance().getVariables();
    }
    props = PropsUi.getInstance();

    try {
      bookmarks = AuditManager.getActive().loadMap( HopNamespace.getNamespace(), BOOKMARKS_AUDIT_TYPE );
    } catch ( Exception e ) {
      LogChannel.GENERAL.logError( "Error loading bookmarks", e );
      bookmarks = new HashMap<>();
    }

    try {
      AuditList auditList = AuditManager.getActive().retrieveList( HopNamespace.getNamespace(), BOOKMARKS_AUDIT_TYPE );
      navigationHistory = auditList.getNames();
    } catch ( Exception e ) {
      LogChannel.GENERAL.logError( "Error loading navigation history", e );
      navigationHistory = new ArrayList<>();
    }
    navigationIndex = navigationHistory.size() - 1;

    fileImage = SwtSvgImageUtil.getImage( parent.getDisplay(), getClass().getClassLoader(), "ui/images/file.svg", ConstUi.ICON_SIZE, ConstUi.ICON_SIZE );
  }

  /**
   * Gets the active instance of this dialog
   *
   * @return value of instance
   */
  public static HopVfsFileDialog getInstance() {
    return instance;
  }

  public String open() {
    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.APPLICATION_MODAL );
    props.setLook( shell );
    shell.setImage( GuiResource.getInstance().getImageHopUi() );
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );
    instance = this;

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;
    shell.setLayout( formLayout );

    if ( text != null ) {
      shell.setText( text );
    }

    //  At the bottom we have an OK and a Cancel button
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOk.addListener( SWT.Selection, e -> okButton() );

    Button wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection, e -> cancel() );

    BaseTransformDialog.positionBottomButtons( shell, new Button[] { wOk, wCancel }, props.getMargin(), null );

    // Above this we have a sash form
    //
    sashForm = new SashForm( shell, SWT.HORIZONTAL );
    FormData fdSashForm = new FormData();
    fdSashForm.left = new FormAttachment( 0, 0 );
    fdSashForm.top = new FormAttachment( 0, 0 );
    fdSashForm.right = new FormAttachment( 100, 0 );
    fdSashForm.bottom = new FormAttachment( wOk, (int) ( -props.getMargin() * props.getZoomFactor() ) );
    sashForm.setLayoutData( fdSashForm );

    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    // On the left there are the bookmarks
    //
    Composite bookmarksComposite = new Composite( sashForm, SWT.NONE );
    props.setLook( bookmarksComposite );
    bookmarksComposite.setLayout( new FormLayout() );

    // Above the bookmarks a toolbar with edit, add, delete
    //
    ToolBar bookmarksToolBar = new ToolBar( bookmarksComposite, SWT.BORDER | SWT.WRAP | SWT.SHADOW_OUT | SWT.LEFT | SWT.HORIZONTAL );
    FormData fdBookmarksToolBar = new FormData();
    fdBookmarksToolBar.left = new FormAttachment( 0, 0 );
    fdBookmarksToolBar.top = new FormAttachment( 0, 0 );
    fdBookmarksToolBar.right = new FormAttachment( 100, 0 );
    bookmarksToolBar.setLayoutData( fdBookmarksToolBar );
    props.setLook( bookmarksToolBar, Props.WIDGET_STYLE_TOOLBAR );

    bookmarksToolbarWidgets = new GuiToolbarWidgets();
    bookmarksToolbarWidgets.createToolbarWidgets( bookmarksToolBar, BOOKMARKS_TOOLBAR_PARENT_ID );
    bookmarksToolBar.pack();

    // Below that we have a list with all the bookmarks in them
    //
    wBookmarks = new List( bookmarksComposite, SWT.SINGLE | SWT.LEFT | SWT.V_SCROLL | SWT.H_SCROLL );
    props.setLook( wBookmarks );
    FormData fdBookmarks = new FormData();
    fdBookmarks.left = new FormAttachment( 0, 0 );
    fdBookmarks.right = new FormAttachment( 100, 0 );
    fdBookmarks.top = new FormAttachment( 0, bookmarksToolBar.getSize().y );
    fdBookmarks.bottom = new FormAttachment( 100, 0 );
    wBookmarks.setLayoutData( fdBookmarks );
    wBookmarks.addListener( SWT.Selection, e -> refreshStates() );
    wBookmarks.addListener( SWT.DefaultSelection, this::bookmarkDefaultSelection );


    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    // On the right there is a folder and files browser
    //
    Composite browserComposite = new Composite( sashForm, SWT.V_SCROLL | SWT.H_SCROLL );
    props.setLook( browserComposite );
    browserComposite.setLayout( new FormLayout() );

    Label wlFilename = new Label( browserComposite, SWT.SINGLE | SWT.LEFT );
    props.setLook( wlFilename );
    wlFilename.setText( "Filename: " );
    FormData fdlFilename = new FormData();
    fdlFilename.left = new FormAttachment( 0, 0 );
    fdlFilename.top = new FormAttachment( 0, 0 );
    wlFilename.setLayoutData( fdlFilename );

    Button wbFilename = new Button( browserComposite, SWT.PUSH );
    props.setLook( wbFilename );
    wbFilename.setText( "Show" );
    wbFilename.addListener( SWT.Selection, e -> refreshBrowser() );
    FormData fdbFilename = new FormData();
    fdbFilename.right = new FormAttachment( 100, 0 );
    fdbFilename.top = new FormAttachment( 0, 0 );
    wbFilename.setLayoutData( fdbFilename );

    wFilters = new Combo( browserComposite, SWT.SINGLE | SWT.BORDER );
    props.setLook( wFilters );
    FormData fdFilters = new FormData();
    fdFilters.right = new FormAttachment( wbFilename, -props.getMargin() );
    fdFilters.top = new FormAttachment( 0, 0 );
    wFilters.setLayoutData( fdFilters );
    wFilters.setItems( filterNames );

    wFilename = new TextVar( variables, browserComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFilename );
    FormData fdFilename = new FormData();
    fdFilename.left = new FormAttachment( wlFilename, props.getMargin() );
    fdFilename.right = new FormAttachment( wFilters, -props.getMargin() );
    fdFilename.top = new FormAttachment( 0, 0 );
    wFilename.setLayoutData( fdFilename );
    wFilename.addListener( SWT.DefaultSelection, e -> enteredFilenameOrFolder() );

    // A toolbar above the browser, below the filename
    //
    ToolBar browserToolBar = new ToolBar( browserComposite, SWT.BORDER | SWT.WRAP | SWT.SHADOW_OUT | SWT.LEFT | SWT.HORIZONTAL );
    FormData fdBrowserToolBar = new FormData();
    fdBrowserToolBar.left = new FormAttachment( 0, 0 );
    fdBrowserToolBar.top = new FormAttachment( wFilename, props.getMargin() );
    fdBrowserToolBar.right = new FormAttachment( 100, 0 );
    browserToolBar.setLayoutData( fdBrowserToolBar );
    props.setLook( browserToolBar, Props.WIDGET_STYLE_TOOLBAR );

    browserToolbarWidgets = new GuiToolbarWidgets();
    browserToolbarWidgets.createToolbarWidgets( browserToolBar, BROWSER_TOOLBAR_PARENT_ID );
    browserToolBar.pack();

    // Put file details or message/logging label at the bottom...
    //
    wDetails = new Text( browserComposite, SWT.MULTI | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL );
    props.setLook( wDetails );
    FormData fdDetails = new FormData();
    fdDetails.left = new FormAttachment( 0, 0 );
    fdDetails.right = new FormAttachment( 100, 0 );
    fdDetails.bottom = new FormAttachment( 100, 0 );
    fdDetails.top = new FormAttachment( 100, (int) ( -50 * props.getZoomFactor() ) );
    wDetails.setLayoutData( fdDetails );

    wBrowser = new Tree( browserComposite, SWT.SINGLE | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL );
    props.setLook( wBrowser );
    wBrowser.setHeaderVisible( true );
    wBrowser.setLinesVisible( true ); // TODO needed?
    FormData fdBrowser = new FormData();
    fdBrowser.left = new FormAttachment( 0, 0 );
    fdBrowser.right = new FormAttachment( 100, 0 );
    fdBrowser.top = new FormAttachment( wbFilename, browserToolBar.getSize().y + 2 * props.getMargin() );
    fdBrowser.bottom = new FormAttachment( wDetails, -props.getMargin() );
    wBrowser.setLayoutData( fdBrowser );

    TreeColumn folderColumn = new TreeColumn( wBrowser, SWT.LEFT );
    folderColumn.setText( "Directory" );
    folderColumn.setWidth( (int) ( 200 * props.getZoomFactor() ) );

    TreeColumn fileColumn = new TreeColumn( wBrowser, SWT.LEFT );
    fileColumn.setText( "Filename" );
    fileColumn.setWidth( (int) ( 400 * props.getZoomFactor() ) );

    TreeColumn sizeColumn = new TreeColumn( wBrowser, SWT.LEFT );
    sizeColumn.setText( "Size" );
    sizeColumn.setWidth( (int) ( 150 * props.getZoomFactor() ) );

    TreeColumn modifiedColumn = new TreeColumn( wBrowser, SWT.LEFT );
    modifiedColumn.setText( "Modified" );
    modifiedColumn.setWidth( (int) ( 150 * props.getZoomFactor() ) );

    wBrowser.addListener( SWT.Selection, this::fileSelection );
    wBrowser.addListener( SWT.DefaultSelection, this::fileDefaultSelection );

    sashForm.setWeights( new int[] { 15, 85 } );

    getData();

    BaseTransformDialog.setSize( shell );

    // The shell size usually ends up a bit too narrow so let's make it a bit higher
    //
    Point shellSize = shell.getSize();
    if ( shellSize.y < shellSize.x / 2 ) {
      shell.setSize( shellSize.x, shellSize.x / 2 );
    }

    shell.open();

    while ( !shell.isDisposed() ) {
      if ( !shell.getDisplay().readAndDispatch() ) {
        shell.getDisplay().sleep();
      }
    }

    if ( activeFileObject == null ) {
      return null;
    }
    return activeFileObject.toString();
  }

  @GuiToolbarElement(
    root = BOOKMARKS_TOOLBAR_PARENT_ID,
    id = BOOKMARKS_ITEM_ID_BOOKMARK_GOTO,
    toolTip = "Browse to the selected bookmark",
    image = "ui/images/toolbar/arrow-right.svg"
  )
  public void browseToSelectedBookmark() {
    String name = getSelectedBookmark();
    if ( name == null ) {
      return;
    }
    String path = bookmarks.get( name );
    if ( path != null ) {
      navigateTo( path, true );
    }
  }

  private String getSelectedBookmark() {
    int selectionIndex = wBookmarks.getSelectionIndex();
    if ( selectionIndex < 0 ) {
      return null;
    }
    String name = wBookmarks.getItems()[ selectionIndex ];
    return name;
  }

  /**
   * User double clicked on a bookmark
   *
   * @param event
   */
  private void bookmarkDefaultSelection( Event event ) {
    browseToSelectedBookmark();
  }

  private void okButton() {
    try {
      activeFileObject = HopVfs.getFileObject( wFilename.getText() );
      ok();
    } catch ( Exception e ) {
      wDetails.setText( "Error parsing filename: '" + wFilename.getText() + Const.CR + Const.getSimpleStackTrace( e ) );
    }
  }


  private void enteredFilenameOrFolder() {
    // Simply refresh the browser
    refreshBrowser();
  }

  private FileObject getSelectedFileObject() {
    TreeItem[] selection = wBrowser.getSelection();
    if ( selection == null || selection.length != 1 ) {
      return null;
    }

    String path = getTreeItemPath( selection[ 0 ] );
    FileObject fileObject = fileObjectsMap.get( path );
    return fileObject;
  }

  /**
   * Something is selected in the browser
   *
   * @param e
   */
  private void fileSelection( Event e ) {
    selectedFile = getSelectedFileObject();
  }

  /**
   * Double clicked on a file or folder
   *
   * @param event
   */
  private void fileDefaultSelection( Event event ) {
    FileObject fileObject = getSelectedFileObject();
    if ( fileObject == null ) {
      return;
    }

    try {
      navigateTo( HopVfs.getFilename( fileObject ), true );

      if ( fileObject.isFolder() ) {
        // Browse into the selected folder...
        //
        refreshBrowser();
      } else {
        // Take this file as the user choice for this dialog
        //
        okButton();
      }
    } catch ( Exception e ) {
      wDetails.setText( "Error handling default selection on file " + fileObject.toString() + Const.CR + Const.getSimpleStackTrace( e ) );
    }

  }

  private void getData() {

    // Take the first by default: All types
    //
    wFilters.select( 0 );

    refreshBookmarks();

    if ( fileName == null ) {
      if ( filterPath != null ) {
        fileName = filterPath;
      } else {
        // Default to the user home directory
        //
        fileName = System.getProperty( "user.home" );
      }
    }
    navigateTo( fileName, true );
  }

  private void refreshBookmarks() {
    // Add the bookmarks
    //
    java.util.List<String> bookmarkNames = new ArrayList<>( bookmarks.keySet() );
    Collections.sort( bookmarkNames );
    wBookmarks.setItems( bookmarkNames.toArray( new String[ 0 ] ) );
  }

  private void refreshBrowser() {
    String filename = wFilename.getText();
    if ( StringUtils.isEmpty( filename ) ) {
      return;
    }

    // Browse to the selected file location...
    //
    try {
      activeFileObject = HopVfs.getFileObject( filename, variables );
      if ( activeFileObject.isFolder() ) {
        activeFolder = activeFileObject;
      } else {
        activeFolder = activeFileObject.getParent();
      }
      wBrowser.removeAll();

      fileObjectsMap = new HashMap<>();

      TreeItem parentFolderItem = new TreeItem( wBrowser, SWT.NONE );
      parentFolderItem.setImage( GuiResource.getInstance().getImageFolder() );
      parentFolderItem.setText( activeFolder.getName().getBaseName() );
      fileObjectsMap.put( getTreeItemPath( parentFolderItem ), activeFolder );

      populateFolder( activeFolder, parentFolderItem );

      parentFolderItem.setExpanded( true );
    } catch ( Exception e ) {
      wDetails.setText( "Error browsing to location: " + filename + Const.CR + Const.NVL( Const.getSimpleStackTrace( e ), "" ) );
    }

  }

  private String getTreeItemPath( TreeItem item ) {
    String path = "/" + item.getText();
    TreeItem parentItem = item.getParentItem();
    while ( parentItem != null ) {
      path = "/" + parentItem.getText() + path;
      parentItem = parentItem.getParentItem();
    }
    return path;
  }

  /**
   * Child folders are always shown at the top. Files below it sorted alphabetically
   *
   * @param folder
   * @param folderItem
   */
  private void populateFolder( FileObject folder, TreeItem folderItem ) throws FileSystemException {

    FileObject[] children = folder.getChildren();
    Arrays.sort( children, Comparator.comparing( o -> o.getName().getBaseName() ) );

    // First the child folders
    //
    for ( FileObject child : children ) {
      if ( child.isFolder() ) {
        String baseFilename = child.getName().getBaseName();
        if (!showingHiddenFiles && baseFilename.startsWith( "." )) {
          continue;
        }
        TreeItem childFolderItem = new TreeItem( folderItem, SWT.NONE );
        childFolderItem.setImage( GuiResource.getInstance().getImageFolder() );
        childFolderItem.setText( child.getName().getBaseName() );
        fileObjectsMap.put( getTreeItemPath( childFolderItem ), child );
      }
    }
    for ( final FileObject child : children ) {
      if ( child.isFolder() ) {
        String baseFilename = child.getName().getBaseName();
        if (!showingHiddenFiles && baseFilename.startsWith( "." )) {
          continue;
        }

        boolean selectFile = false;

        // Check file extension...
        //
        String selectedExtensions = filterExtensions[wFilters.getSelectionIndex()];
        String[] exts = selectedExtensions.split( ";" );
        for (String ext : exts) {
          if ( FilenameUtils.wildcardMatch( baseFilename, ext ) ) {
            selectFile = true;
          }
        }

        // Hidden file?
        //
        if (selectFile) {
          TreeItem childFileItem = new TreeItem( folderItem, SWT.NONE );
          childFileItem.setImage( fileImage );
          childFileItem.setFont( GuiResource.getInstance().getFontBold() );
          childFileItem.setText( 1, child.getName().getBaseName() );
          childFileItem.setText( 2, getFileSize( child ) );
          childFileItem.setText( 3, getFileDate( child ) );
          fileObjectsMap.put( getTreeItemPath( childFileItem ), child );

          if ( child.equals( activeFileObject ) ) {
            wBrowser.setSelection( childFileItem );
            wBrowser.showSelection();
          }
        }
      }
    }

  }

  private void fileSelected( FileObject fileItem ) {
    System.out.println( "File selected in browser : " + fileItem.toString() );
  }

  private String getFileSize( FileObject child ) {
    try {
      long size = child.getContent().getSize();
      String[] units = { "", "kB", "MB", "GB", "TB", "PB", "XB", "YB", "ZB" };
      for ( int i = 0; i < units.length; i++ ) {
        double unitSize = Math.pow( 1024, i );
        double maxSize = Math.pow( 1024, i + 1 );
        if ( size < maxSize ) {
          return new DecimalFormat( "0.#" ).format( size / unitSize ) + units[ i ];
        }
      }
      return Long.toString( size );
    } catch ( Exception e ) {
      LogChannel.GENERAL.logError( "Error getting size of file : " + child.toString(), e );
      return "?";
    }
  }

  private String getFileDate( FileObject child ) {
    try {
      long lastModifiedTime = child.getContent().getLastModifiedTime();
      return new SimpleDateFormat( "yyyy/MM/dd HH:mm:ss" ).format( new Date( lastModifiedTime ) );
    } catch ( Exception e ) {
      LogChannel.GENERAL.logError( "Error getting last modified date of file : " + child.toString(), e );
      return "?";
    }
  }

  private void cancel() {
    activeFileObject = null;
    dispose();
  }

  private void ok() {
    try {
      filterPath = activeFileObject.getParent().getName().getPath();
      fileName = activeFileObject.getName().getBaseName();
      dispose();
    } catch ( FileSystemException e ) {
      wDetails.setText( "Error finding parent folder of file: '" + activeFileObject.toString() + Const.CR + Const.getSimpleStackTrace( e ) );
    }
  }

  private void dispose() {
    instance = null;
    try {
      AuditManager.getActive().storeList( HopNamespace.getNamespace(), BOOKMARKS_AUDIT_TYPE, new AuditList( navigationHistory ) );
    } catch ( Exception e ) {
      LogChannel.GENERAL.logError( "Error storing navigation history", e );
    }
    props.setScreen( new WindowProperty( shell ) );
    shell.dispose();
  }

  @GuiToolbarElement(
    root = BOOKMARKS_TOOLBAR_PARENT_ID,
    id = BOOKMARKS_ITEM_ID_BOOKMARKS,
    label = "Bookmarks:",
    type = GuiToolbarElementType.LABEL
  )
  public void labelBookmark() {
    // nothing specific to do here, just a label
  }


  @GuiToolbarElement(
    root = BROWSER_TOOLBAR_PARENT_ID,
    id = BROWSER_ITEM_ID_BOOKMARK_ADD,
    toolTip = "Add the selected file or folder as a new bookmark",
    image = "ui/images/bookmark-add.svg"
  )
  public void addBookmark() {
    if ( selectedFile != null ) {
      String name = selectedFile.getName().getBaseName();
      EnterStringDialog dialog = new EnterStringDialog( shell, name, "Enter bookmark", "Please enter the name for this bookmark" );
      name = dialog.open();
      if ( name != null ) {
        String path = HopVfs.getFilename( selectedFile );
        bookmarks.put( name, path );
        saveBookmarks();
        refreshBookmarks();
      }
    }
    refreshStates();
  }

  private boolean bookmarksShown = true;

  @GuiToolbarElement(
    root = BOOKMARKS_TOOLBAR_PARENT_ID,
    id = BOOKMARKS_ITEM_ID_BOOKMARK_REMOVE,
    toolTip = "Remove the selected bookmark",
    image = "ui/images/deleteSmall.svg"
  )
  public void removeBookmark() {
    String name = getSelectedBookmark();
    if ( name != null ) {
      bookmarks.remove( name );
      saveBookmarks();
      refreshBookmarks();
    }
    refreshStates();
  }

  private void saveBookmarks() {
    try {
      AuditManager.getActive().saveMap( HopNamespace.getNamespace(), BOOKMARKS_AUDIT_TYPE, bookmarks );
    } catch ( Exception e ) {
      wDetails.setText( "Error saving bookmarks: '" + activeFileObject.toString() + Const.CR + Const.getSimpleStackTrace( e ) );
    }
  }

  public void navigateTo( String filename, boolean saveHistory ) {
    if ( saveHistory ) {
      // Add to navigation history
      //
      if ( navigationIndex >= 0 ) {
        if ( navigationIndex < navigationHistory.size() ) {
          // Clear history above the index...
          //
          navigationHistory.subList( navigationIndex, navigationHistory.size() );
        }
      }

      navigationHistory.add( filename );
      navigationIndex = navigationHistory.size() - 1;
    }

    wFilename.setText( filename );
    refreshBrowser();
    refreshStates();
  }

  @GuiToolbarElement(
    root = BROWSER_TOOLBAR_PARENT_ID,
    id = BROWSER_ITEM_ID_NAVIGATE_HOME,
    toolTip = "Navigate to the user home directory",
    image = "ui/images/home-enabled.svg"
  )
  public void navigateHome() {
    navigateTo( System.getProperty( "user.home" ), true );
  }

  @GuiToolbarElement(
    root = BROWSER_TOOLBAR_PARENT_ID,
    id = BROWSER_ITEM_ID_NAVIGATE_UP,
    toolTip = "Navigate to the parent folder",
    image = "ui/images/9x9_arrow_up.svg"
  )
  public void navigateUp() {
    try {
      FileObject fileObject = HopVfs.getFileObject( wFilename.getText() );
      if ( fileObject.isFile() ) {
        fileObject = fileObject.getParent();
      }
      FileObject parent = fileObject.getParent();
      if ( parent != null ) {
        navigateTo( HopVfs.getFilename( parent ), true );
      }
    } catch ( Exception e ) {
      wDetails.setText( "Error navigating up: '" + activeFileObject.toString() + Const.CR + Const.getSimpleStackTrace( e ) );
    }
  }


/*
  @GuiToolbarElement(
    root = BROWSER_TOOLBAR_PARENT_ID,
    id = BROWSER_ITEM_ID_CREATE_FOLDER,
    toolTip = "Create folder",
    image = "ui/images/new.svg"
  )
  public void newFolder() {

  }

 */

  @GuiToolbarElement(
    root = BROWSER_TOOLBAR_PARENT_ID,
    id = BROWSER_ITEM_ID_NAVIGATE_PREVIOUS,
    toolTip = "Navigate to previous path from your history",
    image = "ui/images/toolbar/arrow-left.svg",
    separator = true
  )
  public void navigateHistoryPrevious() {
    if ( navigationIndex - 1 >= 0 ) {
      navigationIndex--;
      navigateTo( navigationHistory.get( navigationIndex ), false );
    }
  }

  @GuiToolbarElement(
    root = BROWSER_TOOLBAR_PARENT_ID,
    id = BROWSER_ITEM_ID_NAVIGATE_NEXT,
    toolTip = "Navigate to next path from your history",
    image = "ui/images/toolbar/arrow-right.svg"
  )
  public void navigateHistoryNext() {
    if ( navigationIndex + 1 < navigationHistory.size() - 1 ) {
      navigationIndex++;
      navigateTo( navigationHistory.get( navigationIndex ), false );
    }
  }

  @GuiToolbarElement(
    root = BROWSER_TOOLBAR_PARENT_ID,
    id = BROWSER_ITEM_ID_SHOW_HIDDEN,
    toolTip = "Show or hide hidden files and directories",
    image = "ui/images/toolbar/view.svg",
    separator = true
  )
  public void showHideHidden() {
    showingHiddenFiles=!showingHiddenFiles;
    refreshBrowser();
  }

  private void refreshStates() {
    // Navigation icons...
    //
    browserToolbarWidgets.enableToolbarItem( BROWSER_ITEM_ID_NAVIGATE_PREVIOUS, navigationIndex > 0 );
    browserToolbarWidgets.enableToolbarItem( BROWSER_ITEM_ID_NAVIGATE_NEXT, navigationIndex + 1 < navigationHistory.size() );

    // Up...
    //
    boolean canGoUp;
    try {
      FileObject fileObject = HopVfs.getFileObject( wFilename.getText() );
      if ( fileObject.isFile() ) {
        canGoUp = fileObject.getParent().getParent() != null;
      } else {
        canGoUp = fileObject.getParent() != null;
      }
    } catch ( Exception e ) {
      canGoUp = false;
    }
    browserToolbarWidgets.enableToolbarItem( BROWSER_ITEM_ID_NAVIGATE_UP, canGoUp );

    // Bookmarks...
    //
    boolean bookmarkSelected = getSelectedBookmark() != null;
    bookmarksToolbarWidgets.enableToolbarItem( BOOKMARKS_ITEM_ID_BOOKMARK_GOTO, bookmarkSelected );
    bookmarksToolbarWidgets.enableToolbarItem( BOOKMARKS_ITEM_ID_BOOKMARK_REMOVE, bookmarkSelected );

    wOk.setEnabled( StringUtils.isNotEmpty( wFilename.getText() ) );
  }


  /**
   * Gets text
   *
   * @return value of text
   */
  public String getText() {
    return text;
  }

  /**
   * @param text The text to set
   */
  public void setText( String text ) {
    this.text = text;
  }

  /**
   * Gets variables
   *
   * @return value of variables
   */
  public IVariables getVariables() {
    return variables;
  }

  /**
   * @param variables The variables to set
   */
  public void setVariables( IVariables variables ) {
    this.variables = variables;
  }

  /**
   * Gets fileName
   *
   * @return value of fileName
   */
  @Override public String getFileName() {
    return fileName;
  }

  /**
   * @param fileName The fileName to set
   */
  @Override public void setFileName( String fileName ) {
    this.fileName = fileName;
  }

  /**
   * Gets filterExtensions
   *
   * @return value of filterExtensions
   */
  public String[] getFilterExtensions() {
    return filterExtensions;
  }

  /**
   * @param filterExtensions The filterExtensions to set
   */
  public void setFilterExtensions( String[] filterExtensions ) {
    this.filterExtensions = filterExtensions;
  }

  /**
   * Gets filterNames
   *
   * @return value of filterNames
   */
  public String[] getFilterNames() {
    return filterNames;
  }

  /**
   * @param filterNames The filterNames to set
   */
  public void setFilterNames( String[] filterNames ) {
    this.filterNames = filterNames;
  }


  /**
   * Gets bookmarks
   *
   * @return value of bookmarks
   */
  public Map<String, String> getBookmarks() {
    return bookmarks;
  }

  /**
   * @param bookmarks The bookmarks to set
   */
  public void setBookmarks( Map<String, String> bookmarks ) {
    this.bookmarks = bookmarks;
  }

  /**
   * Gets activeFileObject
   *
   * @return value of activeFileObject
   */
  public FileObject getActiveFileObject() {
    return activeFileObject;
  }

  /**
   * @param activeFileObject The activeFileObject to set
   */
  public void setActiveFileObject( FileObject activeFileObject ) {
    this.activeFileObject = activeFileObject;
  }

  /**
   * Gets activeFolder
   *
   * @return value of activeFolder
   */
  public FileObject getActiveFolder() {
    return activeFolder;
  }

  /**
   * @param activeFolder The activeFolder to set
   */
  public void setActiveFolder( FileObject activeFolder ) {
    this.activeFolder = activeFolder;
  }

  /**
   * Gets filterPath
   *
   * @return value of filterPath
   */
  @Override public String getFilterPath() {
    return filterPath;
  }

  /**
   * @param filterPath The filterPath to set
   */
  public void setFilterPath( String filterPath ) {
    this.filterPath = filterPath;
  }

  /**
   * Gets showingHiddenFiles
   *
   * @return value of showingHiddenFiles
   */
  public boolean isShowingHiddenFiles() {
    return showingHiddenFiles;
  }

  /**
   * @param showingHiddenFiles The showingHiddenFiles to set
   */
  public void setShowingHiddenFiles( boolean showingHiddenFiles ) {
    this.showingHiddenFiles = showingHiddenFiles;
  }
}
