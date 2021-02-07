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

package org.apache.hop.ui.core.vfs;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileContent;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.history.AuditList;
import org.apache.hop.history.AuditManager;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.EnterStringDialog;
import org.apache.hop.ui.core.dialog.IDirectoryDialog;
import org.apache.hop.ui.core.dialog.IFileDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.GuiToolbarWidgets;
import org.apache.hop.ui.core.gui.HopNamespace;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.core.widget.TreeUtil;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.HopFileTypePluginType;
import org.apache.hop.ui.hopgui.file.HopFileTypeRegistry;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.dnd.DND;
import org.eclipse.swt.dnd.DragSource;
import org.eclipse.swt.dnd.DragSourceAdapter;
import org.eclipse.swt.dnd.DragSourceEvent;
import org.eclipse.swt.dnd.DropTarget;
import org.eclipse.swt.dnd.DropTargetAdapter;
import org.eclipse.swt.dnd.DropTargetEvent;
import org.eclipse.swt.dnd.TextTransfer;
import org.eclipse.swt.events.MenuAdapter;
import org.eclipse.swt.events.MenuEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.List;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.MenuItem;
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
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@GuiPlugin(description = "Allows you to browse to local or VFS locations")
public class HopVfsFileDialog implements IFileDialog, IDirectoryDialog {

  private static final Class<?> PKG = HopVfsFileDialog.class; // For Translator

  public static final String BOOKMARKS_AUDIT_TYPE = "vfs-bookmarks";

  public static final String BOOKMARKS_TOOLBAR_PARENT_ID = "HopVfsFileDialog-BookmarksToolbar";
  private static final String BOOKMARKS_ITEM_ID_BOOKMARKS = "0000-bookmarks";
  private static final String BOOKMARKS_ITEM_ID_BOOKMARK_ADD = "0010-bookmark-add";
  private static final String BOOKMARKS_ITEM_ID_BOOKMARK_GOTO = "0020-bookmark-goto";
  private static final String BOOKMARKS_ITEM_ID_BOOKMARK_REMOVE = "0030-bookmark-remove";

  public static final String NAVIGATE_TOOLBAR_PARENT_ID = "HopVfsFileDialog-NavigateToolbar";
  private static final String NAVIGATE_ITEM_ID_NAVIGATE_HOME = "0000-navigate-home";
  private static final String NAVIGATE_ITEM_ID_NAVIGATE_UP = "0010-navigate-up";
  private static final String NAVIGATE_ITEM_ID_NAVIGATE_PREVIOUS = "0100-navigate-previous";
  private static final String NAVIGATE_ITEM_ID_NAVIGATE_NEXT = "0110-navigate-next";
  private static final String NAVIGATE_ITEM_ID_REFRESH_ALL = "9999-refresh-all";

  public static final String BROWSER_TOOLBAR_PARENT_ID = "HopVfsFileDialog-BrowserToolbar";
  private static final String BROWSER_ITEM_ID_CREATE_FOLDER = "0020-create-folder";
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

  private Image folderImage;
  private Image fileImage;

  private static HopVfsFileDialog instance;
  private FileObject selectedFile;

  private java.util.List<String> navigationHistory;
  private int navigationIndex;

  private GuiToolbarWidgets navigateToolbarWidgets;
  private GuiToolbarWidgets browserToolbarWidgets;
  private GuiToolbarWidgets bookmarksToolbarWidgets;
  private Button wOk;
  private SashForm sashForm;
  private Combo wFilters;
  private String message;

  private boolean browsingDirectories;
  private boolean savingFile;
  private String saveFilename;

  private int sortIndex = 0;
  private boolean ascending = true;

  private String usedNamespace;

  public HopVfsFileDialog() {}

  public HopVfsFileDialog(
      Shell parent,
      IVariables variables,
      FileObject fileObject,
      boolean browsingDirectories,
      boolean savingFile) {
    this.parent = parent;
    this.variables = variables;
    this.browsingDirectories = browsingDirectories;
    this.savingFile = savingFile;

    this.fileName = fileName == null ? null : HopVfs.getFilename(fileObject);

    if (this.variables == null) {
      this.variables = HopGui.getInstance().getVariables();
    }
    props = PropsUi.getInstance();

    if (props.useGlobalFileBookmarks()) {
      usedNamespace = HopGui.DEFAULT_HOP_GUI_NAMESPACE;
    } else {
      usedNamespace = HopNamespace.getNamespace();
    }

    try {

      bookmarks =
          AuditManager.getActive().loadMap(usedNamespace, BOOKMARKS_AUDIT_TYPE);
    } catch (Exception e) {
      LogChannel.GENERAL.logError("Error loading bookmarks", e);
      bookmarks = new HashMap<>();
    }

    try {
      AuditList auditList =
          AuditManager.getActive().retrieveList(usedNamespace, BOOKMARKS_AUDIT_TYPE);
      navigationHistory = auditList.getNames();
    } catch (Exception e) {
      LogChannel.GENERAL.logError("Error loading navigation history", e);
      navigationHistory = new ArrayList<>();
    }
    navigationIndex = navigationHistory.size() - 1;

    fileImage = GuiResource.getInstance().getImageFile();
    folderImage = GuiResource.getInstance().getImageFolder();
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
    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.APPLICATION_MODAL);
    props.setLook(shell);
    shell.setImage(GuiResource.getInstance().getImageHopUi());
    shell.addShellListener(
        new ShellAdapter() {
          public void shellClosed(ShellEvent e) {
            cancel();
          }
        });
    instance = this;

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;
    shell.setLayout(formLayout);

    if (text != null) {
      shell.setText(text);
    }

    //  At the bottom we have an OK and a Cancel button
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(
        BaseMessages.getString(PKG, (savingFile) ? "System.Button.Save" : "System.Button.Open"));
    wOk.addListener(SWT.Selection, e -> okButton());

    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());

    BaseTransformDialog.positionBottomButtons(
        shell, new Button[] {wOk, wCancel}, props.getMargin(), null);

    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    // On top there are the navigation
    //
    Composite navigateComposite = new Composite(shell, SWT.NONE);
    props.setLook(navigateComposite);
    GridLayout gridLayout = new GridLayout((browsingDirectories) ? 2 : 3, false);
    gridLayout.marginWidth = 0;
    navigateComposite.setLayout(gridLayout);

    FormData fdNavigationForm = new FormData();
    fdNavigationForm.left = new FormAttachment(0, 0);
    fdNavigationForm.top = new FormAttachment(0, 0);
    fdNavigationForm.right = new FormAttachment(100, 0);
    navigateComposite.setLayoutData(fdNavigationForm);

    // A toolbar above the browser, below the filename
    //
    ToolBar navigateToolBar = new ToolBar(navigateComposite, SWT.LEFT | SWT.HORIZONTAL);
    navigateToolBar.setLayoutData(new GridData(SWT.LEFT, SWT.FILL, false, true));
    // Force white color
    navigateToolBar.setBackground(GuiResource.getInstance().getColorWhite());

    navigateToolbarWidgets = new GuiToolbarWidgets();
    navigateToolbarWidgets.registerGuiPluginObject(this);
    navigateToolbarWidgets.createToolbarWidgets(navigateToolBar, NAVIGATE_TOOLBAR_PARENT_ID);
    navigateToolBar.pack();

    wFilename = new TextVar(variables, navigateComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wFilename.addListener(SWT.DefaultSelection, e -> enteredFilenameOrFolder());
    wFilename.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
    props.setLook(wFilename);

    if (!browsingDirectories) {
      wFilters = new Combo(navigateComposite, SWT.SINGLE | SWT.BORDER);
      wFilters.setItems(filterNames);
      wFilters.select(0);
      wFilters.addListener(SWT.Selection, this::fileFilterSelected);
      wFilters.setLayoutData(new GridData(SWT.RIGHT, SWT.FILL, false, false));
      props.setLook(wFilters);
    }

    // Above this we have a sash form
    //
    sashForm = new SashForm(shell, SWT.HORIZONTAL);
    FormData fdSashForm = new FormData();
    fdSashForm.left = new FormAttachment(0, 0);
    fdSashForm.top = new FormAttachment(navigateComposite, props.getMargin());
    fdSashForm.right = new FormAttachment(100, 0);
    fdSashForm.bottom = new FormAttachment(wOk, (int) (-props.getMargin() * props.getZoomFactor()));
    sashForm.setLayoutData(fdSashForm);

    props.setLook(sashForm);

    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    // On the left there are the bookmarks
    //
    Composite bookmarksComposite = new Composite(sashForm, SWT.BORDER);
    props.setLook(bookmarksComposite);
    bookmarksComposite.setLayout(new FormLayout());

    // Above the bookmarks a toolbar with add, delete
    //
    ToolBar bookmarksToolBar =
        new ToolBar(bookmarksComposite, SWT.WRAP | SWT.SHADOW_IN | SWT.LEFT | SWT.HORIZONTAL);
    FormData fdBookmarksToolBar = new FormData();
    fdBookmarksToolBar.left = new FormAttachment(0, 0);
    fdBookmarksToolBar.top = new FormAttachment(0, 0);
    fdBookmarksToolBar.right = new FormAttachment(100, 0);
    bookmarksToolBar.setLayoutData(fdBookmarksToolBar);
    props.setLook(bookmarksToolBar, Props.WIDGET_STYLE_TOOLBAR);

    bookmarksToolbarWidgets = new GuiToolbarWidgets();
    bookmarksToolbarWidgets.registerGuiPluginObject(this);
    bookmarksToolbarWidgets.createToolbarWidgets(bookmarksToolBar, BOOKMARKS_TOOLBAR_PARENT_ID);
    bookmarksToolBar.pack();

    // Below that we have a list with all the bookmarks in them
    //
    wBookmarks = new List(bookmarksComposite, SWT.SINGLE | SWT.LEFT | SWT.V_SCROLL | SWT.H_SCROLL);
    props.setLook(wBookmarks);
    FormData fdBookmarks = new FormData();
    fdBookmarks.left = new FormAttachment(0, 0);
    fdBookmarks.right = new FormAttachment(100, 0);
    fdBookmarks.top = new FormAttachment(0, bookmarksToolBar.getSize().y);
    fdBookmarks.bottom = new FormAttachment(100, 0);
    wBookmarks.setLayoutData(fdBookmarks);
    wBookmarks.addListener(SWT.Selection, e -> refreshStates());
    wBookmarks.addListener(SWT.DefaultSelection, this::bookmarkDefaultSelection);

    // Context menu for bookmarks
    //
    final Menu menu = new Menu(wBookmarks);
    menu.addMenuListener(
        new MenuAdapter() {
          public void menuShown(MenuEvent event) {
            MenuItem[] items = menu.getItems();
            for (int i = 0; i < items.length; i++) {
              items[i].dispose();
            }

            int selected = wBookmarks.getSelectionIndex();

            if (selected < 0 || selected >= wBookmarks.getItemCount()) return;
            MenuItem removeBookmarkMenuItem = new MenuItem(menu, SWT.NONE);
            removeBookmarkMenuItem.setText("Delete");
            removeBookmarkMenuItem.addListener(SWT.Selection, e -> removeBookmark());
          }
        });
    wBookmarks.setMenu(menu);

    // Drag and drop to bookmarks
    //
    DropTarget target = new DropTarget(wBookmarks, DND.DROP_MOVE);
    target.setTransfer(TextTransfer.getInstance());
    target.addDropListener(
        new DropTargetAdapter() {
          @Override
          public void dragEnter(DropTargetEvent event) {
            if (event.detail == DND.DROP_DEFAULT) {
              event.detail =
                  (event.operations & DND.DROP_COPY) != 0 ? DND.DROP_COPY : DND.DROP_NONE;
            }

            // Allow dropping text only
            for (int i = 0, n = event.dataTypes.length; i < n; i++) {
              if (TextTransfer.getInstance().isSupportedType(event.dataTypes[i])) {
                event.currentDataType = event.dataTypes[i];
              }
            }
          }

          @Override
          public void dragOver(DropTargetEvent event) {
            event.feedback = DND.FEEDBACK_SELECT | DND.FEEDBACK_SCROLL;
          }

          @Override
          public void drop(DropTargetEvent event) {
            if (TextTransfer.getInstance().isSupportedType(event.currentDataType)) {
              FileObject file = fileObjectsMap.get((String) event.data);
              if (file != null) {
                String name = file.getName().getBaseName();
                EnterStringDialog dialog =
                    new EnterStringDialog(
                        shell, name, "Enter bookmark", "Please enter the name for this bookmark");
                name = dialog.open();
                if (name != null) {
                  String path = HopVfs.getFilename(file);
                  bookmarks.put(name, path);
                  saveBookmarks();
                  refreshBookmarks();
                }
              }
              refreshStates();
            }
          }
        });

    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    // On the right there is a folder and files browser
    //
    Composite browserComposite = new Composite(sashForm, SWT.BORDER);
    props.setLook(browserComposite);
    browserComposite.setLayout(new FormLayout());

    FormData fdTreeComposite = new FormData();
    fdTreeComposite.left = new FormAttachment(0, 0);
    fdTreeComposite.right = new FormAttachment(100, 0);
    fdTreeComposite.top = new FormAttachment(0, 0);
    fdTreeComposite.bottom = new FormAttachment(100, 0);
    browserComposite.setLayoutData(fdTreeComposite);

    // A toolbar above the browser, below the filename
    //
    ToolBar browserToolBar = new ToolBar(browserComposite, SWT.WRAP | SWT.LEFT | SWT.HORIZONTAL);
    FormData fdBrowserToolBar = new FormData();
    fdBrowserToolBar.left = new FormAttachment(0, 0);
    fdBrowserToolBar.top = new FormAttachment(0, 0);
    fdBrowserToolBar.right = new FormAttachment(100, 0);
    browserToolBar.setLayoutData(fdBrowserToolBar);
    props.setLook(browserToolBar, Props.WIDGET_STYLE_TOOLBAR);

    browserToolbarWidgets = new GuiToolbarWidgets();
    browserToolbarWidgets.registerGuiPluginObject(this);
    browserToolbarWidgets.createToolbarWidgets(browserToolBar, BROWSER_TOOLBAR_PARENT_ID);
    browserToolBar.pack();

    SashForm browseSash = new SashForm(browserComposite, SWT.VERTICAL);

    wBrowser = new Tree(browseSash, SWT.SINGLE | SWT.H_SCROLL | SWT.V_SCROLL);
    props.setLook(wBrowser);
    wBrowser.setHeaderVisible(true);
    wBrowser.setLinesVisible(false); // TODO needed?

    TreeColumn folderColumn = new TreeColumn(wBrowser, SWT.LEFT);
    folderColumn.setText("Name");
    folderColumn.setWidth((int) (200 * props.getZoomFactor()));
    folderColumn.addListener(SWT.Selection, e -> browserColumnSelected(0));

    TreeColumn modifiedColumn = new TreeColumn(wBrowser, SWT.LEFT);
    modifiedColumn.setText("Modified");
    modifiedColumn.setWidth((int) (150 * props.getZoomFactor()));
    modifiedColumn.addListener(SWT.Selection, e -> browserColumnSelected(1));

    TreeColumn sizeColumn = new TreeColumn(wBrowser, SWT.RIGHT);
    sizeColumn.setText("Size");
    sizeColumn.setWidth((int) (100 * props.getZoomFactor()));
    sizeColumn.addListener(SWT.Selection, e -> browserColumnSelected(2));

    wBrowser.addListener(SWT.Selection, this::fileSelected);
    wBrowser.addListener(SWT.DefaultSelection, this::fileDefaultSelected);
    wBrowser.addListener(SWT.Resize, e -> resizeTableColumn());

    // Create the drag source on the tree
    DragSource dragSource = new DragSource(wBrowser, DND.DROP_COPY | DND.DROP_MOVE);
    dragSource.setTransfer(TextTransfer.getInstance());
    dragSource.addDragListener(
        new DragSourceAdapter() {

          public void dragStart(DragSourceEvent event) {

            TreeItem[] selection = wBrowser.getSelection();
            if (selection != null && selection.length == 1) {
              event.doit = true;
            } else event.doit = false;
          }

          public void dragSetData(DragSourceEvent event) {
            // Set the data to be the first selected item's text
            TreeItem[] selection = wBrowser.getSelection();
            event.data = getTreeItemPath(selection[0]);
          }
        });

    // Put file details or message/logging label at the bottom...
    //
    wDetails = new Text(browseSash, SWT.MULTI | SWT.H_SCROLL | SWT.V_SCROLL | SWT.READ_ONLY);
    props.setLook(wDetails);

    FormData fdBrowseSash = new FormData();
    fdBrowseSash.left = new FormAttachment(0, 0);
    fdBrowseSash.right = new FormAttachment(100, 0);
    fdBrowseSash.top = new FormAttachment(browserToolBar, 0);
    fdBrowseSash.bottom = new FormAttachment(100, 0);

    browseSash.setLayoutData(fdBrowseSash);
    browseSash.setWeights(new int[] {90, 10});

    sashForm.setWeights(new int[] {15, 85});

    getData();

    BaseTransformDialog.setSize(shell);

    // The shell size usually ends up a bit too narrow so let's make it a bit higher
    //
    Point shellSize = shell.getSize();
    if (shellSize.y < shellSize.x / 2) {
      shell.setSize(shellSize.x, shellSize.x / 2);
    }

    shell.open();

    while (!shell.isDisposed()) {
      if (!shell.getDisplay().readAndDispatch()) {
        shell.getDisplay().sleep();
      }
    }

    if (activeFileObject == null) {
      return null;
    }
    return activeFileObject.toString();
  }

  private void browserColumnSelected(final int index) {
    if (index == sortIndex) {
      ascending = !ascending;
    } else {
      sortIndex = index;
      ascending = true;
    }
    wBrowser.setSortColumn(wBrowser.getColumn(index));
    wBrowser.setSortDirection(ascending ? SWT.DOWN : SWT.UP);

    refreshBrowser();
  }

  private void fileFilterSelected(Event event) {
    refreshBrowser();
  }

  @Override
  public void setMessage(String message) {
    this.message = message;
  }

  //  @GuiToolbarElement(
  //    root = BOOKMARKS_TOOLBAR_PARENT_ID,
  //    id = BOOKMARKS_ITEM_ID_BOOKMARK_GOTO,
  //    toolTip = "Browse to the selected bookmark",
  //    image = "ui/images/arrow-right.svg"
  //  )
  public void browseToSelectedBookmark() {
    String name = getSelectedBookmark();
    if (name == null) {
      return;
    }
    String path = bookmarks.get(name);
    if (path != null) {
      navigateTo(path, true);
    }
  }

  private String getSelectedBookmark() {
    int selectionIndex = wBookmarks.getSelectionIndex();
    if (selectionIndex < 0) {
      return null;
    }
    String name = wBookmarks.getItems()[selectionIndex];
    return name;
  }

  /**
   * User double clicked on a bookmark
   *
   * @param event
   */
  private void bookmarkDefaultSelection(Event event) {
    browseToSelectedBookmark();
  }

  private void okButton() {
    try {
      activeFileObject = HopVfs.getFileObject(wFilename.getText());
      ok();
    } catch (Throwable e) {
      showError("Error parsing filename: '" + wFilename.getText(), e);
    }
  }

  private void enteredFilenameOrFolder() {
    if (StringUtils.isNotEmpty(saveFilename)) {
      try {
        FileObject fullObject = HopVfs.getFileObject(wFilename.getText());
        if (!fullObject.isFolder()) {
          // We're saving a filename and now if we hit enter we want this to select the file and
          // close the dialog
          //
          activeFileObject = fullObject;
          ok();
          return;
        }
      } catch (Exception e) {
        // Ignore error, just try to refresh and the error will be listed in the message widget
      }
    }
    refreshBrowser();
  }

  private FileObject getSelectedFileObject() {
    TreeItem[] selection = wBrowser.getSelection();
    if (selection == null || selection.length != 1) {
      return null;
    }

    String path = getTreeItemPath(selection[0]);
    FileObject fileObject = fileObjectsMap.get(path);
    return fileObject;
  }

  /**
   * Something is selected in the browser
   *
   * @param e
   */
  private void fileSelected(Event e) {
    FileObject fileObject = getSelectedFileObject();
    if (fileObject != null) {
      selectedFile = fileObject;
      showFilename(selectedFile);
    }
  }

  private void showFilename(FileObject fileObject) {
    try {
      wFilename.setText(HopVfs.getFilename(fileObject));

      FileContent content = fileObject.getContent();

      String details = "";

      if (fileObject.isFolder()) {
        details += "Folder: " + HopVfs.getFilename(fileObject) + Const.CR;
      } else {
        details += "Name: " + fileObject.getName().getBaseName() + "   ";
        details += "Folder: " + HopVfs.getFilename(fileObject.getParent()) + "   ";
        details += "Size: " + content.getSize();
        if (content.getSize() >= 1024) {
          details += " (" + getFileSize(fileObject) + ")";
        }
        details += Const.CR;
      }
      details += "Last modified: " + getFileDate(fileObject) + Const.CR;
      details += "Readable: " + (fileObject.isReadable() ? "Yes" : "No") + "  ";
      details += "Writable: " + (fileObject.isWriteable() ? "Yes" : "No") + "  ";
      details += "Executable: " + (fileObject.isExecutable() ? "Yes" : "No") + Const.CR;
      if (fileObject.isSymbolicLink()) {
        details += "This is a symbolic link" + Const.CR;
      }
      Map<String, Object> attributes = content.getAttributes();
      if (attributes != null && !attributes.isEmpty()) {
        details += "Attributes: " + Const.CR;
        for (String key : attributes.keySet()) {
          Object value = attributes.get(key);
          details += "   " + key + " : " + (value == null ? "" : value.toString()) + Const.CR;
        }
      }
      showDetails(details);
    } catch (Throwable e) {
      showError("Error getting information on file " + fileObject.toString(), e);
    }
  }

  /**
   * Double clicked on a file or folder
   *
   * @param event
   */
  private void fileDefaultSelected(Event event) {
    FileObject fileObject = getSelectedFileObject();
    if (fileObject == null) {
      return;
    }

    try {
      navigateTo(HopVfs.getFilename(fileObject), true);

      if (fileObject.isFolder()) {
        // Browse into the selected folder...
        //
        refreshBrowser();
      } else {
        // Take this file as the user choice for this dialog
        //
        okButton();
      }
    } catch (Throwable e) {
      showError("Error handling default selection on file " + fileObject.toString(), e);
    }
  }

  private void getData() {

    // Take the first by default: All types
    //
    if (!browsingDirectories) {
      wFilters.select(0);
    }

    refreshBookmarks();

    if (StringUtils.isEmpty(fileName)) {
      if (StringUtils.isEmpty(filterPath)) {
        // Default to the user home directory
        //
        fileName = System.getProperty("user.home");
      } else {
        fileName = filterPath;
      }
    }

    showDetails(message);

    navigateTo(fileName, true);
    browserColumnSelected(0);
  }

  private void showDetails(String details) {
    wDetails.setText(Const.NVL(details, Const.NVL(message, "")));
  }

  private void refreshBookmarks() {
    // Add the bookmarks
    //
    java.util.List<String> bookmarkNames = new ArrayList<>(bookmarks.keySet());
    Collections.sort(bookmarkNames);
    wBookmarks.setItems(bookmarkNames.toArray(new String[0]));
  }

  private void refreshBrowser() {
    String filename = wFilename.getText();
    if (StringUtils.isEmpty(filename)) {
      return;
    }

    // Browse to the selected file location...
    //
    try {
      activeFileObject = HopVfs.getFileObject(filename);
      if (activeFileObject.isFolder()) {
        activeFolder = activeFileObject;
      } else {
        activeFolder = activeFileObject.getParent();
      }
      wBrowser.removeAll();

      fileObjectsMap = new HashMap<>();

      TreeItem parentFolderItem = new TreeItem(wBrowser, SWT.NONE);
      parentFolderItem.setImage(folderImage);
      parentFolderItem.setText(activeFolder.getName().getBaseName());
      fileObjectsMap.put(getTreeItemPath(parentFolderItem), activeFolder);

      populateFolder(activeFolder, parentFolderItem);

      parentFolderItem.setExpanded(true);
    } catch (Throwable e) {
      showError("Error browsing to location: " + filename, e);
    }
  }

  private void showError(String string, Throwable e) {
    showDetails(
        string
            + Const.CR
            + Const.getSimpleStackTrace(e)
            + Const.CR
            + Const.CR
            + Const.getClassicStackTrace(e));
  }

  private String getTreeItemPath(TreeItem item) {
    String path = "/" + item.getText();
    TreeItem parentItem = item.getParentItem();
    while (parentItem != null) {
      path = "/" + parentItem.getText() + path;
      parentItem = parentItem.getParentItem();
    }
    String filename = item.getText(0);
    if (StringUtils.isNotEmpty(filename)) {
      path += filename;
    }
    return path;
  }

  /**
   * Child folders are always shown at the top. Files below it sorted alphabetically
   *
   * @param folder
   * @param folderItem
   */
  private void populateFolder(FileObject folder, TreeItem folderItem) throws FileSystemException {

    FileObject[] children = folder.getChildren();

    Arrays.sort(
        children,
        (child1, child2) -> {
          try {
            int cmp;
            switch (sortIndex) {
              case 0:
                String name1 = child1.getName().getBaseName();
                String name2 = child2.getName().getBaseName();
                cmp = name1.compareToIgnoreCase(name2);
                break;
              case 1:
                long time1 = child1.getContent().getLastModifiedTime();
                long time2 = child2.getContent().getLastModifiedTime();
                cmp = Long.compare(time1, time2);
                break;
              case 2:
                long size1 = child1.getContent().getSize();
                long size2 = child2.getContent().getSize();
                cmp = Long.compare(size1, size2);
                break;

              default:
                cmp = 0;
            }
            if (ascending) {
              return -cmp;
            } else {
              return cmp;
            }
          } catch (Exception e) {
            return 0;
          }
        });

    // First the child folders
    //
    for (FileObject child : children) {
      if (child.isFolder()) {
        String baseFilename = child.getName().getBaseName();
        if (!showingHiddenFiles && baseFilename.startsWith(".")) {
          continue;
        }
        TreeItem childFolderItem = new TreeItem(folderItem, SWT.NONE);
        childFolderItem.setImage(folderImage);
        childFolderItem.setText(child.getName().getBaseName());
        fileObjectsMap.put(getTreeItemPath(childFolderItem), child);
      }
    }
    if (!browsingDirectories) {
      for (final FileObject child : children) {
        if (child.isFile()) {
          String baseFilename = child.getName().getBaseName();
          if (!showingHiddenFiles && baseFilename.startsWith(".")) {
            continue;
          }

          boolean selectFile = false;

          // Check file extension...
          //
          String selectedExtensions = filterExtensions[wFilters.getSelectionIndex()];
          String[] exts = selectedExtensions.split(";");
          for (String ext : exts) {
            if (FilenameUtils.wildcardMatch(baseFilename, ext)) {
              selectFile = true;
            }
          }

          // Hidden file?
          //
          if (selectFile) {
            TreeItem childFileItem = new TreeItem(folderItem, SWT.NONE);
            childFileItem.setImage(getFileImage(child));
            childFileItem.setFont(GuiResource.getInstance().getFontBold());
            childFileItem.setText(0, child.getName().getBaseName());
            childFileItem.setText(1, getFileDate(child));
            childFileItem.setText(2, getFileSize(child));
            fileObjectsMap.put(getTreeItemPath(childFileItem), child);

            // Gray out if the file is not readable
            //
            if (!child.isReadable()) {
              childFileItem.setForeground(GuiResource.getInstance().getColorGray());
            }

            if (child.equals(activeFileObject)) {
              wBrowser.setSelection(childFileItem);
              wBrowser.showSelection();
            }
          }
        }
      }
    }
  }

  private Image getFileImage(FileObject file) {
    try {
      IHopFileType<?> fileType =
          HopFileTypeRegistry.getInstance().findHopFileType(file.getName().getBaseName());
      if (fileType != null) {
        IPlugin plugin =
            PluginRegistry.getInstance().getPlugin(HopFileTypePluginType.class, fileType);
        if (plugin != null && plugin.getImageFile() != null) {
          return GuiResource.getInstance()
              .getImage(plugin.getImageFile(), ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE);
        }
      }
    } catch (HopException e) {
      // Ignore
    }

    return fileImage;
  }

  private String getFileSize(FileObject child) {
    try {
      long size = child.getContent().getSize();
      String[] units = {"", " kB", " MB", " GB", " TB", " PB", " XB", " YB", " ZB"};
      for (int i = 0; i < units.length; i++) {
        double unitSize = Math.pow(1024, i);
        double maxSize = Math.pow(1024, i + 1);
        if (size < maxSize) {
          return new DecimalFormat("0.#").format(size / unitSize) + units[i];
        }
      }
      return Long.toString(size);
    } catch (Exception e) {
      LogChannel.GENERAL.logError("Error getting size of file : " + child.toString(), e);
      return "?";
    }
  }

  private String getFileDate(FileObject child) {
    try {
      long lastModifiedTime = child.getContent().getLastModifiedTime();
      return new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(new Date(lastModifiedTime));
    } catch (Exception e) {
      LogChannel.GENERAL.logError(
          "Error getting last modified date of file : " + child.toString(), e);
      return "?";
    }
  }

  private void cancel() {
    activeFileObject = null;
    dispose();
  }

  private void ok() {
    try {
      if (activeFileObject.isFolder()) {
        filterPath = HopVfs.getFilename(activeFileObject);
        fileName = null;
      } else {
        filterPath = HopVfs.getFilename(activeFileObject.getParent());
        fileName = activeFileObject.getName().getBaseName();
      }
      dispose();
    } catch (FileSystemException e) {
      showError("Error finding parent folder of file: '" + activeFileObject.toString(), e);
    }
  }

  private void dispose() {
    instance = null;
    try {
      AuditManager.getActive()
          .storeList(
            usedNamespace, BOOKMARKS_AUDIT_TYPE, new AuditList(navigationHistory));
    } catch (Exception e) {
      LogChannel.GENERAL.logError("Error storing navigation history", e);
    }
    props.setScreen(new WindowProperty(shell));

    // We no longer need the toolbar or the objects it used to listen to the buttons
    //
    bookmarksToolbarWidgets.dispose();
    browserToolbarWidgets.dispose();

    shell.dispose();
  }

  @GuiToolbarElement(
      root = BOOKMARKS_TOOLBAR_PARENT_ID,
      id = BOOKMARKS_ITEM_ID_BOOKMARK_ADD,
      toolTip = "Add the selected file or folder as a new bookmark",
      image = "ui/images/bookmark-add.svg")
  public void addBookmark() {
    if (selectedFile != null) {
      String name = selectedFile.getName().getBaseName();
      EnterStringDialog dialog =
          new EnterStringDialog(
              shell, name, "Enter bookmark", "Please enter the name for this bookmark");
      name = dialog.open();
      if (name != null) {
        String path = HopVfs.getFilename(selectedFile);
        bookmarks.put(name, path);
        saveBookmarks();
        refreshBookmarks();
      }
    }
    refreshStates();
  }

  @GuiToolbarElement(
      root = BOOKMARKS_TOOLBAR_PARENT_ID,
      id = BOOKMARKS_ITEM_ID_BOOKMARK_REMOVE,
      toolTip = "Remove the selected bookmark",
      image = "ui/images/delete.svg")
  public void removeBookmark() {
    String name = getSelectedBookmark();
    if (name != null) {
      bookmarks.remove(name);
      saveBookmarks();
      refreshBookmarks();
    }
    refreshStates();
  }

  private void saveBookmarks() {
    try {
      AuditManager.getActive()
          .saveMap(usedNamespace, BOOKMARKS_AUDIT_TYPE, bookmarks);
    } catch (Throwable e) {
      showError("Error saving bookmarks: '" + activeFileObject.toString(), e);
    }
  }

  public void navigateTo(String filename, boolean saveHistory) {
    if (saveHistory) {
      // Add to navigation history
      //
      if (navigationIndex >= 0) {
        if (navigationIndex < navigationHistory.size()) {
          // Clear history above the index...
          //
          navigationHistory.subList(navigationIndex, navigationHistory.size());
        }
      }

      navigationHistory.add(filename);
      navigationIndex = navigationHistory.size() - 1;
    }

    if (StringUtils.isEmpty(saveFilename)) {
      wFilename.setText(filename);
    } else {
      try {
        // Save the "saveFilename" entered text by the user?
        //
        String oldFull = wFilename.getText();
        if (StringUtils.isNotEmpty(oldFull)) {
          try {
            FileObject oldFullObject = HopVfs.getFileObject(oldFull);
            if (!oldFullObject.isFolder()) {
              saveFilename = oldFullObject.getName().getBaseName();
            }
          } catch (Exception e) {
            // This wasn't a valid filename, ignore the error to reduce spamming
          }
        } else {
          // First call, set to filter path plus saveFilename
          //
          if (StringUtils.isNotEmpty(filterPath)) {
            wFilename.setText(filterPath + "/" + saveFilename);
          }
        }

        if (HopVfs.getFileObject(filename).isFolder()) {
          String fullPath = FilenameUtils.concat(filename, saveFilename);
          wFilename.setText(fullPath);
          // Select the saveFilename part...
          //
          int start = fullPath.lastIndexOf(saveFilename);
          int end = fullPath.lastIndexOf(".");
          wFilename.getTextWidget().setSelection(start, end);
          wFilename.setFocus();
        }
      } catch (Exception e) {
        wFilename.setText(filename);
      }
    }
    refreshBrowser();
    refreshStates();
    resizeTableColumn();
  }

  @GuiToolbarElement(
      root = NAVIGATE_TOOLBAR_PARENT_ID,
      id = NAVIGATE_ITEM_ID_NAVIGATE_HOME,
      toolTip = "Navigate to the user home directory",
      image = "ui/images/home.svg")
  public void navigateHome() {
    navigateTo(System.getProperty("user.home"), true);
  }

  @GuiToolbarElement(
      root = NAVIGATE_TOOLBAR_PARENT_ID,
      id = NAVIGATE_ITEM_ID_REFRESH_ALL,
      toolTip = "Refresh",
      image = "ui/images/refresh.svg")
  public void refreshAll() {
    refreshBookmarks();
    refreshBrowser();
  }

  @GuiToolbarElement(
      root = NAVIGATE_TOOLBAR_PARENT_ID,
      id = NAVIGATE_ITEM_ID_NAVIGATE_UP,
      toolTip = "Navigate to the parent folder",
      image = "ui/images/navigate-up.svg")
  public void navigateUp() {
    try {
      FileObject fileObject = HopVfs.getFileObject(wFilename.getText());
      if (fileObject.isFile()) {
        fileObject = fileObject.getParent();
      }
      FileObject parent = fileObject.getParent();
      if (parent != null) {
        navigateTo(HopVfs.getFilename(parent), true);
      }
    } catch (Throwable e) {
      showError("Error navigating up: '" + activeFileObject.toString(), e);
    }
  }

  @GuiToolbarElement(
      root = BROWSER_TOOLBAR_PARENT_ID,
      id = BROWSER_ITEM_ID_CREATE_FOLDER,
      toolTip = "Create folder",
      image = "ui/images/folder-add.svg")
  public void createFolder() {
    String folder = "";
    EnterStringDialog dialog =
        new EnterStringDialog(
            shell,
            folder,
            "Create directory",
            "Please enter name of the folder to create in : " + activeFolder);
    folder = dialog.open();
    if (folder != null) {
      String newPath = activeFolder.toString();
      if (!newPath.endsWith("/") && !newPath.endsWith("\\")) {
        newPath += "/";
      }
      newPath += folder;
      try {
        FileObject newFolder = HopVfs.getFileObject(newPath);
        newFolder.createFolder();
        refreshBrowser();
      } catch (Throwable e) {
        showError("Error creating folder '" + newPath + "'", e);
      }
    }
  }

  @GuiToolbarElement(
      root = NAVIGATE_TOOLBAR_PARENT_ID,
      id = NAVIGATE_ITEM_ID_NAVIGATE_PREVIOUS,
      toolTip = "Navigate to previous path from your history",
      image = "ui/images/navigate-back.svg",
      separator = true)
  public void navigateHistoryPrevious() {
    if (navigationIndex - 1 >= 0) {
      navigationIndex--;
      navigateTo(navigationHistory.get(navigationIndex), false);
    }
  }

  @GuiToolbarElement(
      root = NAVIGATE_TOOLBAR_PARENT_ID,
      id = NAVIGATE_ITEM_ID_NAVIGATE_NEXT,
      toolTip = "Navigate to next path from your history",
      image = "ui/images/navigate-forward.svg")
  public void navigateHistoryNext() {
    if (navigationIndex + 1 < navigationHistory.size() - 1) {
      navigationIndex++;
      navigateTo(navigationHistory.get(navigationIndex), false);
    }
  }

  @GuiToolbarElement(
      root = BROWSER_TOOLBAR_PARENT_ID,
      id = BROWSER_ITEM_ID_SHOW_HIDDEN,
      toolTip = "Show or hide hidden files and directories",
      image = "ui/images/show.svg",
      separator = true)
  public void showHideHidden() {
    showingHiddenFiles = !showingHiddenFiles;

    refreshBrowser();
  }

  private void refreshStates() {
    // Navigation icons...
    //
    navigateToolbarWidgets.enableToolbarItem(
        NAVIGATE_ITEM_ID_NAVIGATE_PREVIOUS, navigationIndex > 0);
    navigateToolbarWidgets.enableToolbarItem(
        NAVIGATE_ITEM_ID_NAVIGATE_NEXT, navigationIndex + 1 < navigationHistory.size());

    // Up...
    //
    boolean canGoUp;
    try {
      FileObject fileObject = HopVfs.getFileObject(wFilename.getText());
      if (fileObject.isFile()) {
        canGoUp = fileObject.getParent().getParent() != null;
      } else {
        canGoUp = fileObject.getParent() != null;
      }
    } catch (Exception e) {
      canGoUp = false;
    }
    browserToolbarWidgets.enableToolbarItem(NAVIGATE_ITEM_ID_NAVIGATE_UP, canGoUp);

    // Bookmarks...
    //
    boolean bookmarkSelected = getSelectedBookmark() != null;
    bookmarksToolbarWidgets.enableToolbarItem(BOOKMARKS_ITEM_ID_BOOKMARK_GOTO, bookmarkSelected);
    bookmarksToolbarWidgets.enableToolbarItem(BOOKMARKS_ITEM_ID_BOOKMARK_REMOVE, bookmarkSelected);

    wOk.setEnabled(StringUtils.isNotEmpty(wFilename.getText()));
  }

  private void resizeTableColumn() {
    TreeUtil.setOptimalWidthOnColumns(wBrowser);
  }

  /**
   * Gets text
   *
   * @return value of text
   */
  public String getText() {
    return text;
  }

  /** @param text The text to set */
  public void setText(String text) {
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

  /** @param variables The variables to set */
  public void setVariables(IVariables variables) {
    this.variables = variables;
  }

  /**
   * Gets fileName
   *
   * @return value of fileName
   */
  @Override
  public String getFileName() {
    return fileName;
  }

  /** @param fileName The fileName to set */
  @Override
  public void setFileName(String fileName) {
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

  /** @param filterExtensions The filterExtensions to set */
  public void setFilterExtensions(String[] filterExtensions) {
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

  /** @param filterNames The filterNames to set */
  public void setFilterNames(String[] filterNames) {
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

  /** @param bookmarks The bookmarks to set */
  public void setBookmarks(Map<String, String> bookmarks) {
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

  /** @param activeFileObject The activeFileObject to set */
  public void setActiveFileObject(FileObject activeFileObject) {
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

  /** @param activeFolder The activeFolder to set */
  public void setActiveFolder(FileObject activeFolder) {
    this.activeFolder = activeFolder;
  }

  /**
   * Gets filterPath
   *
   * @return value of filterPath
   */
  @Override
  public String getFilterPath() {
    return filterPath;
  }

  /** @param filterPath The filterPath to set */
  public void setFilterPath(String filterPath) {
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

  /** @param showingHiddenFiles The showingHiddenFiles to set */
  public void setShowingHiddenFiles(boolean showingHiddenFiles) {
    this.showingHiddenFiles = showingHiddenFiles;
  }

  /**
   * Gets message
   *
   * @return value of message
   */
  public String getMessage() {
    return message;
  }

  /**
   * Gets browsingDirectories
   *
   * @return value of browsingDirectories
   */
  public boolean isBrowsingDirectories() {
    return browsingDirectories;
  }

  /** @param browsingDirectories The browsingDirectories to set */
  public void setBrowsingDirectories(boolean browsingDirectories) {
    this.browsingDirectories = browsingDirectories;
  }

  /**
   * Gets savingFile
   *
   * @return value of savingFile
   */
  public boolean isSavingFile() {
    return savingFile;
  }

  /** @param savingFile The savingFile to set */
  public void setSavingFile(boolean savingFile) {
    this.savingFile = savingFile;
  }

  /**
   * Gets saveFilename
   *
   * @return value of saveFilename
   */
  public String getSaveFilename() {
    return saveFilename;
  }

  /** @param saveFilename The saveFilename to set */
  public void setSaveFilename(String saveFilename) {
    this.saveFilename = saveFilename;
  }
}
