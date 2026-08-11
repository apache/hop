/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.ui.core.widget;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Props;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.FindReplaceDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.GuiToolbarWidgets;
import org.apache.hop.ui.core.gui.IToolbarContainer;
import org.apache.hop.ui.hopgui.ToolbarFacade;
import org.eclipse.swt.SWT;
import org.eclipse.swt.dnd.Clipboard;
import org.eclipse.swt.dnd.TextTransfer;
import org.eclipse.swt.events.MenuDetectListener;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.MenuItem;

/**
 * Multi-line text editor composite used across Hop dialogs (SQL, scripts, logs, etc.).
 *
 * <p>Supports an optional toolbar (undo/redo, clipboard, find/replace) that plugins can extend via
 * {@link GuiToolbarElement} with {@link #ID_TOOLBAR} as the root, the same way {@link TableView}
 * toolbars work.
 *
 * <p>Toolbar plugins receive a {@link TextComposite} instance. To know what kind of text is being
 * edited, use {@link #getStyleType()} (e.g. {@link #STYLE_TYPE_SQL}) rather than {@code instanceof}
 * checks on subclasses — on Hop Web many specialized editors are replaced by a plain {@link
 * StyledTextComp} with the same style type set.
 *
 * <pre>{@code
 * @GuiToolbarElement(root = TextComposite.ID_TOOLBAR, id = "...", image = "...")
 * public static void mySqlAction(TextComposite text) {
 *   if (!TextComposite.STYLE_TYPE_SQL.equals(text.getStyleType())) {
 *     return;
 *   }
 *   // ...
 * }
 * }</pre>
 */
@GuiPlugin
public abstract class TextComposite extends Composite {
  private static final Class<?> PKG = StyledTextComp.class;

  public static final String ID_TOOLBAR = "TextComposite-Toolbar";
  public static final String ID_TOOLBAR_UNDO = "textcomposite-toolbar-10000-undo";
  public static final String ID_TOOLBAR_REDO = "textcomposite-toolbar-10010-redo";
  public static final String ID_TOOLBAR_CUT = "textcomposite-toolbar-10100-cut";
  public static final String ID_TOOLBAR_COPY = "textcomposite-toolbar-10110-copy";
  public static final String ID_TOOLBAR_PASTE = "textcomposite-toolbar-10120-paste";
  public static final String ID_TOOLBAR_SELECT_ALL = "textcomposite-toolbar-10130-select-all";
  public static final String ID_TOOLBAR_FIND = "textcomposite-toolbar-10200-find";
  public static final String ID_TOOLBAR_FIND_REPLACE = "textcomposite-toolbar-10210-find-replace";

  /** Default / unspecified multi-line text. */
  public static final String STYLE_TYPE_GENERIC = "Generic";

  /** SQL (or SQL-like) script. */
  public static final String STYLE_TYPE_SQL = "SQL";

  /** JavaScript source. */
  public static final String STYLE_TYPE_JAVASCRIPT = "JavaScript";

  /** Java source. */
  public static final String STYLE_TYPE_JAVA = "Java";

  /** Generic scripting language (engine may vary). */
  public static final String STYLE_TYPE_SCRIPT = "Script";

  /** Execution or application log output. */
  public static final String STYLE_TYPE_LOG = "Log";

  /** Unified / git-style diff. */
  public static final String STYLE_TYPE_DIFF = "Diff";

  /** Regular expression. */
  public static final String STYLE_TYPE_REGEX = "Regex";

  /** Formula / expression language. */
  public static final String STYLE_TYPE_FORMULA = "Formula";

  /** JSON document or query payload. */
  public static final String STYLE_TYPE_JSON = "JSON";

  /** Free-form human-readable text / message body. */
  public static final String STYLE_TYPE_TEXT = "Text";

  /** Cassandra Query Language. */
  public static final String STYLE_TYPE_CQL = "CQL";

  /** Salesforce Object Query Language. */
  public static final String STYLE_TYPE_SOQL = "SOQL";

  /** Drools rules. */
  public static final String STYLE_TYPE_DROOLS = "Drools";

  @Getter private final boolean toolbarEnabled;
  @Getter @Setter private Control toolbar;
  @Getter @Setter private GuiToolbarWidgets toolbarWidgets;

  /**
   * Semantic type of the text being edited (for toolbar plugins and similar contributors). Defaults
   * to {@link #STYLE_TYPE_GENERIC}. Prefer the {@code STYLE_TYPE_*} constants; plugins may use
   * other free-form values.
   *
   * <p><strong>Must be set via the constructor</strong> so that {@link
   * org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElementFilter} sees the correct value when the
   * toolbar is built. Calling {@link #setStyleType(String)} after construction does not rebuild the
   * toolbar or re-run filters.
   */
  @Getter private String styleType = STYLE_TYPE_GENERIC;

  private final List<String> removeToolItems;
  private final PropsUi props;

  /**
   * Sets the semantic style type of this editor.
   *
   * <p>Does <strong>not</strong> rebuild the toolbar or re-evaluate toolbar filters. Prefer passing
   * {@code styleType} to the constructor so plugins that filter on type (for example JSON format)
   * work correctly.
   *
   * @param styleType type label; {@code null} resets to {@link #STYLE_TYPE_GENERIC}
   */
  public void setStyleType(String styleType) {
    this.styleType = styleType != null ? styleType : STYLE_TYPE_GENERIC;
  }

  /**
   * Constructs a new instance of this class given its parent and a style value describing its
   * behavior and appearance.
   *
   * <p>The style value is either one of the style constants defined in class <code>SWT</code> which
   * is applicable to instances of this class, or must be built by <em>bitwise OR</em>'ing together
   * (that is, using the <code>int</code> "|" operator) two or more of those <code>SWT</code> style
   * constants. The class description lists the style constants that are applicable to the class.
   * Style bits are also inherited from superclasses.
   *
   * @param parent a widget which will be the parent of the new instance (cannot be null)
   * @param style the style of widget to construct
   * @throws IllegalArgumentException
   */
  public TextComposite(Composite parent, int style) {
    this(parent, style, true, new ArrayList<>(), STYLE_TYPE_GENERIC);
  }

  /**
   * @param parent parent composite
   * @param style SWT style for this composite (usually {@link SWT#NONE}; the text control has its
   *     own style)
   * @param toolbarEnabled whether a toolbar may be shown above the text
   */
  public TextComposite(Composite parent, int style, boolean toolbarEnabled) {
    this(parent, style, toolbarEnabled, new ArrayList<>(), STYLE_TYPE_GENERIC);
  }

  /**
   * @param parent parent composite
   * @param style SWT style for this composite
   * @param toolbarEnabled whether a toolbar may be shown above the text
   * @param styleType semantic content type ({@link #STYLE_TYPE_SQL}, …); used by toolbar filters
   */
  public TextComposite(Composite parent, int style, boolean toolbarEnabled, String styleType) {
    this(parent, style, toolbarEnabled, new ArrayList<>(), styleType);
  }

  /**
   * @param parent parent composite
   * @param style SWT style for this composite
   * @param toolbarEnabled whether a toolbar may be shown above the text
   * @param removeToolItems toolbar item IDs to hide for this instance
   */
  public TextComposite(
      Composite parent, int style, boolean toolbarEnabled, List<String> removeToolItems) {
    this(parent, style, toolbarEnabled, removeToolItems, STYLE_TYPE_GENERIC);
  }

  /**
   * Full constructor. {@code styleType} is applied <em>before</em> the toolbar is created so {@link
   * org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElementFilter} methods can read it.
   *
   * @param parent parent composite
   * @param style SWT style for this composite
   * @param toolbarEnabled whether a toolbar may be shown above the text
   * @param removeToolItems toolbar item IDs to hide for this instance
   * @param styleType semantic content type ({@link #STYLE_TYPE_SQL}, …)
   */
  public TextComposite(
      Composite parent,
      int style,
      boolean toolbarEnabled,
      List<String> removeToolItems,
      String styleType) {
    super(parent, style);
    this.props = PropsUi.getInstance();
    this.toolbarEnabled = toolbarEnabled;
    this.removeToolItems =
        removeToolItems != null ? new ArrayList<>(removeToolItems) : new ArrayList<>();
    this.styleType = styleType != null ? styleType : STYLE_TYPE_GENERIC;

    FormLayout layout = new FormLayout();
    layout.marginLeft = 0;
    layout.marginRight = 0;
    layout.marginTop = 0;
    layout.marginBottom = 0;
    setLayout(layout);

    addToolbar();
  }

  /**
   * Control that content (the text widget and variable icon) should attach below, or {@code null}
   * when no toolbar is shown.
   */
  public Control getTopControl() {
    return toolbar;
  }

  protected void addToolbar() {
    toolbarWidgets = new GuiToolbarWidgets();
    // Register under TextComposite so toolbar listeners and plugin static methods resolve this
    // instance for every subclass (StyledTextComp, SQLStyledTextComp, …).
    toolbarWidgets.registerGuiPluginObject(TextComposite.class.getName(), this);

    if (toolbarEnabled && props.isShowTextCompositeToolbar()) {
      IToolbarContainer toolBarContainer =
          ToolbarFacade.createToolbarContainer(this, SWT.WRAP | SWT.LEFT | SWT.HORIZONTAL);
      toolbar = toolBarContainer.getControl();
      FormData fdToolBar = new FormData();
      fdToolBar.left = new FormAttachment(0, 0);
      fdToolBar.top = new FormAttachment(0, 0);
      fdToolBar.right = new FormAttachment(100, 0);
      toolbar.setLayoutData(fdToolBar);
      PropsUi.setLook(toolbar, Props.WIDGET_STYLE_TOOLBAR);

      toolbarWidgets.createToolbarWidgets(toolBarContainer, ID_TOOLBAR, removeToolItems);
      toolbar.pack();
    }
  }

  /** Refresh enablement of built-in toolbar buttons based on selection and editability. */
  public void updateToolbar() {
    if (toolbarWidgets == null || toolbar == null || toolbar.isDisposed()) {
      return;
    }
    boolean editable = isEditable();
    boolean hasSelection = getSelectionCount() > 0;
    boolean canPaste = editable && checkPaste();

    toolbarWidgets.enableToolbarItem(ID_TOOLBAR_UNDO, canUndo());
    toolbarWidgets.enableToolbarItem(ID_TOOLBAR_REDO, canRedo());
    toolbarWidgets.enableToolbarItem(ID_TOOLBAR_CUT, editable && hasSelection);
    toolbarWidgets.enableToolbarItem(ID_TOOLBAR_COPY, hasSelection);
    toolbarWidgets.enableToolbarItem(ID_TOOLBAR_PASTE, canPaste);
    toolbarWidgets.enableToolbarItem(ID_TOOLBAR_SELECT_ALL, getCharCount() > 0);
    toolbarWidgets.enableToolbarItem(ID_TOOLBAR_FIND, true);
    toolbarWidgets.enableToolbarItem(ID_TOOLBAR_FIND_REPLACE, editable);
  }

  // --- Toolbar actions (static so they resolve for every TextComposite subclass) ---

  @GuiToolbarElement(
      root = ID_TOOLBAR,
      id = ID_TOOLBAR_UNDO,
      image = "ui/images/undo.svg",
      toolTip = "i18n::TextComposite.ToolBarWidget.Undo.ToolTip")
  public static void toolbarUndo(TextComposite text) {
    text.undo();
    text.updateToolbar();
  }

  @GuiToolbarElement(
      root = ID_TOOLBAR,
      id = ID_TOOLBAR_REDO,
      image = "ui/images/redo.svg",
      toolTip = "i18n::TextComposite.ToolBarWidget.Redo.ToolTip")
  public static void toolbarRedo(TextComposite text) {
    text.redo();
    text.updateToolbar();
  }

  @GuiToolbarElement(
      root = ID_TOOLBAR,
      id = ID_TOOLBAR_CUT,
      image = "ui/images/cut.svg",
      toolTip = "i18n::TextComposite.ToolBarWidget.Cut.ToolTip",
      separator = true)
  public static void toolbarCut(TextComposite text) {
    text.cut();
    text.updateToolbar();
  }

  @GuiToolbarElement(
      root = ID_TOOLBAR,
      id = ID_TOOLBAR_COPY,
      image = "ui/images/copy.svg",
      toolTip = "i18n::TextComposite.ToolBarWidget.Copy.ToolTip")
  public static void toolbarCopy(TextComposite text) {
    text.copy();
    text.updateToolbar();
  }

  @GuiToolbarElement(
      root = ID_TOOLBAR,
      id = ID_TOOLBAR_PASTE,
      image = "ui/images/paste.svg",
      toolTip = "i18n::TextComposite.ToolBarWidget.Paste.ToolTip")
  public static void toolbarPaste(TextComposite text) {
    text.paste();
    text.updateToolbar();
  }

  @GuiToolbarElement(
      root = ID_TOOLBAR,
      id = ID_TOOLBAR_SELECT_ALL,
      image = "ui/images/select-all.svg",
      toolTip = "i18n::TextComposite.ToolBarWidget.SelectAll.ToolTip")
  public static void toolbarSelectAll(TextComposite text) {
    text.selectAll();
    text.updateToolbar();
  }

  @GuiToolbarElement(
      root = ID_TOOLBAR,
      id = ID_TOOLBAR_FIND,
      image = "ui/images/search.svg",
      toolTip = "i18n::TextComposite.ToolBarWidget.Find.ToolTip",
      separator = true)
  public static void toolbarFind(TextComposite text) {
    text.find();
  }

  @GuiToolbarElement(
      root = ID_TOOLBAR,
      id = ID_TOOLBAR_FIND_REPLACE,
      image = "ui/images/edit.svg",
      toolTip = "i18n::TextComposite.ToolBarWidget.FindReplace.ToolTip")
  public static void toolbarFindReplace(TextComposite text) {
    text.findAndReplace();
  }

  /** Open the find dialog (Ctrl+F). */
  public void find() {
    FindReplaceDialog.open(getShell(), this, false);
  }

  /** Open the find and replace dialog (Ctrl+H). */
  public void findAndReplace() {
    FindReplaceDialog.open(getShell(), this, true);
  }

  public abstract void addModifyListener(ModifyListener lsMod);

  public abstract void addLineStyleListener();

  public abstract void addLineStyleListener(List<String> keywords);

  public void addLineStyleListener(String scriptEngine) {
    throw new UnsupportedOperationException("Cannot specify a script engine");
  }

  /**
   * Highlight a range of the text with the given colors. Implementations backed by a plain Text
   * widget (Hop Web) can't style text, so this is a no-op by default. Callers must not reference
   * {@code org.eclipse.swt.custom.StyledText} themselves: that class doesn't exist under RAP and
   * naming it in a signature breaks the reflection done at GUI plugin registration time.
   *
   * @param start offset of the first character to style
   * @param length number of characters to style
   * @param background background color, null to leave unchanged
   * @param foreground foreground color, null to leave unchanged
   */
  public void setStyleRange(int start, int length, Color background, Color foreground) {
    // No styling support by default.
  }

  /**
   * Adds the listener to the collection of listeners who will be notified when the
   * platform-specific context menu trigger has occurred, by sending it one of the messages defined
   * in the <code>MenuDetectListener</code> interface.
   *
   * @param listener the listener which should be notified
   */
  public abstract void addMenuDetectListener(MenuDetectListener listener);

  /** Sets the receiver's pop up menu to the argument. */
  public abstract void setMenu(Menu menu);

  /** Cuts the selected text. */
  public abstract void cut();

  /** Copies the selected text. */
  public abstract void copy();

  /** Pastes text from clipboard. */
  public abstract void paste();

  /** Selects all the text in the receiver. */
  public abstract void selectAll();

  /**
   * Returns the caret position relative to the start of the text.
   *
   * <p>Indexing is zero based.
   *
   * @return the caret position relative to the start of the text.
   */
  public abstract int getCaretPosition();

  /**
   * Sets the caret position.
   *
   * @param position set caret offset, relative to the first character in the text.
   */
  public abstract void setCaretPosition(int position);

  /**
   * Gets the number of characters.
   *
   * @return number of characters in the widget
   */
  public abstract int getCharCount();

  /**
   * @return The caret line number, starting from 1.
   */
  public int getLineNumber() {
    String text = getText();
    if (StringUtils.isEmpty(text)) {
      return 1;
    }

    int rowNumber = 1;
    int textPosition = getCaretPosition();
    while (textPosition > 0) {
      if (text.charAt(textPosition - 1) == '\n') {
        rowNumber++;
      }
      textPosition--;
    }

    return rowNumber;
  }

  /**
   * @return The caret column number, starting from 1.
   */
  public int getColumnNumber() {
    String text = getText();
    if (StringUtils.isEmpty(text)) {
      return 1;
    }

    int columnNumber = 1;
    int textPosition = getCaretPosition();
    while (textPosition > 0
        && text.charAt(textPosition - 1) != '\n'
        && text.charAt(textPosition - 1) != '\r') {
      textPosition--;
      columnNumber++;
    }

    return columnNumber;
  }

  /**
   * Returns the widget text.
   *
   * <p>The text for a text widget is the characters in the widget, or an empty string if this has
   * never been set.
   *
   * @return the widget text
   */
  public abstract String getText();

  /** Sets the contents of the receiver to the given string. */
  public abstract void setText(String text);

  /**
   * Returns the number of selected characters.
   *
   * @return the number of selected characters.
   */
  public abstract int getSelectionCount();

  /** Gets the selected text, or an empty string if there is no current selection. */
  public abstract String getSelectionText();

  /**
   * Inserts a string.
   *
   * <p>The old selection is replaced with the new text.
   *
   * @param strInsert the string
   */
  public abstract void insert(String strInsert);

  /**
   * Sets the selection.
   *
   * <p>Indexing is zero based. The range of a selection is from 0..N where N is the number of
   * characters in the widget.
   *
   * @param start new caret position
   */
  public abstract void setSelection(int start);

  /**
   * Sets the selection to the range specified by the given start and end indices.
   *
   * <p>Indexing is zero based. The range of a selection is from 0..N where N is the number of
   * characters in the widget.
   *
   * @param start the start of the range
   * @param end the end of the range
   */
  public abstract void setSelection(int start, int end);

  /**
   * Returns the editable state.
   *
   * @return whether or not the receiver is editable
   */
  public abstract boolean isEditable();

  /**
   * Sets the editable state.
   *
   * @param editable the new editable state
   */
  public abstract void setEditable(boolean editable);

  /**
   * Check if something is stored inside the Clipboard.
   *
   * @return false if no text is available inside the Clipboard
   */
  protected boolean checkPaste() {
    try {
      Clipboard clipboard = new Clipboard(getParent().getDisplay());
      String text = (String) clipboard.getContents(TextTransfer.getInstance());
      if (!Utils.isEmpty(text)) {
        return true;
      } else {
        return false;
      }
    } catch (Exception e) {
      return false;
    }
  }

  protected void undo() {}

  protected void redo() {}

  protected boolean isSupportUnoRedo() {
    return false;
  }

  /** Whether undo is currently available. */
  protected boolean canUndo() {
    return false;
  }

  /** Whether redo is currently available. */
  protected boolean canRedo() {
    return false;
  }

  protected void buildingStyledTextMenu(Menu popupMenu) {

    if (isSupportUnoRedo()) {
      final MenuItem undoItem = new MenuItem(popupMenu, SWT.PUSH);
      undoItem.setText(
          OsHelper.customizeMenuitemText(BaseMessages.getString(PKG, "WidgetDialog.Styled.Undo")));
      undoItem.setImage(
          GuiResource.getInstance()
              .getImage("ui/images/undo.svg", ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE));
      undoItem.addListener(
          SWT.Selection,
          event -> {
            undo();
            updateToolbar();
          });

      final MenuItem redoItem = new MenuItem(popupMenu, SWT.PUSH);
      redoItem.setText(
          OsHelper.customizeMenuitemText(BaseMessages.getString(PKG, "WidgetDialog.Styled.Redo")));
      redoItem.setImage(
          GuiResource.getInstance()
              .getImage("ui/images/redo.svg", ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE));
      redoItem.addListener(
          SWT.Selection,
          event -> {
            redo();
            updateToolbar();
          });

      new MenuItem(popupMenu, SWT.SEPARATOR);
    }

    final MenuItem cutItem = new MenuItem(popupMenu, SWT.PUSH);
    cutItem.setText(
        OsHelper.customizeMenuitemText(BaseMessages.getString(PKG, "WidgetDialog.Styled.Cut")));
    cutItem.setImage(
        GuiResource.getInstance()
            .getImage("ui/images/cut.svg", ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE));
    cutItem.addListener(
        SWT.Selection,
        event -> {
          cut();
          updateToolbar();
        });

    final MenuItem copyItem = new MenuItem(popupMenu, SWT.PUSH);
    copyItem.setText(
        OsHelper.customizeMenuitemText(BaseMessages.getString(PKG, "WidgetDialog.Styled.Copy")));
    copyItem.setImage(
        GuiResource.getInstance()
            .getImage("ui/images/copy.svg", ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE));
    copyItem.addListener(SWT.Selection, event -> copy());

    final MenuItem pasteItem = new MenuItem(popupMenu, SWT.PUSH);
    pasteItem.setText(
        OsHelper.customizeMenuitemText(BaseMessages.getString(PKG, "WidgetDialog.Styled.Paste")));
    pasteItem.setImage(
        GuiResource.getInstance()
            .getImage("ui/images/paste.svg", ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE));
    pasteItem.addListener(
        SWT.Selection,
        event -> {
          paste();
          updateToolbar();
        });

    new MenuItem(popupMenu, SWT.SEPARATOR);

    final MenuItem selectAllItem = new MenuItem(popupMenu, SWT.PUSH);
    selectAllItem.setText(
        OsHelper.customizeMenuitemText(
            BaseMessages.getString(PKG, "WidgetDialog.Styled.SelectAll")));
    selectAllItem.setImage(
        GuiResource.getInstance()
            .getImage(
                "ui/images/select-all.svg", ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE));
    selectAllItem.addListener(
        SWT.Selection,
        event -> {
          selectAll();
          updateToolbar();
        });

    new MenuItem(popupMenu, SWT.SEPARATOR);

    final MenuItem findItem = new MenuItem(popupMenu, SWT.PUSH);
    findItem.setText(
        OsHelper.customizeMenuitemText(BaseMessages.getString(PKG, "WidgetDialog.Styled.Find")));
    findItem.setImage(
        GuiResource.getInstance()
            .getImage("ui/images/search.svg", ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE));
    findItem.addListener(SWT.Selection, event -> find());

    final MenuItem findReplaceItem = new MenuItem(popupMenu, SWT.PUSH);
    findReplaceItem.setText(
        OsHelper.customizeMenuitemText(
            BaseMessages.getString(PKG, "WidgetDialog.Styled.FindReplace")));
    findReplaceItem.setImage(
        GuiResource.getInstance()
            .getImage("ui/images/edit.svg", ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE));
    findReplaceItem.addListener(SWT.Selection, event -> findAndReplace());

    addListener(
        SWT.KeyDown,
        event -> {
          if (isSupportUnoRedo()
              && event.keyCode == 'z'
              && (event.stateMask & SWT.MOD1) != 0
              && (event.stateMask & SWT.MOD2) != 0) {
            redo();
            updateToolbar();
          } else if (isSupportUnoRedo()
              && event.keyCode == 'z'
              && (event.stateMask & SWT.MOD1) != 0) {
            undo();
            updateToolbar();
          } else if (event.keyCode == 'a' && (event.stateMask & SWT.MOD1) != 0) {
            selectAll();
            updateToolbar();
          } else if (event.keyCode == 'f' && (event.stateMask & SWT.MOD1) != 0) {
            find();
            event.doit = false;
          } else if (event.keyCode == 'h' && (event.stateMask & SWT.MOD1) != 0) {
            findAndReplace();
            event.doit = false;
          }
        });

    addListener(SWT.Modify, event -> updateToolbar());
    addListener(SWT.Selection, event -> updateToolbar());

    addMenuDetectListener(
        event -> {
          pasteItem.setEnabled(checkPaste());
          if (getSelectionCount() > 0) {
            cutItem.setEnabled(true);
            copyItem.setEnabled(true);
          } else {
            cutItem.setEnabled(false);
            copyItem.setEnabled(false);
          }
          findReplaceItem.setEnabled(isEditable());
          updateToolbar();
        });

    setMenu(popupMenu);

    // Initial enablement once the text control exists
    getDisplay()
        .asyncExec(
            () -> {
              if (!isDisposed()) {
                updateToolbar();
              }
            });
  }
}
