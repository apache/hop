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

package org.apache.hop.ui.core.dialog;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.gui.AreaOwner;
import org.apache.hop.core.gui.DPoint;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.Rectangle;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.action.GuiAction;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElementType;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.search.SearchMatcher;
import org.apache.hop.history.AuditManager;
import org.apache.hop.history.AuditState;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.GuiToolbarWidgets;
import org.apache.hop.ui.core.gui.HopNamespace;
import org.apache.hop.ui.core.gui.IToolbarContainer;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.OsHelper;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.ToolbarFacade;
import org.apache.hop.ui.hopgui.context.ContextDialogPlacement;
import org.apache.hop.ui.hopgui.context.GuiActionFavorites;
import org.apache.hop.ui.hopgui.palette.GraphPalette;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.dnd.DND;
import org.eclipse.swt.dnd.DragSource;
import org.eclipse.swt.dnd.DragSourceAdapter;
import org.eclipse.swt.dnd.DragSourceEvent;
import org.eclipse.swt.dnd.TextTransfer;
import org.eclipse.swt.dnd.Transfer;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.Font;
import org.eclipse.swt.graphics.GC;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Canvas;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Monitor;
import org.eclipse.swt.widgets.ScrollBar;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.ToolItem;

@GuiPlugin(description = "This dialog presents you all the actions you can take in a given context")
public class ContextDialog extends Dialog {

  public static final Class<?> PKG = ContextDialog.class; // i18n

  public static final String CATEGORY_OTHER = "Other";

  public static final String GUI_PLUGIN_TOOLBAR_PARENT_ID = "ContextDialog-Toolbar";
  public static final String TOOLBAR_ITEM_COLLAPSE_ALL = "ContextDialog-Toolbar-10010-CollapseAll";
  public static final String TOOLBAR_ITEM_EXPAND_ALL = "ContextDialog-Toolbar-10020-ExpandAll";
  public static final String TOOLBAR_ITEM_ENABLE_CATEGORIES =
      "ContextDialog-Toolbar-10030-EnableCategories";
  public static final String TOOLBAR_ITEM_FIXED_WIDTH = "ContextDialog-Toolbar-10040-FixedWidth";
  public static final String TOOLBAR_ITEM_CLEAR_SEARCH = "ContextDialog-Toolbar-10040-ClearSearch";

  public static final String AUDIT_TYPE_TOOLBAR_SHOW_CATEGORIES = "ContextDialogShowCategories";
  public static final String AUDIT_TYPE_TOOLBAR_FIXED_WIDTH = "ContextDialogFixedWidth";
  public static final String AUDIT_TYPE_CONTEXT_DIALOG = "ContextDialog";
  public static final String AUDIT_NAME_CATEGORY_STATES = "CategoryStates";

  private final Point location;
  private List<GuiAction> actions;
  private final Supplier<List<GuiAction>> actionsSupplier;
  private final PropsUi props;
  private Shell shell;
  private Text wSearch;
  private Label wlTooltip;
  private Canvas wCanvas;
  private ScrolledComposite wScrolledComposite;

  private final int iconSize;

  private final int margin;
  private int xMargin;
  private int yMargin;

  private boolean shiftClicked;
  private boolean ctrlClicked;
  private boolean focusLost;

  /**
   * True when the user started dragging a placeable Create item out of this dialog (issue #3111).
   * On native SWT the dialog closes on drag-start and the graph continues placement; on Hop Web
   * HTML5/SWT DnD is used and the shell is only hidden until dragFinished.
   */
  private boolean placementDrag;

  /**
   * True when a canvas DropTarget already created the transform/action (Hop Web DnD path). Prevents
   * GuiContextUtil from starting a second placement gesture.
   */
  private boolean placementCompletedByDrop;

  /** Item under the mouse when a potential placement drag was armed (MouseDown on Create item). */
  private Item pressItem;

  /** Display coordinates of the MouseDown that armed a potential placement drag. */
  private org.eclipse.swt.graphics.Point pressDisplayLocation;

  private Listener placementArmMoveFilter;
  private Listener placementArmUpFilter;

  /** Item currently being dragged via SWT DnD (Hop Web). */
  private Item dndDragItem;

  /** Minimum pointer movement (display px) before a press on a Create item becomes a drag. */
  private static final int PLACEMENT_DRAG_THRESHOLD_PX = 8;

  /** All context items. */
  private final List<Item> items = new ArrayList<>();

  /** List of filtered items. */
  private final List<Item> filteredItems = new ArrayList<>();

  private Item selectedItem;

  private GuiAction selectedAction;

  private List<AreaOwner> areaOwners = new ArrayList<>();

  private final Color highlightColor;

  private int totalContentHeight = 0;
  private int previousTotalContentHeight = 0;
  private Font headerFont;
  private Font itemsFont;
  private Item firstShownItem;
  private Item lastShownItem;
  private GuiToolbarWidgets toolBarWidgets;

  private static ContextDialog activeInstance;

  private enum OwnerType {
    CATEGORY,
    ITEM,
  }

  private class CategoryAndOrder {
    String category;
    String order;
    boolean collapsed;

    public CategoryAndOrder(String category, String order, boolean collapsed) {
      this.category = category;
      this.order = order;
      this.collapsed = collapsed;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      CategoryAndOrder that = (CategoryAndOrder) o;
      return category.equals(that.category);
    }

    @Override
    public int hashCode() {
      return Objects.hash(category);
    }

    /**
     * Gets category
     *
     * @return value of category
     */
    public String getCategory() {
      return category;
    }

    /**
     * @param category The category to set
     */
    public void setCategory(String category) {
      this.category = category;
    }

    /**
     * Gets order
     *
     * @return value of order
     */
    public String getOrder() {
      return order;
    }

    /**
     * @param order The order to set
     */
    public void setOrder(String order) {
      this.order = order;
    }

    /**
     * Gets collapsed
     *
     * @return value of collapsed
     */
    public boolean isCollapsed() {
      return collapsed;
    }

    /**
     * @param collapsed The collapsed to set
     */
    public void setCollapsed(boolean collapsed) {
      this.collapsed = collapsed;
    }

    public void flipCollapsed() {
      collapsed = !collapsed;
    }
  }

  private List<CategoryAndOrder> categories;

  private static class Item {
    private final GuiAction action;
    private final Image image;
    private boolean selected;
    private AreaOwner areaOwner;

    public Item(GuiAction action, Image image) {
      this.action = action;
      this.image = image;
      this.selected = false;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Item item = (Item) o;
      return Objects.equals(action, item.action);
    }

    @Override
    public int hashCode() {
      return Objects.hash(action);
    }

    public GuiAction getAction() {
      return action;
    }

    public String getText() {
      return action.getShortName();
    }

    public Image getImage() {
      return image;
    }

    /**
     * Gets selected
     *
     * @return value of selected
     */
    public boolean isSelected() {
      return selected;
    }

    /**
     * @param selected The selected to set
     */
    public void setSelected(boolean selected) {
      this.selected = selected;
    }

    /**
     * Gets areaOwner
     *
     * @return value of areaOwner
     */
    public AreaOwner getAreaOwner() {
      return areaOwner;
    }

    /**
     * @param areaOwner The areaOwner to set
     */
    public void setAreaOwner(AreaOwner areaOwner) {
      this.areaOwner = areaOwner;
    }
  }

  public ContextDialog(
      Shell parent, String title, Point location, List<GuiAction> actions, String contextId) {
    this(parent, title, location, actions, contextId, null);
  }

  public ContextDialog(
      Shell parent,
      String title,
      Point location,
      List<GuiAction> actions,
      String contextId,
      Supplier<List<GuiAction>> actionsSupplier) {
    super(parent);

    this.setText(title);
    this.location = location;
    this.actions = actions;
    this.actionsSupplier = actionsSupplier;

    props = PropsUi.getInstance();

    shiftClicked = false;
    ctrlClicked = false;

    // Make the icons a bit smaller to fit more
    //
    iconSize = (int) Math.round(props.getZoomFactor() * props.getIconSize() * 0.75);
    margin = PropsUi.getMargin();
    highlightColor = new Color(parent.getDisplay(), props.contrastColor(201, 232, 251));
  }

  public GuiAction open() {
    shell = new Shell(getParent(), SWT.DIALOG_TRIM | SWT.RESIZE);
    shell.setText(getText());
    shell.setMinimumSize(200, 180);
    shell.setImage(GuiResource.getInstance().getImageHop());
    shell.setLayout(new FormLayout());

    xMargin = 3 * margin;
    yMargin = 2 * margin;

    rebuildCategoriesAndItems();

    // Add a search bar at the top...
    //
    Composite searchComposite = new Composite(shell, SWT.NONE);
    searchComposite.setLayout(new GridLayout(3, false));
    PropsUi.setLook(searchComposite, Props.WIDGET_STYLE_TOOLBAR);
    FormData fdlSearchComposite = new FormData();
    fdlSearchComposite.top = new FormAttachment(0, 0);
    fdlSearchComposite.left = new FormAttachment(0, 0);
    fdlSearchComposite.right = new FormAttachment(100, 0);
    searchComposite.setLayoutData(fdlSearchComposite);

    Label wlSearch = new Label(searchComposite, SWT.LEFT);
    wlSearch.setText(BaseMessages.getString(PKG, "ContextDialog.Search.Label.Text"));
    PropsUi.setLook(wlSearch, Props.WIDGET_STYLE_TOOLBAR);

    wSearch =
        new Text(
            searchComposite,
            SWT.LEFT | SWT.BORDER | SWT.SINGLE | SWT.SEARCH | SWT.ICON_SEARCH | SWT.ICON_CANCEL);
    wSearch.setLayoutData(new GridData(GridData.FILL_BOTH));
    PropsUi.setLook(wSearch, Props.WIDGET_STYLE_TOOLBAR);

    // Create a toolbar at the right of the search bar...
    //
    IToolbarContainer toolBarContainer =
        ToolbarFacade.createToolbarContainer(searchComposite, SWT.WRAP | SWT.LEFT | SWT.HORIZONTAL);
    Control toolBar = toolBarContainer.getControl();
    toolBarWidgets = new GuiToolbarWidgets();
    toolBarWidgets.registerGuiPluginObject(this);
    toolBarWidgets.createToolbarWidgets(toolBarContainer, GUI_PLUGIN_TOOLBAR_PARENT_ID);
    toolBar.pack();
    PropsUi.setLook(toolBar, Props.WIDGET_STYLE_TOOLBAR);

    recallToolbarSettings();

    // Add a description label at the bottom...
    //
    Composite wTooltipComposite = new Composite(shell, SWT.NONE);
    GridLayout gdlTooltipComposite = new GridLayout(1, false);
    gdlTooltipComposite.marginLeft = PropsUi.getFormMargin();
    gdlTooltipComposite.marginRight = PropsUi.getFormMargin();
    gdlTooltipComposite.marginTop = PropsUi.getFormMargin();
    gdlTooltipComposite.marginBottom = PropsUi.getFormMargin();
    wTooltipComposite.setLayout(new GridLayout(1, false));
    PropsUi.setLook(wTooltipComposite, Props.WIDGET_STYLE_TOOLBAR);

    FormData fdlTooltip = new FormData();
    fdlTooltip.left = new FormAttachment(0, 0);
    fdlTooltip.right = new FormAttachment(100, 0);
    fdlTooltip.top = new FormAttachment(100, -(int) (props.getZoomFactor() * 50));
    fdlTooltip.bottom = new FormAttachment(100, 0);
    wTooltipComposite.setLayoutData(fdlTooltip);

    wlTooltip = new Label(wTooltipComposite, SWT.LEFT);
    wlTooltip.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));
    PropsUi.setLook(wlTooltip, Props.WIDGET_STYLE_TOOLBAR);

    // The rest of the dialog is used to draw the actions...
    //
    wScrolledComposite = new ScrolledComposite(shell, SWT.V_SCROLL);
    wCanvas = new Canvas(wScrolledComposite, SWT.NO_BACKGROUND | SWT.DOUBLE_BUFFERED);
    wScrolledComposite.setContent(wCanvas);
    FormData fdCanvas = new FormData();
    fdCanvas.left = new FormAttachment(0, 0);
    fdCanvas.right = new FormAttachment(100, 0);
    fdCanvas.top = new FormAttachment(searchComposite, 0);
    fdCanvas.bottom = new FormAttachment(wTooltipComposite, 0);
    wScrolledComposite.setLayoutData(fdCanvas);
    // Expand + min size is the reliable ScrolledComposite/RAP pattern; content height is
    // measured in onPaint and applied via setMinHeight / updateVerticalBar.
    wScrolledComposite.setExpandHorizontal(true);
    wScrolledComposite.setExpandVertical(true);

    itemsFont = GuiResource.getInstance().getFontDefault();

    int fontHeight = itemsFont.getFontData()[0].getHeight() + 1;
    headerFont =
        new Font(
            getParent().getDisplay(),
            props.getDefaultFont().getName(),
            fontHeight,
            props.getGraphFont().getStyle() | SWT.BOLD | SWT.ITALIC);

    // TODO: Calculate a more dynamic size based on number of actions, screen size
    // and so on
    //
    int width = (int) Math.round(800 * props.getZoomFactor());
    int height = (int) Math.round(600 * props.getZoomFactor());

    // Position the dialog where there was a click to be more intuitive
    //
    if (location != null) {
      /*Adapt to the monitor */
      Monitor monitor = shell.getMonitor();
      boolean fitOtherMonitors = false;
      for (Monitor monitorCheck : shell.getDisplay().getMonitors()) {
        org.eclipse.swt.graphics.Rectangle displayPositionCheck = monitorCheck.getBounds();
        if (((location.x - displayPositionCheck.x) <= monitorCheck.getClientArea().width - width)
            && (location.y - displayPositionCheck.y
                <= monitorCheck.getClientArea().height - height)) {
          fitOtherMonitors = true;
          break;
        }
        if (monitorCheck.getClientArea().contains(location.x, location.y)) {
          monitor = monitorCheck;
        }
      }
      org.eclipse.swt.graphics.Rectangle displayPosition = monitor.getBounds();
      // Make sure the dialog fits on the display
      if (width > displayPosition.width) {
        width = displayPosition.width;
      }
      if (height > displayPosition.height) {
        height = displayPosition.height;
      }
      if (!fitOtherMonitors) {
        if ((location.x - displayPosition.x) > monitor.getClientArea().width - width)
          location.x = (monitor.getClientArea().width + displayPosition.x) - width;
        if (location.y - displayPosition.y > monitor.getClientArea().height - height)
          location.y = (monitor.getClientArea().height + displayPosition.y) - height;
      }
      shell.setSize(width, height);
      shell.setLocation(location.x, location.y);
    } else {
      BaseTransformDialog.setSize(shell, width, height, false);
    }

    // Add all the listeners
    //

    // If the shell is re-sized we need to recalculate things...
    //
    shell.addListener(SWT.Resize, this::onResize);
    shell.addListener(SWT.Deactivate, event -> onFocusLost());
    shell.addListener(SWT.Close, event -> storeDialogSettings());

    wSearch.addListener(SWT.KeyDown, this::onKeyPressed);
    wSearch.addListener(SWT.Modify, event -> onModifySearch());
    wSearch.addListener(
        SWT.DefaultSelection,
        event -> {

          // Ignore this event
          //
          if (event.detail == SWT.ICON_SEARCH || event.detail == SWT.ICON_CANCEL) {
            return;
          }

          // Pressed enter
          //
          if (selectedItem != null) {
            selectedAction = selectedItem.getAction();
          }
          dispose();
        });

    wCanvas.addListener(SWT.KeyDown, this::onKeyPressed);
    wCanvas.addListener(SWT.Paint, this::onPaint);
    wCanvas.addListener(SWT.MouseDown, this::onMouseDown);
    wCanvas.addListener(SWT.MouseUp, this::onMouseUp);
    if (!EnvironmentUtils.getInstance().isWeb()) {
      wCanvas.addListener(SWT.MouseMove, this::onMouseMove);
    } else {
      // Hop Web: RAP does not deliver reliable mouse-move-while-pressed for Display filters.
      // Use HTML5-backed SWT DnD so the user can drag a create item onto the graph canvas.
      installWebPlacementDragSource();
    }

    // OS Specific listeners...
    //
    if (OsHelper.isMac()) {
      wCanvas.addListener(
          SWT.MouseVerticalWheel,
          event -> {
            org.eclipse.swt.graphics.Point origin = wScrolledComposite.getOrigin();
            origin.y -= event.count;
            wScrolledComposite.setOrigin(origin);
          });
    }

    // Layout all the widgets in the shell.
    //
    shell.layout();

    // Set the active instance.
    //
    activeInstance = this;

    // Manually set canvas size otherwise canvas never gets drawn.
    wCanvas.setSize(10, 10);

    // Show the dialog now
    //
    shell.open();

    // Filter all actions by default
    //
    this.filter(null);

    // Force focus on the search bar
    //
    wSearch.setFocus();

    // Wait until the dialog is closed
    //
    Display display = shell.getDisplay();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }

    activeInstance = null;

    // When automatic closing occurs upon loss of focus, we must help set the focus to the parent
    // (Widows only).
    if (focusLost) {
      getParent().setFocus();
    }

    return selectedAction;
  }

  /**
   * Gets the currently active instance
   *
   * @return The currently active instance or null if the dialog is not showing.
   */
  public static ContextDialog getInstance() {
    return activeInstance;
  }

  private void recallToolbarSettings() {
    Button categoriesCheckBox = getCategoriesCheckBox();
    if (categoriesCheckBox != null) {
      String strUseCategories = HopConfig.getGuiProperty(AUDIT_TYPE_TOOLBAR_SHOW_CATEGORIES);
      categoriesCheckBox.setSelection("Y".equalsIgnoreCase(Const.NVL(strUseCategories, "Y")));
    }

    Button fixedWidthCheckBox = getFixedWidthCheckBox();
    if (fixedWidthCheckBox != null) {
      String strUseFixedWidth = HopConfig.getGuiProperty(AUDIT_TYPE_TOOLBAR_FIXED_WIDTH);
      fixedWidthCheckBox.setSelection("Y".equalsIgnoreCase(Const.NVL(strUseFixedWidth, "Y")));
    }

    AuditState auditState =
        AuditManager.retrieveState(
            LogChannel.UI,
            HopNamespace.getNamespace(),
            AUDIT_TYPE_CONTEXT_DIALOG,
            AUDIT_NAME_CATEGORY_STATES);
    if (auditState != null) {
      Map<String, Object> states = auditState.getStateMap();
      for (CategoryAndOrder category : categories) {
        Object expanded = states.get(category.getCategory());
        if (expanded == null) {
          category.setCollapsed(false);
        } else {
          category.setCollapsed("N".equalsIgnoreCase(expanded.toString()));
        }
      }
    }
  }

  private void storeDialogSettings() {
    // Save the shell size and location in case the position isn't a mouse click
    //
    if (location == null) {
      props.setScreen(new WindowProperty(shell));
    }

    Button categoriesCheckBox = getCategoriesCheckBox();
    if (categoriesCheckBox != null) {
      HopConfig.setGuiProperty(
          AUDIT_TYPE_TOOLBAR_SHOW_CATEGORIES, categoriesCheckBox.getSelection() ? "Y" : "N");
    }

    Button fixedWidthCheckBox = getFixedWidthCheckBox();
    if (fixedWidthCheckBox != null) {
      HopConfig.setGuiProperty(
          AUDIT_TYPE_TOOLBAR_FIXED_WIDTH, fixedWidthCheckBox.getSelection() ? "Y" : "N");
    }

    try {
      HopConfig.getInstance().saveToFile();
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "ContextDialog.SaveConfig.Error.Dialog.Header"),
          BaseMessages.getString(PKG, "ContextDialog.SaveConfig.Error.Dialog.Message"),
          e);
    }

    // Store the category states: expanded or not
    //
    Map<String, Object> states = new HashMap<>();
    for (CategoryAndOrder category : categories) {
      states.put(category.getCategory(), category.isCollapsed() ? "N" : "Y");
    }
    AuditManager.storeState(
        LogChannel.UI,
        HopNamespace.getNamespace(),
        AUDIT_TYPE_CONTEXT_DIALOG,
        AUDIT_NAME_CATEGORY_STATES,
        states);
  }

  public boolean isDisposed() {
    return shell.isDisposed();
  }

  public void dispose() {
    if (shell == null || shell.isDisposed()) {
      return;
    }

    removePlacementArmFilters();

    // Store the toolbar settings
    storeDialogSettings();

    // Close the dialog window
    shell.close();

    // Do not dispose item images. They are cached by GuiResource so that they're only ever loaded
    // once.
    // There's no need to keep re-loading all the time.
    // Previously this cache was not functional so that we needed to dispose here.

    if (highlightColor != null && !highlightColor.isDisposed()) {
      highlightColor.dispose();
    }
    if (headerFont != null && !headerFont.isDisposed()) {
      headerFont.dispose();
    }
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_COLLAPSE_ALL,
      toolTip = "i18n::ContextDialog.GuiAction.CollapseCategories.Tooltip",
      image = "ui/images/collapse-all.svg")
  public void collapseAll() {
    for (CategoryAndOrder category : categories) {
      category.setCollapsed(true);
    }
    wCanvas.redraw();
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_EXPAND_ALL,
      toolTip = "i18n::ContextDialog.GuiAction.ExpandCategories.Tooltip",
      image = "ui/images/expand-all.svg")
  public void expandAll() {
    for (CategoryAndOrder category : categories) {
      category.setCollapsed(false);
    }
    wCanvas.redraw();
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_ENABLE_CATEGORIES,
      label = "i18n::ContextDialog.GuiAction.ShowCategories.Label",
      toolTip = "i18n::ContextDialog.GuiAction.ShowCategories.Tooltip",
      type = GuiToolbarElementType.CHECKBOX)
  public void enableDisableCategories() {
    wCanvas.redraw();
    wSearch.setFocus();
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_FIXED_WIDTH,
      label = "i18n::ContextDialog.GuiAction.FixedWidth.Label",
      toolTip = "i18n::ContextDialog.GuiAction.FixedWidth.Tooltip",
      type = GuiToolbarElementType.CHECKBOX)
  public void enableDisableFixedWidth() {
    wCanvas.redraw();
    wSearch.setFocus();
  }

  private Button getCategoriesCheckBox() {
    if (toolBarWidgets == null) {
      return null;
    }
    ToolItem checkboxItem = toolBarWidgets.findToolItem(TOOLBAR_ITEM_ENABLE_CATEGORIES);
    if (checkboxItem == null) {
      return null;
    }
    return (Button) checkboxItem.getControl();
  }

  private Button getFixedWidthCheckBox() {
    if (toolBarWidgets == null) {
      return null;
    }
    ToolItem checkboxItem = toolBarWidgets.findToolItem(TOOLBAR_ITEM_FIXED_WIDTH);
    if (checkboxItem == null) {
      return null;
    }
    return (Button) checkboxItem.getControl();
  }

  private void onMouseMove(Event event) {
    // Do we mouse over an action?
    //
    Item item = findItem(event.x, event.y);
    if (item != null) {
      selectItem(item, false);
    }
  }

  private void onMouseDown(Event event) {
    if (event.button != 1 || placementDrag) {
      return;
    }
    AreaOwner areaOwner = AreaOwner.getVisibleAreaOwner(areaOwners, event.x, event.y);
    if (areaOwner == null || areaOwner.getParent() != OwnerType.ITEM) {
      return;
    }
    Item item = (Item) areaOwner.getOwner();
    if (item == null || !GuiActionFavorites.isPlaceableCreateAction(item.getAction())) {
      return;
    }
    selectItem(item, false);
    // Native Hop GUI: arm Display-filter placement drag. Hop Web uses SWT DnD instead (see
    // installWebPlacementDragSource) because RAP does not deliver mouse-move-while-pressed.
    if (EnvironmentUtils.getInstance().isWeb()) {
      return;
    }
    pressItem = item;
    pressDisplayLocation = shell.getDisplay().getCursorLocation();
    installPlacementArmFilters();
  }

  private void onMouseUp(Event event) {
    if (placementDrag) {
      // Drag already committed; dialog is closing or closed.
      return;
    }
    removePlacementArmFilters();
    pressItem = null;
    pressDisplayLocation = null;

    AreaOwner areaOwner = AreaOwner.getVisibleAreaOwner(areaOwners, event.x, event.y);
    if (areaOwner == null) {
      return;
    }

    OwnerType ownerType = (OwnerType) areaOwner.getParent();
    switch (ownerType) {
      case CATEGORY:
        // Clicked on a category header: expand or unfold
        //
        CategoryAndOrder categoryAndOrder = (CategoryAndOrder) areaOwner.getOwner();
        categoryAndOrder.flipCollapsed();
        wCanvas.redraw();
        break;
      case ITEM:
        // See which item we clicked on...
        //
        Item item = (Item) areaOwner.getOwner();
        if (item != null) {
          // ALT-Click: toggle transform/action favorite without closing the dialog (issue #3526)
          //
          boolean altClicked = (event.stateMask & SWT.ALT) != 0;
          if (altClicked && GuiActionFavorites.tryToggleFromAction(item.getAction())) {
            try {
              HopConfig.getInstance().saveToFile();
            } catch (Exception e) {
              new ErrorDialog(
                  shell,
                  BaseMessages.getString(PKG, "ContextDialog.SaveConfig.Error.Dialog.Header"),
                  BaseMessages.getString(PKG, "ContextDialog.SaveConfig.Error.Dialog.Message"),
                  e);
            }
            GraphPalette.fireFavoritesChanged(HopGui.getInstance());
            refreshActionsFromSupplier();
            return;
          }

          selectedAction = item.getAction();

          shiftClicked = (event.stateMask & SWT.SHIFT) != 0;
          ctrlClicked =
              (event.stateMask & SWT.CONTROL) != 0
                  || (Const.isOSX() && (event.stateMask & SWT.COMMAND) != 0);

          dispose();
        }
        break;
      default:
        break;
    }
  }

  private void installPlacementArmFilters() {
    removePlacementArmFilters();
    Display display = shell.getDisplay();
    placementArmMoveFilter =
        event -> {
          if (event.type != SWT.MouseMove || pressItem == null || placementDrag) {
            return;
          }
          // Only commit while the primary button is still held (avoids stray move events).
          if ((event.stateMask & SWT.BUTTON1) == 0) {
            return;
          }
          if (shell.isDisposed()) {
            removePlacementArmFilters();
            return;
          }
          org.eclipse.swt.graphics.Point cursor = display.getCursorLocation();
          int dx = cursor.x - pressDisplayLocation.x;
          int dy = cursor.y - pressDisplayLocation.y;
          int thresholdSq = PLACEMENT_DRAG_THRESHOLD_PX * PLACEMENT_DRAG_THRESHOLD_PX;
          if (dx * dx + dy * dy > thresholdSq) {
            commitPlacementDrag(pressItem);
          }
        };
    placementArmUpFilter =
        event -> {
          if (event.type == SWT.MouseUp) {
            // Click path: dialog MouseUp will select. Clear arm state only.
            removePlacementArmFilters();
            pressItem = null;
            pressDisplayLocation = null;
          }
        };
    display.addFilter(SWT.MouseMove, placementArmMoveFilter);
    display.addFilter(SWT.MouseUp, placementArmUpFilter);
  }

  private void removePlacementArmFilters() {
    if (shell == null || shell.isDisposed()) {
      placementArmMoveFilter = null;
      placementArmUpFilter = null;
      return;
    }
    Display display = shell.getDisplay();
    if (placementArmMoveFilter != null) {
      display.removeFilter(SWT.MouseMove, placementArmMoveFilter);
      placementArmMoveFilter = null;
    }
    if (placementArmUpFilter != null) {
      display.removeFilter(SWT.MouseUp, placementArmUpFilter);
      placementArmUpFilter = null;
    }
  }

  private void commitPlacementDrag(Item item) {
    if (item == null || placementDrag) {
      return;
    }
    selectedAction = item.getAction();
    placementDrag = true;
    focusLost = false;
    shiftClicked = false;
    ctrlClicked = false;
    pressItem = null;
    pressDisplayLocation = null;
    removePlacementArmFilters();
    dispose();
  }

  /**
   * Hop Web: DragSource on the icon canvas so HTML5 DnD can carry a placeable create action to the
   * pipeline/workflow canvas DropTarget. The shell is hidden on dragStart (so the canvas is
   * visible) but kept alive until dragFinished so the DragSource remains valid.
   */
  private void installWebPlacementDragSource() {
    DragSource dragSource = new DragSource(wCanvas, DND.DROP_COPY);
    dragSource.setTransfer(new Transfer[] {TextTransfer.getInstance()});
    dragSource.addDragListener(
        new DragSourceAdapter() {
          @Override
          public void dragStart(DragSourceEvent event) {
            Item item = findItem(event.x, event.y);
            if (item == null || !GuiActionFavorites.isPlaceableCreateAction(item.getAction())) {
              event.doit = false;
              dndDragItem = null;
              return;
            }
            dndDragItem = item;
            selectItem(item, false);
            selectedAction = item.getAction();
            placementDrag = true;
            placementCompletedByDrop = false;
            focusLost = false;
            // Prefer the item icon as drag image; fall back to Hop logo on web if needed.
            if (item.getImage() != null && !item.getImage().isDisposed()) {
              event.image = item.getImage();
            } else {
              event.image = GuiResource.getInstance().getImageHop();
            }
            // Hide (do not dispose) so the graph canvas is usable while the DragSource stays alive.
            if (shell != null && !shell.isDisposed()) {
              shell.setVisible(false);
            }
            event.doit = true;
          }

          @Override
          public void dragSetData(DragSourceEvent event) {
            if (TextTransfer.getInstance().isSupportedType(event.dataType) && dndDragItem != null) {
              event.data = ContextDialogPlacement.encode(dndDragItem.getAction());
              event.doit = event.data != null;
            }
          }

          @Override
          public void dragFinished(DragSourceEvent event) {
            dndDragItem = null;
            // End the modal open() loop. If the drop already created the item,
            // GuiContextUtil will see placementCompletedByDrop and skip a second create.
            if (selectedAction == null && !placementCompletedByDrop) {
              // Drag cancelled without a selection — treat as focus-lost style cancel.
              placementDrag = false;
            }
            dispose();
          }
        });
  }

  /** Called by canvas drop targets when a web DnD drop successfully placed a transform/action. */
  public void markPlacementCompletedByDrop() {
    placementCompletedByDrop = true;
    placementDrag = true;
    focusLost = false;
  }

  /**
   * @return true if a canvas DropTarget already handled creation for this placement gesture
   */
  public boolean isPlacementCompletedByDrop() {
    return placementCompletedByDrop;
  }

  /**
   * Rebuild the category list and icon items from the current {@link #actions} list. Preserves
   * collapsed state of categories when refreshing after a favorites toggle.
   */
  private void rebuildCategoriesAndItems() {
    Map<String, Boolean> previousCollapsed = new HashMap<>();
    if (categories != null) {
      for (CategoryAndOrder category : categories) {
        previousCollapsed.put(category.getCategory(), category.isCollapsed());
      }
    }

    categories = new ArrayList<>();
    for (GuiAction action : actions) {
      CategoryAndOrder categoryAndOrder;
      if (StringUtils.isNotEmpty(action.getCategory())) {
        categoryAndOrder =
            new CategoryAndOrder(
                action.getCategory(), Const.NVL(action.getCategoryOrder(), "0"), false);
      } else {
        categoryAndOrder = new CategoryAndOrder(CATEGORY_OTHER, "9999", false);
      }
      if (!categories.contains(categoryAndOrder)) {
        Boolean wasCollapsed = previousCollapsed.get(categoryAndOrder.getCategory());
        if (wasCollapsed != null) {
          categoryAndOrder.setCollapsed(wasCollapsed);
        }
        categories.add(categoryAndOrder);
      }
    }

    categories.sort(Comparator.comparing(o -> o.order));

    int correctedIconSize = (int) (iconSize / props.getZoomFactor());
    Display display = shell != null && !shell.isDisposed() ? shell.getDisplay() : null;

    items.clear();
    for (GuiAction action : actions) {
      ClassLoader classLoader = action.getClassLoader();
      if (classLoader == null) {
        classLoader = ClassLoader.getSystemClassLoader();
      }
      Image image;
      try {
        image =
            GuiResource.getInstance()
                .getImage(action.getImage(), classLoader, correctedIconSize, correctedIconSize);
      } catch (Exception e) {
        if (display != null) {
          image =
              GuiResource.getInstance()
                  .getSwtImageMissing()
                  .getAsBitmapForSize(display, correctedIconSize, correctedIconSize);
        } else {
          image = null;
        }
      }
      items.add(new Item(action, image));
    }
  }

  /**
   * Reload actions from the supplier (after a favorite toggle) and re-apply the current search
   * filter so the Favorites category appears/disappears without closing the dialog.
   */
  private void refreshActionsFromSupplier() {
    if (actionsSupplier == null) {
      return;
    }
    String selectedName = selectedItem != null ? selectedItem.getAction().getName() : null;
    actions = actionsSupplier.get();
    rebuildCategoriesAndItems();
    String searchText = wSearch != null && !wSearch.isDisposed() ? wSearch.getText() : "";
    filter(searchText);

    // Try to re-select the same tool by name after the list refresh
    //
    if (selectedName != null) {
      for (Item item : filteredItems) {
        if (selectedName.equals(item.getAction().getName())) {
          selectItem(item, false);
          break;
        }
      }
    }
  }

  private void onResize(Event event) {
    // Width changes reflow icons and change total content height; force a full remeasure.
    previousTotalContentHeight = 0;
    if (wCanvas != null && !wCanvas.isDisposed()) {
      wCanvas.redraw();
    }
    updateVerticalBar();
  }

  /**
   * This is where all the actions are drawn
   *
   * @param event
   */
  private void onPaint(Event event) {

    GC gc = event.gc;

    org.eclipse.swt.graphics.Rectangle area = wScrolledComposite.getClientArea();
    org.eclipse.swt.graphics.Rectangle canvas = wCanvas.getBounds();

    boolean useCategories;
    Button categoriesCheckBox = getCategoriesCheckBox();
    if (categoriesCheckBox == null) {
      useCategories = true;
    } else {
      useCategories = categoriesCheckBox.getSelection();
    }
    useCategories &= !categories.isEmpty();

    boolean useFixedWidth;
    Button fixedWidthCheckBox = getFixedWidthCheckBox();
    if (fixedWidthCheckBox == null) {
      useFixedWidth = false;
    } else {
      useFixedWidth = fixedWidthCheckBox.getSelection();
    }

    updateToolbar();

    // Fill everything with white...
    //
    gc.setForeground(GuiResource.getInstance().getColorBlack());
    gc.setBackground(GuiResource.getInstance().getColorBackground());
    gc.fillRectangle(0, 0, canvas.width, canvas.height);

    // For text and lines...
    //
    gc.setForeground(GuiResource.getInstance().getColorBlack());
    gc.setLineWidth(1);

    // Remember the area owners
    //
    areaOwners = new ArrayList<>();

    // Draw all actions
    // Loop over the categories, if any...
    //
    int height = 0; // should always be about the same
    int categoryNr = 0;
    int x = margin;
    int y = margin;

    firstShownItem = null;

    while ((useCategories && categoryNr < categories.size())
        || (!useCategories || categories.isEmpty()) && (categoryNr == 0)) {

      CategoryAndOrder categoryAndOrder;
      if (!useCategories || categories.isEmpty()) {
        categoryAndOrder = null;
      } else {
        categoryAndOrder = categories.get(categoryNr);
      }

      // Get the list of actions for the given categoryAndOrder
      //
      List<Item> itemsToPaint = findItemsForCategory(categoryAndOrder);

      if (!itemsToPaint.isEmpty()) {
        if (categoryAndOrder != null) {
          // Draw the category header
          //
          gc.setFont(headerFont);
          if (categoryAndOrder.isCollapsed()) {
            gc.setForeground(GuiResource.getInstance().getColorDarkGray());
          } else {
            gc.setForeground(GuiResource.getInstance().getColorBlack());
          }
          org.eclipse.swt.graphics.Point categoryExtent = gc.textExtent(categoryAndOrder.category);
          gc.drawText(categoryAndOrder.category, x, y);
          areaOwners.add(
              new AreaOwner(
                  AreaOwner.AreaType.CUSTOM,
                  x,
                  y,
                  categoryExtent.x,
                  categoryExtent.y,
                  new DPoint(0, 0),
                  OwnerType.CATEGORY,
                  categoryAndOrder));
          y += categoryExtent.y + yMargin;
          gc.setLineWidth(1);
          gc.drawLine(margin, y - yMargin, area.width - xMargin, y - yMargin);
        }

        gc.setForeground(GuiResource.getInstance().getColorBlack());
        gc.setFont(itemsFont);

        if (categoryAndOrder == null || !categoryAndOrder.isCollapsed()) {

          Map<GuiAction, ActionDetails> detailsMap = new HashMap<>();

          // Calculate sizes...
          //
          for (Item item : itemsToPaint) {
            ActionDetails details = new ActionDetails();
            details.name = Const.NVL(item.action.getName(), item.action.getId());
            details.imageBounds = item.image.getBounds();
            details.nameExtent = gc.textExtent(details.name);
            details.width = Math.max(details.nameExtent.x, details.imageBounds.width);
            details.height = details.nameExtent.y + margin + details.imageBounds.height;
            detailsMap.put(item.action, details);
          }

          // If we have a fixed width, simply unify the width
          //
          if (useFixedWidth) {
            int maxWidth = 0;
            for (ActionDetails details : detailsMap.values()) {
              maxWidth = Math.max(maxWidth, details.width);
            }
            for (ActionDetails details : detailsMap.values()) {
              details.width = maxWidth;
            }
          }

          // Paint the action items
          //
          for (Item item : itemsToPaint) {
            ActionDetails details = detailsMap.get(item.action);

            lastShownItem = item;
            if (firstShownItem == null) {
              firstShownItem = item;
            }

            int width = details.width;
            height = details.height;

            if (x + width + xMargin > area.width) {
              x = margin;
              y += height + yMargin;
            }

            if (item.isSelected()) {
              gc.setLineWidth(2);
              gc.setBackground(highlightColor);
              gc.fillRoundRectangle(
                  x - xMargin / 2,
                  y - yMargin / 2,
                  width + xMargin,
                  height + yMargin,
                  margin,
                  margin);
            }

            // So we draw the icon in the centre of the width...
            //
            int imageMargin = (width - details.imageBounds.width) / 2;
            gc.drawImage(item.getImage(), x + imageMargin, y);

            // Then we draw the text underneath
            //
            int textMargin = (width - details.nameExtent.x) / 2;
            gc.drawText(details.name, x + textMargin, y + details.imageBounds.height + margin);

            // Reset the background color
            //
            gc.setLineWidth(1);
            gc.setBackground(GuiResource.getInstance().getColorBackground());

            // The drawn area is the complete rectangle
            //
            AreaOwner areaOwner =
                new AreaOwner(
                    AreaOwner.AreaType.CUSTOM,
                    x,
                    y,
                    width,
                    height,
                    new DPoint(0, 0),
                    OwnerType.ITEM,
                    item);
            areaOwners.add(areaOwner);
            item.setAreaOwner(areaOwner);

            // Now we advance x and y to where we want to draw the next one...
            //
            x += width + xMargin;
            if (x > area.width) {
              x = margin;
              y += height + yMargin;
            }
          }

          // Back to the left on a next line to draw the next category (if any)
          //
          x = margin;
          y += height + yMargin;
        } else {
          y -= yMargin; // tighter together when collapsed
        }
      }

      // Pick the next category
      //
      categoryNr++;
      if (!itemsToPaint.isEmpty()) {
        y += yMargin;
      }
    }

    totalContentHeight = Math.max(area.height, y);

    // Content size is only known after paint. Resize the canvas and refresh the scrollbar here.
    // updateVerticalBar() used to run only from filter()/resize *before* the first paint, so on
    // Hop Web (RAP) the scroll range could stay stale and truncate the list mid-way (#7868).
    int canvasWidth = wCanvas.getBounds().width;
    if (previousTotalContentHeight != totalContentHeight || canvasWidth != area.width) {
      previousTotalContentHeight = totalContentHeight;
      wCanvas.setSize(area.width, totalContentHeight);
      wScrolledComposite.setMinWidth(area.width);
      wScrolledComposite.setMinHeight(totalContentHeight);
      updateVerticalBar();
    }
  }

  private void updateToolbar() {
    if (toolBarWidgets == null) {
      return;
    }
    Button categoriesCheckBox = getCategoriesCheckBox();
    boolean categoriesEnabled = categoriesCheckBox != null && categoriesCheckBox.getSelection();
    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_COLLAPSE_ALL, categoriesEnabled);
    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_EXPAND_ALL, categoriesEnabled);
  }

  private List<Item> findItemsForCategory(CategoryAndOrder categoryAndOrder) {
    List<Item> list = new ArrayList<>();
    for (Item filteredItem : filteredItems) {
      if (categoryAndOrder == null
          || categoryAndOrder.category.equalsIgnoreCase(filteredItem.action.getCategory())) {
        list.add(filteredItem);
      } else if (CATEGORY_OTHER.equals(categoryAndOrder.category)
          && StringUtils.isEmpty(filteredItem.action.getCategory())) {
        list.add(filteredItem);
      }
    }
    return list;
  }

  private void selectItem(Item selectedItem, boolean scroll) {

    for (Item item : items) {
      item.setSelected(false);
    }

    if (selectedItem == null) {
      wlTooltip.setText("");
    } else {

      this.selectedItem = selectedItem;
      wlTooltip.setText(Const.NVL(selectedItem.getAction().getTooltip(), ""));
      selectedItem.setSelected(true);

      // See if we need to show the selected item.
      //
      if (!EnvironmentUtils.getInstance().isWeb() && scroll && totalContentHeight > 0) {
        Rectangle itemArea = selectedItem.getAreaOwner().getArea();
        org.eclipse.swt.graphics.Rectangle clientArea = wScrolledComposite.getClientArea();

        ScrollBar verticalBar = wScrolledComposite.getVerticalBar();
        // Scroll down
        //
        while (itemArea.y + itemArea.height + 2 * yMargin
            > verticalBar.getSelection() + clientArea.height) {
          wScrolledComposite.setOrigin(
              0,
              Math.min(
                  verticalBar.getSelection() + verticalBar.getPageIncrement(),
                  verticalBar.getMaximum() - verticalBar.getThumb()));
        }

        // Scroll up
        //
        while (itemArea.y < verticalBar.getSelection()) {
          wScrolledComposite.setOrigin(
              0, Math.max(verticalBar.getSelection() - verticalBar.getPageIncrement(), 0));
        }
      }
    }

    wCanvas.redraw();
  }

  /**
   * Gets the search text widget
   *
   * @return the search text widget
   */
  public Text getSearchTextWidget() {
    return wSearch;
  }

  /**
   * The filtered item whose name is exactly the search text, ignoring case, or null if there is
   * none.
   *
   * <p>Relevance scoring on its own does not put it first: searching for "Null if" scores "If null"
   * higher, and "Table input" scores "Spark lake table input" higher, so typing a transform's full
   * name and pressing Enter gave you a different transform - and one whose name shares words with
   * others could not be selected by typing at all when it landed in another category.
   */
  private Item exactNameMatch(String text) {
    for (Item item : filteredItems) {
      if (isExactName(item.getAction().getName(), text)) {
        return item;
      }
    }
    return null;
  }

  /** Whether a name is exactly what was searched for, ignoring case and surrounding space. */
  static boolean isExactName(String name, String searchText) {
    String wanted = Const.trim(searchText);
    return StringUtils.isNotEmpty(wanted) && wanted.equalsIgnoreCase(Const.trim(name));
  }

  public void filter(String text) {

    if (text == null) {
      text = "";
    }

    filteredItems.clear();
    if (StringUtils.isEmpty(text)) {
      filteredItems.addAll(items);
    } else {
      // Score every action with the shared matcher (fuzzy + multi-term), keep the matches and sort
      // them best-first.
      SearchMatcher matcher = new SearchMatcher(text, false, false, true);
      Map<Item, Double> scores = new IdentityHashMap<>();
      for (Item item : items) {
        double score = item.getAction().matchScore(matcher);
        if (score > 0.0) {
          scores.put(item, score);
          filteredItems.add(item);
        }
      }
      filteredItems.sort((a, b) -> Double.compare(scores.get(b), scores.get(a)));
    }

    // An item called exactly what was typed comes first, whatever it scored.
    //
    Item exactMatch = exactNameMatch(text);
    if (exactMatch != null) {
      filteredItems.remove(exactMatch);
      filteredItems.add(0, exactMatch);
    }

    if (filteredItems.isEmpty()) {
      selectItem(null, false);
    }

    // Typing something's full name selects that thing, even if the selection was already on a
    // result that survived the narrowing.
    //
    else if (exactMatch != null) {
      selectItem(exactMatch, false);
    }

    // if selected item is exclude, change to a new default selection: first in the list
    //
    else if (!filteredItems.contains(selectedItem)) {
      selectItem(filteredItems.get(0), false);
    }

    // Update vertical bar
    //
    this.updateVerticalBar();

    wCanvas.redraw();
  }

  private void onFocusLost() {
    // Placement drag closes the dialog intentionally; do not treat as cancel.
    if (placementDrag || selectedAction != null) {
      return;
    }
    focusLost = true;
    dispose();
  }

  /**
   * @return true if the dialog closed because the user started dragging a placeable create item
   *     onto the canvas (issue #3111)
   */
  public boolean isPlacementDrag() {
    return placementDrag;
  }

  private void onModifySearch() {
    String text = wSearch.getText();
    this.filter(text);
  }

  private synchronized void onKeyPressed(Event event) {

    if (filteredItems.isEmpty()) {
      return;
    }
    if (shell.isDisposed() || !shell.isVisible()) {
      return;
    }

    // Which item area are we currently using as a base...
    //
    org.apache.hop.core.gui.Rectangle area = null;

    if (selectedItem == null) {
      // Select the first shown item
      if (firstShownItem != null) {
        area = firstShownItem.getAreaOwner().getArea();
      }
    } else {
      if (selectedItem.getAreaOwner() != null) {
        area = selectedItem.getAreaOwner().getArea();
      }
    }

    switch (event.keyCode) {
      case SWT.ARROW_DOWN:
        selectItemDown(area);
        break;
      case SWT.ARROW_UP:
        selectItemUp(area);
        break;
      case SWT.PAGE_UP:
        selectItemPageUp(area);
        break;
      case SWT.PAGE_DOWN:
        selectItemPageDown(area);
        break;
      case SWT.ARROW_LEFT:
        selectItemLeft(area);
        break;
      case SWT.ARROW_RIGHT:
        selectItemRight(area);
        break;
      case SWT.HOME:
        selectItem(firstShownItem, true);
        break;
      case SWT.END:
        selectItem(lastShownItem, true);
        break;
      default:
        break;
    }
  }

  private void selectClosest(Rectangle area, List<AreaOwner> areas) {
    // Sort by distance...
    //
    areas.sort((o1, o2) -> (int) (o1.getArea().distance(area) - o2.getArea().distance(area)));

    if (!areas.isEmpty()) {
      Item item = (Item) areas.get(0).getOwner();
      selectItem(item, true);
    }
  }

  /**
   * Find an area owner directly to the right of the area
   *
   * @param area
   */
  private void selectItemRight(Rectangle area) {
    List<AreaOwner> rightAreas = new ArrayList<>();
    for (AreaOwner areaOwner : areaOwners) {
      if (areaOwner.getOwner() instanceof Item) {
        // Only keep the items to the left
        //
        Rectangle r = areaOwner.getArea();
        if (r.x > area.x + area.width && r.y - 2 * yMargin < area.y && r.y + 2 * yMargin > area.y) {
          rightAreas.add(areaOwner);
        }
      }
    }
    selectClosest(area, rightAreas);
  }

  /**
   * Find an area owner directly to the left of the area
   *
   * @param area
   */
  private void selectItemLeft(Rectangle area) {
    List<AreaOwner> leftAreas = new ArrayList<>();
    for (AreaOwner areaOwner : areaOwners) {
      if (areaOwner.getOwner() instanceof Item) {
        // Only keep the items to the left
        //
        Rectangle r = areaOwner.getArea();
        if (r.x < area.x && r.y - 2 * yMargin < area.y && r.y + 2 * yMargin > area.y) {
          // Select only in the same band of items
          //
          leftAreas.add(areaOwner);
        }
      }
    }
    selectClosest(area, leftAreas);
  }

  /**
   * Find an area owner directly to the top of the area
   *
   * @param area
   */
  private void selectItemUp(Rectangle area) {
    List<AreaOwner> topAreas = new ArrayList<>();
    for (AreaOwner areaOwner : areaOwners) {
      if (areaOwner.getOwner() instanceof Item && areaOwner.getArea().y < area.y) {
        // Only keep the items to the left
        //
        topAreas.add(areaOwner);
      }
    }
    selectClosest(area, topAreas);
  }

  /**
   * Find an area owner directly to the bottom of the area
   *
   * @param area
   */
  private void selectItemDown(Rectangle area) {
    List<AreaOwner> bottomAreas = new ArrayList<>();
    for (AreaOwner areaOwner : areaOwners) {
      if (areaOwner.getOwner() instanceof Item && areaOwner.getArea().y > area.y + area.height) {
        // Only keep the items to the left
        //
        bottomAreas.add(areaOwner);
      }
    }
    selectClosest(area, bottomAreas);
  }

  private void selectItemPageUp(Rectangle area) {
    ScrollBar verticalBar = wScrolledComposite.getVerticalBar();
    List<AreaOwner> topAreas = new ArrayList<>();
    for (AreaOwner areaOwner : areaOwners) {
      if (areaOwner.getOwner() instanceof Item
          && areaOwner.getArea().y < area.y - verticalBar.getPageIncrement()) {
        // Only keep the items to the left
        //
        topAreas.add(areaOwner);
      }
    }
    if (topAreas.isEmpty()) topAreas.add(firstShownItem.getAreaOwner());

    selectClosest(area, topAreas);
  }

  private void selectItemPageDown(Rectangle area) {
    ScrollBar verticalBar = wScrolledComposite.getVerticalBar();
    List<AreaOwner> bottomAreas = new ArrayList<>();
    for (AreaOwner areaOwner : areaOwners) {
      if (areaOwner.getOwner() instanceof Item) {
        // Only keep the items to the left
        //
        Rectangle r = areaOwner.getArea();
        if (r.y > area.y + area.height + verticalBar.getPageIncrement()) {
          bottomAreas.add(areaOwner);
        }
      }
    }

    if (bottomAreas.isEmpty()) bottomAreas.add(lastShownItem.getAreaOwner());

    selectClosest(area, bottomAreas);
  }

  private void updateVerticalBar() {
    if (wScrolledComposite == null || wScrolledComposite.isDisposed()) {
      return;
    }
    ScrollBar verticalBar = wScrolledComposite.getVerticalBar();
    if (verticalBar == null || verticalBar.isDisposed()) {
      return;
    }
    org.eclipse.swt.graphics.Rectangle clientArea = wScrolledComposite.getClientArea();

    // Prefer the height measured in onPaint; canvas bounds can still be the dummy 10x10 size
    // when filter() runs before the first paint.
    int contentHeight = totalContentHeight;
    if (contentHeight <= 0 && wCanvas != null && !wCanvas.isDisposed()) {
      contentHeight = wCanvas.getBounds().height;
    }

    if (contentHeight <= clientArea.height) {
      verticalBar.setEnabled(false);
      verticalBar.setVisible(false);
    } else {
      verticalBar.setEnabled(true);
      verticalBar.setVisible(true);

      verticalBar.setMinimum(0);
      verticalBar.setMaximum(contentHeight);

      // Thumb is the visible portion of the content (pixels).
      // Note: RAP ScrollBar has no setPageIncrement/setIncrement — do not call them here.
      verticalBar.setThumb(Math.min(clientArea.height, contentHeight));
    }
  }

  private Item findItem(int x, int y) {

    for (AreaOwner areaOwner : areaOwners) {
      if (areaOwner.contains(x, y) && areaOwner.getOwner() instanceof Item item) {
        return item;
      }
    }

    return null;
  }

  /**
   * Gets shiftClicked
   *
   * @return value of shiftClicked
   */
  public boolean isShiftClicked() {
    return shiftClicked;
  }

  /**
   * @param shiftClicked The shiftClicked to set
   */
  public void setShiftClicked(boolean shiftClicked) {
    this.shiftClicked = shiftClicked;
  }

  /**
   * Gets ctrlClicked
   *
   * @return value of ctrlClicked
   */
  public boolean isCtrlClicked() {
    return ctrlClicked;
  }

  /**
   * @param ctrlClicked The ctrlClicked to set
   */
  public void setCtrlClicked(boolean ctrlClicked) {
    this.ctrlClicked = ctrlClicked;
  }

  /**
   * Gets focusLost
   *
   * @return value of focusLost
   */
  public boolean isFocusLost() {
    return focusLost;
  }

  /**
   * @param focusLost The focusLost to set
   */
  public void setFocusLost(boolean focusLost) {
    this.focusLost = focusLost;
  }
}
