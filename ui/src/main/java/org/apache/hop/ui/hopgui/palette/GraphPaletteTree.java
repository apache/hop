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

package org.apache.hop.ui.hopgui.palette;

import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.gui.Point;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.bus.HopGuiEvents;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.TreeMemory;
import org.apache.hop.ui.hopgui.context.ContextDialogPlacement;
import org.apache.hop.ui.hopgui.context.GuiActionFavorites;
import org.apache.hop.ui.hopgui.context.GuiActionFavorites.Kind;
import org.apache.hop.ui.hopgui.palette.GraphPaletteModel.Category;
import org.apache.hop.ui.hopgui.palette.GraphPaletteModel.Item;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.dnd.DND;
import org.eclipse.swt.dnd.DragSource;
import org.eclipse.swt.dnd.DragSourceAdapter;
import org.eclipse.swt.dnd.DragSourceEvent;
import org.eclipse.swt.dnd.TextTransfer;
import org.eclipse.swt.dnd.Transfer;
import org.eclipse.swt.graphics.GC;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.MenuItem;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.ToolItem;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeItem;

/** Spoon-style categorized tree of transforms or workflow actions (issue #7114). */
public class GraphPaletteTree extends Composite {

  private static final Class<?> PKG = GraphPaletteTree.class;

  private static final String TREE_MEMORY_PIPELINE = "PipelinePaletteTree";
  private static final String TREE_MEMORY_WORKFLOW = "WorkflowPaletteTree";

  private final IGraphPaletteHost host;
  private final String treeMemoryName;
  private final String eventGuiId;

  private Text filterText;
  private Tree tree;
  private GraphPaletteModel model;
  private boolean populated;
  private boolean dirty = true;
  private boolean shiftHeld;
  private boolean altHeld;
  private Listener shiftKeyFilter;
  private Image dragImage;

  public GraphPaletteTree(Composite parent, IGraphPaletteHost host) {
    super(parent, SWT.NONE);
    this.host = host;
    this.treeMemoryName =
        host.getPaletteKind() == Kind.TRANSFORM ? TREE_MEMORY_PIPELINE : TREE_MEMORY_WORKFLOW;
    this.eventGuiId = "GraphPaletteTree-" + host.getPaletteHostId();

    FormLayout layout = new FormLayout();
    layout.marginWidth = 0;
    layout.marginHeight = 0;
    setLayout(layout);
    PropsUi.setLook(this);

    createFilter();
    ToolBar toolBar = createToolbar();
    createTree(toolBar);
    createContextMenu();
    createDragSource();

    host.getHopGui()
        .getEventsHandler()
        .addEventListener(eventGuiId, e -> asyncRefresh(), HopGuiEvents.FavoritesChanged.name());

    addDisposeListener(e -> host.getHopGui().getEventsHandler().removeEventListeners(eventGuiId));
  }

  private void createFilter() {
    filterText = new Text(this, SWT.SEARCH | SWT.ICON_SEARCH | SWT.ICON_CANCEL);
    PropsUi.setLook(filterText);
    filterText.setMessage(
        BaseMessages.getString(
            PKG,
            host.getPaletteKind() == Kind.TRANSFORM
                ? "GraphPalette.Filter.Transforms.Placeholder"
                : "GraphPalette.Filter.Actions.Placeholder"));
    FormData fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(0, 0);
    fd.right = new FormAttachment(100, 0);
    filterText.setLayoutData(fd);
    filterText.addListener(SWT.Modify, e -> rebuildTree());
    filterText.addListener(
        SWT.DefaultSelection,
        e -> {
          if (e.detail == SWT.ICON_CANCEL) {
            clearFilter();
          }
        });
  }

  private ToolBar createToolbar() {
    ToolBar toolBar = new ToolBar(this, SWT.FLAT | SWT.HORIZONTAL | SWT.WRAP);
    PropsUi.setLook(toolBar, org.apache.hop.core.Props.WIDGET_STYLE_TOOLBAR);
    FormData fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.top = new FormAttachment(filterText, PropsUi.getMargin());
    fd.right = new FormAttachment(100, 0);
    toolBar.setLayoutData(fd);

    GuiResource images = GuiResource.getInstance();
    ToolItem expand = new ToolItem(toolBar, SWT.PUSH);
    expand.setImage(images.getImage("ui/images/expand-all.svg"));
    expand.setToolTipText(BaseMessages.getString(PKG, "GraphPalette.Toolbar.ExpandAll.Tooltip"));
    expand.addListener(SWT.Selection, e -> expandAll(true));

    ToolItem collapse = new ToolItem(toolBar, SWT.PUSH);
    collapse.setImage(images.getImage("ui/images/collapse-all.svg"));
    collapse.setToolTipText(
        BaseMessages.getString(PKG, "GraphPalette.Toolbar.CollapseAll.Tooltip"));
    collapse.addListener(SWT.Selection, e -> expandAll(false));

    ToolItem clear = new ToolItem(toolBar, SWT.PUSH);
    clear.setImage(images.getImage("ui/images/clear.svg"));
    clear.setToolTipText(BaseMessages.getString(PKG, "GraphPalette.Toolbar.ClearFilter.Tooltip"));
    clear.addListener(SWT.Selection, e -> clearFilter());

    toolBar.pack();
    return toolBar;
  }

  private void createTree(ToolBar toolBar) {
    Composite border = new Composite(this, SWT.BORDER);
    border.setLayout(new FormLayout());
    FormData borderFd = new FormData();
    borderFd.left = new FormAttachment(0, 0);
    borderFd.top = new FormAttachment(toolBar, PropsUi.getMargin());
    borderFd.right = new FormAttachment(100, 0);
    borderFd.bottom = new FormAttachment(100, 0);
    border.setLayoutData(borderFd);
    PropsUi.setLook(border);

    tree = new Tree(border, SWT.SINGLE | SWT.H_SCROLL | SWT.V_SCROLL);
    tree.setHeaderVisible(false);
    PropsUi.setLook(tree);
    FormData treeFd = new FormData();
    treeFd.left = new FormAttachment(0, 0);
    treeFd.top = new FormAttachment(0, 0);
    treeFd.right = new FormAttachment(100, 0);
    treeFd.bottom = new FormAttachment(100, 0);
    tree.setLayoutData(treeFd);

    TreeMemory.addTreeListener(tree, treeMemoryName);

    tree.addListener(SWT.MouseDown, this::onMouseDown);
    tree.addListener(SWT.DefaultSelection, this::onDefaultSelection);
    tree.addListener(SWT.MouseHover, this::onHover);
    tree.addListener(SWT.MouseMove, this::onHover);
  }

  private void createContextMenu() {
    Menu menu = new Menu(tree);
    MenuItem favoriteItem = new MenuItem(menu, SWT.PUSH);
    favoriteItem.addListener(SWT.Selection, e -> toggleFavorite(selectedItem()));
    tree.setMenu(menu);
    tree.addListener(
        SWT.MenuDetect,
        event -> {
          Item item = selectedItem();
          if (item == null) {
            event.doit = false;
            return;
          }
          boolean favorite = GuiActionFavorites.isFavorite(host.getPaletteKind(), item.pluginId());
          favoriteItem.setText(
              BaseMessages.getString(
                  PKG,
                  favorite ? "GraphPalette.Menu.RemoveFavorite" : "GraphPalette.Menu.AddFavorite"));
        });
  }

  private void createDragSource() {
    DragSource dragSource = new DragSource(tree, DND.DROP_COPY);
    dragSource.setTransfer(new Transfer[] {TextTransfer.getInstance()});
    dragSource.addDragListener(
        new DragSourceAdapter() {
          @Override
          public void dragStart(DragSourceEvent event) {
            Item item = selectedItem();
            if (item == null || altHeld) {
              event.doit = false;
              return;
            }
            installShiftKeyFilter();
            setDragImage(event);
          }

          @Override
          public void dragSetData(DragSourceEvent event) {
            Item item = selectedItem();
            if (item == null || !TextTransfer.getInstance().isSupportedType(event.dataType)) {
              event.doit = false;
              return;
            }
            event.data = ContextDialogPlacement.encode(item.actionId(), shiftHeld);
          }

          @Override
          public void dragFinished(DragSourceEvent event) {
            removeShiftKeyFilter();
            if (dragImage != null) {
              dragImage.dispose();
              dragImage = null;
            }
          }
        });
  }

  private void setDragImage(DragSourceEvent event) {
    if (EnvironmentUtils.getInstance().isWeb()) {
      event.image = GuiResource.getInstance().getImageHop();
      return;
    }
    TreeItem[] selection = tree.getSelection();
    if (selection == null || selection.length == 0) {
      return;
    }
    Rectangle bounds = selection[0].getBounds();
    int w = Math.max(1, bounds.width);
    int h = Math.max(1, bounds.height);
    try {
      dragImage = new Image(getDisplay(), w, h);
      GC gc = new GC(tree);
      try {
        gc.copyArea(dragImage, bounds.x, bounds.y);
      } finally {
        gc.dispose();
      }
      event.image = dragImage;
    } catch (Exception e) {
      // Fall back to the default drag image.
    }
  }

  private void installShiftKeyFilter() {
    removeShiftKeyFilter();
    Display display = getDisplay();
    shiftKeyFilter =
        event -> {
          if (event.keyCode == SWT.SHIFT) {
            shiftHeld = event.type == SWT.KeyDown;
          }
        };
    display.addFilter(SWT.KeyDown, shiftKeyFilter);
    display.addFilter(SWT.KeyUp, shiftKeyFilter);
  }

  private void removeShiftKeyFilter() {
    if (shiftKeyFilter == null) {
      return;
    }
    Display display = getDisplay();
    if (display != null && !display.isDisposed()) {
      display.removeFilter(SWT.KeyDown, shiftKeyFilter);
      display.removeFilter(SWT.KeyUp, shiftKeyFilter);
    }
    shiftKeyFilter = null;
  }

  private void onMouseDown(Event event) {
    shiftHeld = (event.stateMask & SWT.SHIFT) != 0;
    altHeld = (event.stateMask & SWT.ALT) != 0;
    if (!altHeld || event.button != 1) {
      return;
    }
    TreeItem treeItem = tree.getItem(new org.eclipse.swt.graphics.Point(event.x, event.y));
    Item item = itemOf(treeItem);
    if (item != null) {
      toggleFavorite(item);
    }
  }

  private void onDefaultSelection(Event event) {
    // Shift-double-click does not always change Tree.getSelection(); use the item that was
    // actually activated (event.item) so we add that plugin, not a previously selected one.
    TreeItem treeItem = event.item instanceof TreeItem ti ? ti : null;
    Item item = itemOf(treeItem);
    if (item == null) {
      item = selectedItem();
    }
    if (item == null) {
      return;
    }
    if (treeItem != null) {
      tree.setSelection(treeItem);
    }
    if ((event.stateMask & SWT.ALT) != 0) {
      toggleFavorite(item);
      return;
    }
    boolean chain = (event.stateMask & SWT.SHIFT) != 0 || shiftHeld;
    // Shift-double-click: let the graph place the new item in a row after the last/selected
    // one (null location). Plain double-click still uses the last canvas click.
    Point location = chain ? null : host.getPaletteDropLocation();
    host.placePaletteAction(item.actionId(), location, chain);
  }

  private void onHover(Event event) {
    TreeItem treeItem = tree.getItem(new org.eclipse.swt.graphics.Point(event.x, event.y));
    Item item = itemOf(treeItem);
    if (item == null) {
      tree.setToolTipText(null);
      return;
    }
    boolean favorite = GuiActionFavorites.isFavorite(host.getPaletteKind(), item.pluginId());
    tree.setToolTipText(GuiActionFavorites.tooltipWithFavoriteHint(item.description(), favorite));
  }

  private void toggleFavorite(Item item) {
    if (item == null) {
      return;
    }
    GuiActionFavorites.toggle(host.getPaletteKind(), item.pluginId());
    host.persistFavoritesChange();
  }

  /** Rebuild from plugins the next time the tree is shown, or immediately when already visible. */
  public void refresh() {
    dirty = true;
    if (!isDisposed() && GraphPalette.isVisible()) {
      rebuildTree();
    }
  }

  public void ensurePopulated() {
    if (isDisposed()) {
      return;
    }
    if (dirty || !populated) {
      rebuildTree();
    }
  }

  private void asyncRefresh() {
    if (isDisposed()) {
      return;
    }
    getDisplay()
        .asyncExec(
            () -> {
              if (!isDisposed()) {
                refresh();
              }
            });
  }

  private void rebuildTree() {
    if (tree == null || tree.isDisposed()) {
      return;
    }
    model = GraphPaletteModel.fromPlugins(host.getPaletteKind());
    String filter = filterText == null || filterText.isDisposed() ? "" : filterText.getText();
    List<Category> categories = model.filter(filter);
    boolean filtering = StringUtils.isNotEmpty(filter);

    tree.setRedraw(false);
    try {
      tree.removeAll();
      GuiResource images = GuiResource.getInstance();
      String favoritesName = GraphPaletteModel.favoritesCategoryName();
      for (Category category : categories) {
        TreeItem categoryItem = new TreeItem(tree, SWT.NONE);
        categoryItem.setText(category.name());
        if (favoritesName.equals(category.name())) {
          categoryItem.setImage(images.getImage("ui/images/bookmark-add.svg"));
        } else {
          categoryItem.setImage(images.getImageFolder());
        }
        for (Item item : category.items()) {
          TreeItem leaf = new TreeItem(categoryItem, SWT.NONE);
          leaf.setText(item.name());
          leaf.setImage(iconFor(item));
          leaf.setData(item);
        }
      }
      if (filtering) {
        expandAll(true);
      } else if (!populated) {
        expandAll(true);
      } else {
        TreeMemory.setExpandedFromMemory(tree, treeMemoryName);
      }
    } finally {
      tree.setRedraw(true);
    }
    populated = true;
    dirty = false;
  }

  private Image iconFor(Item item) {
    int size = ConstUi.SMALL_ICON_SIZE;
    GuiResource images = GuiResource.getInstance();
    if (host.getPaletteKind() == Kind.TRANSFORM) {
      return images
          .getSwtImageTransform(item.pluginId())
          .getAsBitmapForSize(getDisplay(), size, size);
    }
    return images.getSwtImageAction(item.pluginId()).getAsBitmapForSize(getDisplay(), size, size);
  }

  private void expandAll(boolean expanded) {
    if (tree == null || tree.isDisposed()) {
      return;
    }
    tree.setRedraw(false);
    try {
      for (TreeItem item : tree.getItems()) {
        expandTreeItem(item, expanded);
      }
    } finally {
      tree.setRedraw(true);
    }
  }

  private void expandTreeItem(TreeItem item, boolean expanded) {
    item.setExpanded(expanded);
    TreeMemory.getInstance().storeExpanded(treeMemoryName, item, expanded);
    for (TreeItem child : item.getItems()) {
      expandTreeItem(child, expanded);
    }
  }

  public void clearFilter() {
    if (filterText == null || filterText.isDisposed()) {
      return;
    }
    if (StringUtils.isNotEmpty(filterText.getText())) {
      filterText.setText("");
    } else {
      rebuildTree();
    }
    filterText.setFocus();
  }

  private Item selectedItem() {
    if (tree == null || tree.isDisposed() || tree.getSelectionCount() != 1) {
      return null;
    }
    return itemOf(tree.getSelection()[0]);
  }

  private static Item itemOf(TreeItem treeItem) {
    if (treeItem == null) {
      return null;
    }
    Object data = treeItem.getData();
    return data instanceof Item item ? item : null;
  }
}
