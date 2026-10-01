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

package org.apache.hop.ai.ui;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.hop.ai.advisor.AiAdvisorOpenRequest;
import org.apache.hop.ai.session.AiAdvisorSession;
import org.apache.hop.ai.session.AiAdvisorSessionStore;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.EnterStringDialog;
import org.apache.hop.ui.core.gui.GuiToolbarWidgets;
import org.apache.hop.ui.core.gui.IToolbarContainer;
import org.apache.hop.ui.hopgui.ToolbarFacade;
import org.apache.hop.ui.hopgui.shared.SashFormMemory;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeItem;

/**
 * Multi-session AI advisory UI. A plain Composite so it can live in a perspective, a floating
 * dialog, or a dock tab. Sessions are stored on the Hop GUI shell so all hosts share them.
 */
@GuiPlugin(classLoaderGroup = "hop-ai")
public class AiAdvisorWorkbench extends Composite {

  public static final Class<?> PKG = AiAdvisorPerspective.class;

  public static final String GUI_PLUGIN_TOOLBAR_PARENT_ID = "AiAdvisorWorkbench-Toolbar";
  public static final String TOOLBAR_ITEM_NEW = "AiAdvisorWorkbench-Toolbar-10000-New";
  public static final String TOOLBAR_ITEM_RENAME = "AiAdvisorWorkbench-Toolbar-10010-Rename";
  public static final String TOOLBAR_ITEM_CLOSE = "AiAdvisorWorkbench-Toolbar-10020-Close";
  public static final String TOOLBAR_ITEM_FLOAT = "AiAdvisorWorkbench-Toolbar-20000-Float";
  public static final String TOOLBAR_ITEM_DOCK = "AiAdvisorWorkbench-Toolbar-20010-Dock";

  private final IAiAdvisorWorkbenchHost host;
  private final AiAdvisorSessionStore store;
  private final Runnable storeListener = this::refreshFromStore;

  private final Tree tree;
  private final AiAdvisorSessionPane sessionPane;
  private final GuiToolbarWidgets toolBarWidgets;
  private boolean refreshing;

  public AiAdvisorWorkbench(Composite parent, IAiAdvisorWorkbenchHost host) {
    super(parent, SWT.NONE);
    this.host = host;
    this.store = AiAdvisorSessionStore.get(host.getHopGui());

    PropsUi.setLook(this);
    setLayout(PropsUi.getInstance().createFormLayout());

    IToolbarContainer toolBarContainer =
        ToolbarFacade.createToolbarContainer(this, SWT.WRAP | SWT.LEFT | SWT.HORIZONTAL);
    Control toolBar = toolBarContainer.getControl();
    toolBar.setLayoutData(new FormDataBuilder().top().fullWidth().result());
    PropsUi.setLook(toolBar, PropsUi.WIDGET_STYLE_TOOLBAR);
    toolBarWidgets = new GuiToolbarWidgets();
    toolBarWidgets.registerGuiPluginObject(this);
    toolBarWidgets.createToolbarWidgets(toolBarContainer, GUI_PLUGIN_TOOLBAR_PARENT_ID);
    toolBar.pack();

    SashForm sash = new SashForm(this, SWT.HORIZONTAL);
    sash.setLayoutData(
        new FormDataBuilder().left().right().top(toolBar, PropsUi.getMargin()).bottom().result());

    Composite treeComposite = new Composite(sash, SWT.NONE);
    treeComposite.setLayout(PropsUi.getInstance().createFormLayout());
    PropsUi.setLook(treeComposite);

    Label wlSessions = new Label(treeComposite, SWT.LEFT);
    wlSessions.setText(BaseMessages.getString(PKG, "AiAdvisor.Sessions.Label"));
    PropsUi.setLook(wlSessions);
    wlSessions.setLayoutData(new FormDataBuilder().top().fullWidth().result());

    tree = new Tree(treeComposite, SWT.SINGLE | SWT.BORDER | SWT.V_SCROLL | SWT.H_SCROLL);
    PropsUi.setLook(tree);
    tree.setLayoutData(
        new FormDataBuilder().top(wlSessions, PropsUi.getMargin()).bottom().fullWidth().result());
    tree.addListener(SWT.Selection, e -> treeSelection());

    sessionPane = new AiAdvisorSessionPane(sash, host);
    sash.setWeights(24, 76);
    SashFormMemory.persist(sash, "ai-advisor-workbench-sash", 24, 76);

    store.addListener(storeListener);
    addDisposeListener(e -> store.removeListener(storeListener));

    refreshFromStore();
  }

  public AiAdvisorSession openSession(AiAdvisorOpenRequest request) {
    AiAdvisorSession session = store.open(request);
    host.activate();
    return session;
  }

  public AiAdvisorSession newSession() {
    AiAdvisorOpenRequest request = new AiAdvisorOpenRequest();
    request.setReuseExisting(false);
    request.setTitle(BaseMessages.getString(PKG, "AiAdvisor.Session.Untitled"));
    return store.open(request);
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_NEW,
      toolTip = "i18n::AiAdvisor.Toolbar.New.Tooltip",
      image = "ui/images/add.svg")
  public void toolbarNew() {
    newSession();
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_RENAME,
      toolTip = "i18n::AiAdvisor.Toolbar.Rename.Tooltip",
      image = "ui/images/rename.svg")
  public void toolbarRename() {
    AiAdvisorSession session = store.getActiveSession();
    if (session == null) {
      return;
    }
    EnterStringDialog dialog =
        new EnterStringDialog(
            host.getShell(),
            session.displayTitle(),
            BaseMessages.getString(PKG, "AiAdvisor.Rename.Title"),
            BaseMessages.getString(PKG, "AiAdvisor.Rename.Message"));
    String title = dialog.open();
    if (!Utils.isEmpty(title)) {
      session.setTitle(title);
      store.fireChanged();
    }
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_CLOSE,
      toolTip = "i18n::AiAdvisor.Toolbar.Close.Tooltip",
      image = "ui/images/close.svg")
  public void toolbarClose() {
    AiAdvisorSession session = store.getActiveSession();
    if (session != null) {
      store.remove(session.getId());
    }
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_FLOAT,
      toolTip = "i18n::AiAdvisor.Toolbar.Float.Tooltip",
      image = "ui/images/detach-panel.svg",
      separator = true)
  public void toolbarFloat() {
    AiAdvisorViews.openDialog(host.getHopGui());
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_DOCK,
      toolTip = "i18n::AiAdvisor.Toolbar.Dock.Tooltip",
      image = "ui/images/dock-panel.svg")
  public void toolbarDock() {
    AiAdvisorViews.openDock(host.getHopGui());
  }

  private void treeSelection() {
    if (refreshing) {
      return;
    }
    TreeItem[] selection = tree.getSelection();
    if (selection.length == 0) {
      return;
    }
    Object data = selection[0].getData();
    if (data instanceof String sessionId && !sessionId.equals(store.getActiveSessionId())) {
      store.setActiveSessionId(sessionId);
    }
  }

  private void refreshFromStore() {
    if (isDisposed() || refreshing) {
      return;
    }
    refreshing = true;
    tree.setRedraw(false);
    try {
      tree.removeAll();
      Map<String, TreeItem> groups = new LinkedHashMap<>();
      for (AiAdvisorSession session : store.getSessions()) {
        String area = session.areaLabel();
        TreeItem group = groups.get(area);
        if (group == null) {
          group = new TreeItem(tree, SWT.NONE);
          group.setText(area);
          groups.put(area, group);
        }
        TreeItem item = new TreeItem(group, SWT.NONE);
        item.setText(session.displayTitle());
        item.setData(session.getId());
        group.setExpanded(true);
        if (session.getId().equals(store.getActiveSessionId())) {
          tree.setSelection(item);
        }
      }
      sessionPane.showSession(store.getActiveSession());
      updateToolbar();
    } finally {
      tree.setRedraw(true);
      refreshing = false;
    }
  }

  private void updateToolbar() {
    boolean hasSession = store.getActiveSession() != null;
    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_RENAME, hasSession);
    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_CLOSE, hasSession);
  }
}
