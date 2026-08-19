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

package org.apache.hop.ui.server.loadbalance;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.server.HopServerMeta;
import org.apache.hop.server.loadbalance.ILoadBalancingRunConfiguration;
import org.apache.hop.server.loadbalance.LoadBalancingServerEntry;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.GuiCompositeWidgetsAdapter;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.TableItem;

/** Tabbed editor for load-balancing engine options: Management, Load-balancing, Hop Servers. */
public class LoadBalancingRunConfigurationWidgets {

  private static final Class<?> PKG = LoadBalancingRunConfigurationWidgets.class;

  private final IVariables variables;
  private final IHopMetadataProvider metadataProvider;
  private GuiCompositeWidgets managementWidgets;
  private GuiCompositeWidgets loadBalancingWidgets;
  private TableView wServers;
  private ColumnInfo serverColumn;

  public LoadBalancingRunConfigurationWidgets(
      IVariables variables, IHopMetadataProvider metadataProvider) {
    this.variables = variables;
    this.metadataProvider = metadataProvider;
  }

  public void addTo(Composite parent, Object sourceConfig, ModifyListener modifyListener) {
    PropsUi props = PropsUi.getInstance();
    int margin = PropsUi.getMargin();

    CTabFolder wTabFolder = new CTabFolder(parent, SWT.BORDER);
    PropsUi.setLook(wTabFolder);
    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(0, 0);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(100, 0);
    wTabFolder.setLayoutData(fdTabFolder);

    Composite wManagement =
        addScrolledTab(
            wTabFolder,
            BaseMessages.getString(PKG, "LoadBalancingRunConfiguration.Tab.Management"),
            GuiResource.getInstance().getImageHop());
    managementWidgets = new GuiCompositeWidgets(variables);
    managementWidgets.createCompositeWidgets(
        sourceConfig,
        null,
        wManagement,
        ILoadBalancingRunConfiguration.GUI_WIDGETS_MANAGEMENT,
        null);
    packScrolledTab(wManagement);
    listenForChanges(managementWidgets, modifyListener);

    Composite wLoadBalancing =
        addScrolledTab(
            wTabFolder,
            BaseMessages.getString(PKG, "LoadBalancingRunConfiguration.Tab.LoadBalancing"),
            GuiResource.getInstance().getImageRun());
    loadBalancingWidgets = new GuiCompositeWidgets(variables);
    loadBalancingWidgets.createCompositeWidgets(
        sourceConfig,
        null,
        wLoadBalancing,
        ILoadBalancingRunConfiguration.GUI_WIDGETS_LOAD_BALANCING,
        null);
    packScrolledTab(wLoadBalancing);
    listenForChanges(loadBalancingWidgets, modifyListener);

    Composite wServersTab =
        addFillTab(
            wTabFolder,
            BaseMessages.getString(PKG, "LoadBalancingRunConfiguration.Tab.HopServers"),
            GuiResource.getInstance().getImageServer());
    addServersTable(wServersTab, modifyListener, props, margin);

    wTabFolder.setSelection(0);
  }

  public void setContents(ILoadBalancingRunConfiguration config) {
    if (config == null) {
      return;
    }
    if (managementWidgets != null) {
      managementWidgets.setWidgetsContents(
          config, null, ILoadBalancingRunConfiguration.GUI_WIDGETS_MANAGEMENT);
    }
    if (loadBalancingWidgets != null) {
      loadBalancingWidgets.setWidgetsContents(
          config, null, ILoadBalancingRunConfiguration.GUI_WIDGETS_LOAD_BALANCING);
    }
    setServers(config);
  }

  public void getContents(ILoadBalancingRunConfiguration config) {
    if (config == null) {
      return;
    }
    if (managementWidgets != null) {
      managementWidgets.getWidgetsContents(
          config, ILoadBalancingRunConfiguration.GUI_WIDGETS_MANAGEMENT);
    }
    if (loadBalancingWidgets != null) {
      loadBalancingWidgets.getWidgetsContents(
          config, ILoadBalancingRunConfiguration.GUI_WIDGETS_LOAD_BALANCING);
    }
    getServers(config);
  }

  public void setServers(ILoadBalancingRunConfiguration config) {
    if (wServers == null || wServers.isDisposed() || config == null) {
      return;
    }
    refreshServerNames();
    wServers.clearAll();
    List<LoadBalancingServerEntry> servers = config.getServers();
    if (servers != null) {
      for (LoadBalancingServerEntry entry : servers) {
        TableItem item = new TableItem(wServers.table, SWT.NONE);
        item.setText(1, Const.NVL(entry.getHopServerName(), ""));
        item.setText(2, entry.isEnabled() ? "Y" : "N");
        item.setText(3, Const.NVL(entry.getMaxConcurrent(), ""));
      }
    }
    if (wServers.table.getItemCount() == 0) {
      new TableItem(wServers.table, SWT.NONE);
    }
    wServers.removeEmptyRows();
    wServers.setRowNums();
    wServers.optWidth(true);
  }

  public void getServers(ILoadBalancingRunConfiguration config) {
    if (wServers == null || wServers.isDisposed() || config == null) {
      return;
    }
    List<LoadBalancingServerEntry> servers = new ArrayList<>();
    for (int i = 0; i < wServers.nrNonEmpty(); i++) {
      TableItem item = wServers.getNonEmpty(i);
      String name = item.getText(1);
      if (name.isEmpty()) {
        continue;
      }
      LoadBalancingServerEntry entry = new LoadBalancingServerEntry();
      entry.setHopServerName(name);
      entry.setEnabled(!"N".equalsIgnoreCase(item.getText(2)));
      entry.setMaxConcurrent(item.getText(3));
      servers.add(entry);
    }
    config.setServers(servers);
  }

  public void refreshServerNames() {
    if (serverColumn == null) {
      return;
    }
    serverColumn.setComboValues(loadServerNames());
  }

  private void addServersTable(
      Composite parent, ModifyListener modifyListener, PropsUi props, int margin) {
    serverColumn =
        new ColumnInfo(
            BaseMessages.getString(PKG, "LoadBalancingRunConfiguration.Servers.Column.Server"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            loadServerNames(),
            false);
    serverColumn.setUsingVariables(true);

    ColumnInfo enabledColumn =
        new ColumnInfo(
            BaseMessages.getString(PKG, "LoadBalancingRunConfiguration.Servers.Column.Enabled"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {"Y", "N"},
            true);

    ColumnInfo maxColumn =
        new ColumnInfo(
            BaseMessages.getString(
                PKG, "LoadBalancingRunConfiguration.Servers.Column.MaxConcurrent"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);
    maxColumn.setUsingVariables(true);
    maxColumn.setToolTip(
        BaseMessages.getString(
            PKG, "LoadBalancingRunConfiguration.Servers.Column.MaxConcurrent.ToolTip"));

    wServers =
        new TableView(
            variables,
            parent,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            new ColumnInfo[] {serverColumn, enabledColumn, maxColumn},
            1,
            modifyListener,
            props);
    PropsUi.setLook(wServers);
    FormData fdServers = new FormData();
    fdServers.left = new FormAttachment(0, margin);
    fdServers.top = new FormAttachment(0, margin);
    fdServers.right = new FormAttachment(100, -margin);
    fdServers.bottom = new FormAttachment(100, -margin);
    wServers.setLayoutData(fdServers);
  }

  private static Composite addScrolledTab(CTabFolder folder, String title, Image image) {
    CTabItem item = new CTabItem(folder, SWT.NONE);
    item.setFont(GuiResource.getInstance().getFontDefault());
    item.setText(title);
    if (image != null) {
      item.setImage(image);
    }

    ScrolledComposite scrolled = new ScrolledComposite(folder, SWT.V_SCROLL | SWT.H_SCROLL);
    scrolled.setLayout(new FillLayout());
    Composite composite = new Composite(scrolled, SWT.NONE);
    PropsUi.setLook(composite);
    FormLayout layout = new FormLayout();
    layout.marginWidth = PropsUi.getFormMargin();
    layout.marginHeight = PropsUi.getFormMargin();
    composite.setLayout(layout);
    scrolled.setContent(composite);
    scrolled.setExpandHorizontal(true);
    scrolled.setExpandVertical(true);
    item.setControl(scrolled);
    return composite;
  }

  private static Composite addFillTab(CTabFolder folder, String title, Image image) {
    CTabItem item = new CTabItem(folder, SWT.NONE);
    item.setFont(GuiResource.getInstance().getFontDefault());
    item.setText(title);
    if (image != null) {
      item.setImage(image);
    }
    Composite composite = new Composite(folder, SWT.NONE);
    PropsUi.setLook(composite);
    composite.setLayout(new FormLayout());
    item.setControl(composite);
    return composite;
  }

  private static void packScrolledTab(Composite composite) {
    Control scrolled = composite.getParent();
    if (!(scrolled instanceof ScrolledComposite scrolledComposite)) {
      return;
    }
    composite.pack();
    Rectangle bounds = composite.getBounds();
    scrolledComposite.setMinWidth(bounds.width);
    scrolledComposite.setMinHeight(bounds.height);
  }

  private static void listenForChanges(GuiCompositeWidgets widgets, ModifyListener modifyListener) {
    if (widgets == null || modifyListener == null) {
      return;
    }
    widgets.setWidgetsListener(
        new GuiCompositeWidgetsAdapter() {
          @Override
          public void widgetModified(
              GuiCompositeWidgets compositeWidgets, Control changedWidget, String widgetId) {
            modifyListener.modifyText(null);
          }
        });
  }

  private String[] loadServerNames() {
    try {
      List<String> names = metadataProvider.getSerializer(HopServerMeta.class).listObjectNames();
      return names.toArray(new String[0]);
    } catch (Exception e) {
      LogChannel.UI.logError("Error listing Hop servers for the load-balancing table", e);
      return new String[0];
    }
  }
}
