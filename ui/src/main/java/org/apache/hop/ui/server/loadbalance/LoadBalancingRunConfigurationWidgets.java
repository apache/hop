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
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.TableItem;

/** TableView for the Hop Server group on a load-balancing run configuration. */
public class LoadBalancingRunConfigurationWidgets {

  private static final Class<?> PKG = LoadBalancingRunConfigurationWidgets.class;

  private final IVariables variables;
  private final IHopMetadataProvider metadataProvider;
  private TableView wServers;
  private ColumnInfo serverColumn;

  public LoadBalancingRunConfigurationWidgets(
      IVariables variables, IHopMetadataProvider metadataProvider) {
    this.variables = variables;
    this.metadataProvider = metadataProvider;
  }

  public void addTo(Composite parent, ModifyListener modifyListener) {
    PropsUi props = PropsUi.getInstance();
    int margin = PropsUi.getMargin();

    Control lastControl = lastChild(parent);

    Label wlServers = new Label(parent, SWT.LEFT);
    PropsUi.setLook(wlServers);
    wlServers.setText(BaseMessages.getString(PKG, "LoadBalancingRunConfiguration.Servers.Label"));
    FormData fdlServers = new FormData();
    fdlServers.left = new FormAttachment(0, 0);
    fdlServers.right = new FormAttachment(100, 0);
    if (lastControl == null) {
      fdlServers.top = new FormAttachment(0, margin);
    } else {
      fdlServers.top = new FormAttachment(lastControl, margin);
    }
    wlServers.setLayoutData(fdlServers);

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
    fdServers.left = new FormAttachment(0, 0);
    fdServers.top = new FormAttachment(wlServers, margin);
    fdServers.right = new FormAttachment(100, 0);
    fdServers.bottom = new FormAttachment(100, 0);
    wServers.setLayoutData(fdServers);
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

  private String[] loadServerNames() {
    try {
      List<String> names = metadataProvider.getSerializer(HopServerMeta.class).listObjectNames();
      return names.toArray(new String[0]);
    } catch (Exception e) {
      LogChannel.UI.logError("Error listing Hop servers for the load-balancing table", e);
      return new String[0];
    }
  }

  private static Control lastChild(Composite parent) {
    Control[] children = parent.getChildren();
    if (children == null || children.length == 0) {
      return null;
    }
    return children[children.length - 1];
  }
}
