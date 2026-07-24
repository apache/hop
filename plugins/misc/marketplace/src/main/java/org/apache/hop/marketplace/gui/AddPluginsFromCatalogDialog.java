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

package org.apache.hop.marketplace.gui;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.marketplace.catalog.OptionalPluginCatalog;
import org.apache.hop.marketplace.catalog.OptionalPluginInfo;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

/** Multi-select optional plugins from the marketplace catalog. */
public class AddPluginsFromCatalogDialog extends Dialog {

  private static final Class<?> PKG = MarketplaceGuiPlugin.class;

  private final PropsUi props;
  private final Set<String> alreadyListed;
  private final List<OptionalPluginInfo> selected = new ArrayList<>();

  private Shell shell;
  private TableView wTable;
  private Text wFilter;

  public AddPluginsFromCatalogDialog(Shell parent, Set<String> alreadyListedArtifactIds) {
    super(parent, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.RESIZE | SWT.MAX);
    this.props = PropsUi.getInstance();
    this.alreadyListed =
        alreadyListedArtifactIds == null ? Set.of() : new HashSet<>(alreadyListedArtifactIds);
  }

  /**
   * @return selected catalog entries, or empty if cancelled
   */
  public List<OptionalPluginInfo> open() {
    Shell parent = getParent();
    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.RESIZE | SWT.MAX);
    PropsUi.setLook(shell);
    shell.setImage(GuiResource.getInstance().getImageHopUi());
    shell.setText(BaseMessages.getString(PKG, "AddPluginsFromCatalogDialog.Shell.Title"));

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();
    shell.setLayout(formLayout);

    Label wlFilter = new Label(shell, SWT.RIGHT);
    PropsUi.setLook(wlFilter);
    wlFilter.setText(BaseMessages.getString(PKG, "AddPluginsFromCatalogDialog.Filter.Label"));
    FormData fdlFilter = new FormData();
    fdlFilter.left = new FormAttachment(0, 0);
    fdlFilter.top = new FormAttachment(0, 0);
    wlFilter.setLayoutData(fdlFilter);

    wFilter = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFilter);
    FormData fdFilter = new FormData();
    fdFilter.left = new FormAttachment(wlFilter, PropsUi.getMargin());
    fdFilter.top = new FormAttachment(wlFilter, 0, SWT.CENTER);
    fdFilter.right = new FormAttachment(100, 0);
    wFilter.setLayoutData(fdFilter);
    wFilter.addListener(SWT.Modify, e -> refreshTable());

    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "AddPluginsFromCatalogDialog.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> shell.dispose());

    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "AddPluginsFromCatalogDialog.Button.Add"));
    wOk.addListener(SWT.Selection, e -> ok());

    BaseTransformDialog.positionBottomButtons(
        shell, new Button[] {wOk, wCancel}, PropsUi.getMargin(), null);

    ColumnInfo[] columns = {
      new ColumnInfo(
          BaseMessages.getString(PKG, "AddPluginsFromCatalogDialog.Column.Name"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          true),
      new ColumnInfo(
          BaseMessages.getString(PKG, "AddPluginsFromCatalogDialog.Column.Artifact"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          true),
      new ColumnInfo(
          BaseMessages.getString(PKG, "AddPluginsFromCatalogDialog.Column.Category"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          true),
      new ColumnInfo(
          BaseMessages.getString(PKG, "AddPluginsFromCatalogDialog.Column.Description"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          true),
    };
    wTable =
        new TableView(
            Variables.getADefaultVariableSpace(),
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL,
            columns,
            1,
            true,
            null,
            props,
            false);
    PropsUi.setLook(wTable);

    FormData fdTable = new FormData();
    fdTable.left = new FormAttachment(0, 0);
    fdTable.top = new FormAttachment(wFilter, PropsUi.getMargin() * 2);
    fdTable.right = new FormAttachment(100, 0);
    fdTable.bottom = new FormAttachment(wOk, -PropsUi.getMargin() * 2);
    wTable.setLayoutData(fdTable);

    refreshTable();

    BaseTransformDialog.setSize(shell);
    shell.setSize(Math.max(shell.getSize().x, 720), Math.max(shell.getSize().y, 480));
    shell.open();
    Display display = parent.getDisplay();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
    return selected;
  }

  private void refreshTable() {
    wTable.table.removeAll();
    String filter = wFilter != null ? wFilter.getText() : "";
    for (OptionalPluginInfo info : OptionalPluginCatalog.query(filter)) {
      if (info == null || StringUtils.isBlank(info.getArtifactId())) {
        continue;
      }
      if (alreadyListed.contains(info.getArtifactId())) {
        continue;
      }
      TableItem item = new TableItem(wTable.table, SWT.NONE);
      item.setText(1, Const.NVL(info.getName(), info.getArtifactId()));
      item.setText(2, Const.NVL(info.getArtifactId(), ""));
      item.setText(3, Const.NVL(info.getCategory(), ""));
      item.setText(4, Const.NVL(info.getDescription(), ""));
      item.setData(info);
    }
    wTable.optimizeTableView();
  }

  private void ok() {
    selected.clear();
    for (TableItem item : wTable.table.getSelection()) {
      if (item.getData() instanceof OptionalPluginInfo info) {
        selected.add(info);
      }
    }
    shell.dispose();
  }
}
