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

package org.apache.hop.projects.xp;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.AttributesContext;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.projects.resources.DiskSpaceRequirement;
import org.apache.hop.projects.resources.ResourceAttributes;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.AttributesDialogExtension;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

/**
 * Contributes a System resources tab on the lifecycle environment dialog. Settings are stored under
 * {@link ResourceAttributes#GROUP} on the shared {@link AttributesContext}.
 */
@ExtensionPoint(
    id = "ResourceLifecycleEnvironmentDialogTabs",
    description = "Add system resource requirements tab to the lifecycle environment dialog",
    extensionPointId = "HopGuiLifecycleEnvironmentDialogTabs")
public class ResourceLifecycleEnvironmentDialogTabsExtensionPoint
    implements IExtensionPoint<AttributesDialogExtension> {

  private static final Class<?> PKG = ResourceAttributes.class;

  private Combo wOnEnable;
  private Text wMinMaxMemoryMb;
  private Text wMinProcessors;
  private TableView wDiskChecks;
  private IVariables variables;

  @Override
  public void callExtensionPoint(
      ILogChannel log, IVariables variables, AttributesDialogExtension extension)
      throws HopException {
    if (extension == null || extension.getTabFolder() == null) {
      return;
    }
    this.variables = variables;

    PropsUi props = PropsUi.getInstance();
    CTabFolder folder = extension.getTabFolder();
    int margin = PropsUi.getMargin();
    int middle = props.getMiddlePct();

    CTabItem tab = new CTabItem(folder, SWT.NONE);
    tab.setText(BaseMessages.getString(PKG, "ResourceLifecycleEnv.Tab.Title"));
    tab.setImage(GuiResource.getInstance().getImageServer());
    Composite comp = new Composite(folder, SWT.NONE);
    PropsUi.setLook(comp);
    FormLayout layout = new FormLayout();
    layout.marginWidth = PropsUi.getFormMargin();
    layout.marginHeight = PropsUi.getFormMargin();
    comp.setLayout(layout);
    tab.setControl(comp);

    Label wlHelp = new Label(comp, SWT.LEFT | SWT.WRAP);
    PropsUi.setLook(wlHelp);
    wlHelp.setText(BaseMessages.getString(PKG, "ResourceLifecycleEnv.Help"));
    FormData fdlHelp = new FormData();
    fdlHelp.left = new FormAttachment(0, 0);
    fdlHelp.top = new FormAttachment(0, 0);
    fdlHelp.right = new FormAttachment(100, 0);
    wlHelp.setLayoutData(fdlHelp);

    Label wlOnEnable = new Label(comp, SWT.RIGHT);
    PropsUi.setLook(wlOnEnable);
    wlOnEnable.setText(BaseMessages.getString(PKG, "ResourceLifecycleEnv.OnEnable.Label"));
    FormData fdlOn = new FormData();
    fdlOn.left = new FormAttachment(0, 0);
    fdlOn.top = new FormAttachment(wlHelp, margin * 2);
    fdlOn.right = new FormAttachment(middle, -margin);
    wlOnEnable.setLayoutData(fdlOn);

    wOnEnable = new Combo(comp, SWT.READ_ONLY | SWT.BORDER);
    PropsUi.setLook(wOnEnable);
    wOnEnable.setItems(
        ResourceAttributes.ON_ENABLE_OFF,
        ResourceAttributes.ON_ENABLE_WARN,
        ResourceAttributes.ON_ENABLE_ENFORCE);
    FormData fdOn = new FormData();
    fdOn.left = new FormAttachment(middle, 0);
    fdOn.top = new FormAttachment(wlOnEnable, 0, SWT.CENTER);
    fdOn.right = new FormAttachment(100, 0);
    wOnEnable.setLayoutData(fdOn);
    wOnEnable.setToolTipText(BaseMessages.getString(PKG, "ResourceLifecycleEnv.OnEnable.Tooltip"));

    Label wlMem = new Label(comp, SWT.RIGHT);
    PropsUi.setLook(wlMem);
    wlMem.setText(BaseMessages.getString(PKG, "ResourceLifecycleEnv.MinMaxMemoryMb.Label"));
    FormData fdlMem = new FormData();
    fdlMem.left = new FormAttachment(0, 0);
    fdlMem.top = new FormAttachment(wOnEnable, margin * 2);
    fdlMem.right = new FormAttachment(middle, -margin);
    wlMem.setLayoutData(fdlMem);

    wMinMaxMemoryMb = new Text(comp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wMinMaxMemoryMb);
    wMinMaxMemoryMb.setToolTipText(
        BaseMessages.getString(PKG, "ResourceLifecycleEnv.MinMaxMemoryMb.Tooltip"));
    FormData fdMem = new FormData();
    fdMem.left = new FormAttachment(middle, 0);
    fdMem.top = new FormAttachment(wlMem, 0, SWT.CENTER);
    fdMem.right = new FormAttachment(100, 0);
    wMinMaxMemoryMb.setLayoutData(fdMem);

    Label wlCpu = new Label(comp, SWT.RIGHT);
    PropsUi.setLook(wlCpu);
    wlCpu.setText(BaseMessages.getString(PKG, "ResourceLifecycleEnv.MinProcessors.Label"));
    FormData fdlCpu = new FormData();
    fdlCpu.left = new FormAttachment(0, 0);
    fdlCpu.top = new FormAttachment(wMinMaxMemoryMb, margin * 2);
    fdlCpu.right = new FormAttachment(middle, -margin);
    wlCpu.setLayoutData(fdlCpu);

    wMinProcessors = new Text(comp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wMinProcessors);
    wMinProcessors.setToolTipText(
        BaseMessages.getString(PKG, "ResourceLifecycleEnv.MinProcessors.Tooltip"));
    FormData fdCpu = new FormData();
    fdCpu.left = new FormAttachment(middle, 0);
    fdCpu.top = new FormAttachment(wlCpu, 0, SWT.CENTER);
    fdCpu.right = new FormAttachment(100, 0);
    wMinProcessors.setLayoutData(fdCpu);

    Label wlDisk = new Label(comp, SWT.LEFT);
    PropsUi.setLook(wlDisk);
    wlDisk.setText(BaseMessages.getString(PKG, "ResourceLifecycleEnv.Disk.Label"));
    FormData fdlDisk = new FormData();
    fdlDisk.left = new FormAttachment(0, 0);
    fdlDisk.top = new FormAttachment(wMinProcessors, margin * 2);
    fdlDisk.right = new FormAttachment(100, 0);
    wlDisk.setLayoutData(fdlDisk);

    Button wBrowse = new Button(comp, SWT.PUSH);
    wBrowse.setText(BaseMessages.getString(PKG, "ResourceLifecycleEnv.Disk.Button.Browse"));
    FormData fdBrowse = new FormData();
    fdBrowse.right = new FormAttachment(100, 0);
    fdBrowse.top = new FormAttachment(wlDisk, margin);
    wBrowse.setLayoutData(fdBrowse);
    wBrowse.addListener(
        SWT.Selection,
        e -> {
          String directory =
              BaseDialog.presentDirectoryDialog(
                  extension.getShell(), (String) null, (String) null, variables);
          if (StringUtils.isNotBlank(directory)
              && wDiskChecks != null
              && !wDiskChecks.isDisposed()) {
            TableItem item = new TableItem(wDiskChecks.table, SWT.NONE);
            item.setText(1, directory);
            item.setText(2, "1024");
            wDiskChecks.removeEmptyRows();
            wDiskChecks.setRowNums();
            wDiskChecks.optWidth(true);
          }
        });

    ColumnInfo[] columns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "ResourceLifecycleEnv.Disk.Column.Path"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "ResourceLifecycleEnv.Disk.Column.MinFreeMb"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
        };
    columns[0].setUsingVariables(true);
    columns[1].setUsingVariables(true);

    wDiskChecks =
        new TableView(
            variables, comp, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, columns, 1, null, props);
    PropsUi.setLook(wDiskChecks);
    FormData fdDisk = new FormData();
    fdDisk.left = new FormAttachment(0, 0);
    fdDisk.top = new FormAttachment(wlDisk, margin);
    fdDisk.right = new FormAttachment(wBrowse, -margin);
    fdDisk.bottom = new FormAttachment(100, 0);
    wDiskChecks.setLayoutData(fdDisk);

    extension.addLoadCallback(this::loadFromContext);
    extension.addSaveCallback(this::saveToContext);
  }

  private void loadFromContext(AttributesContext context) {
    if (wOnEnable == null || wOnEnable.isDisposed()) {
      return;
    }
    wOnEnable.setText(ResourceAttributes.resolveOnEnable(context, context.getPurpose()));
    wMinMaxMemoryMb.setText(
        Const.NVL(
            context.getAttribute(
                ResourceAttributes.GROUP, ResourceAttributes.KEY_MIN_MAX_MEMORY_MB),
            ""));
    wMinProcessors.setText(
        Const.NVL(
            context.getAttribute(ResourceAttributes.GROUP, ResourceAttributes.KEY_MIN_PROCESSORS),
            ""));

    wDiskChecks.table.removeAll();
    List<DiskSpaceRequirement> disks =
        ResourceAttributes.parseDiskChecks(
            context.getAttribute(ResourceAttributes.GROUP, ResourceAttributes.KEY_DISK_CHECKS));
    if (disks.isEmpty()) {
      new TableItem(wDiskChecks.table, SWT.NONE);
    } else {
      for (DiskSpaceRequirement disk : disks) {
        TableItem item = new TableItem(wDiskChecks.table, SWT.NONE);
        item.setText(1, Const.NVL(disk.getPath(), ""));
        item.setText(2, Const.NVL(disk.getMinFreeBytes(), ""));
      }
    }
    wDiskChecks.removeEmptyRows();
    wDiskChecks.setRowNums();
    wDiskChecks.optWidth(true);
  }

  private void saveToContext(AttributesContext context) {
    if (wOnEnable == null || wOnEnable.isDisposed()) {
      return;
    }
    context.setAttribute(
        ResourceAttributes.GROUP,
        ResourceAttributes.KEY_ON_ENABLE,
        Const.NVL(wOnEnable.getText(), ResourceAttributes.ON_ENABLE_OFF));

    setOrClear(context, ResourceAttributes.KEY_MIN_MAX_MEMORY_MB, wMinMaxMemoryMb.getText());
    setOrClear(context, ResourceAttributes.KEY_MIN_PROCESSORS, wMinProcessors.getText());

    List<DiskSpaceRequirement> disks = new ArrayList<>();
    for (TableItem item : wDiskChecks.getNonEmptyItems()) {
      String path = item.getText(1);
      String mb = item.getText(2);
      if (StringUtils.isBlank(path) || StringUtils.isBlank(mb)) {
        continue;
      }
      // Keep expression as entered (literals, expanded numbers, or variables); resolve at check
      // time
      disks.add(new DiskSpaceRequirement(path.trim(), mb.trim()));
    }
    String encoded = ResourceAttributes.formatDiskChecks(disks);
    setOrClear(context, ResourceAttributes.KEY_DISK_CHECKS, encoded);
  }

  private static void setOrClear(AttributesContext context, String key, String value) {
    if (StringUtils.isBlank(value)) {
      Map<String, String> group = context.getAttributes(ResourceAttributes.GROUP);
      if (group != null) {
        group.remove(key);
      }
    } else {
      context.setAttribute(ResourceAttributes.GROUP, key, value.trim());
    }
  }
}
