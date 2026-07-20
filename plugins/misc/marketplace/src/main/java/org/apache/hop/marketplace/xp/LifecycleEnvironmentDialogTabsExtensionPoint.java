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

package org.apache.hop.marketplace.xp;

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.AttributesContext;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.marketplace.env.MarketplaceAttributes;
import org.apache.hop.marketplace.gui.HopEnvironmentDialog;
import org.apache.hop.marketplace.gui.MarketplaceGuiPlugin;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.AttributesDialogExtension;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.gui.GuiResource;
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
import org.eclipse.swt.widgets.Text;

/**
 * Contributes a Marketplace / Plugins tab on the lifecycle environment dialog. Settings are stored
 * under {@link MarketplaceAttributes#GROUP} on the shared {@link AttributesContext}.
 */
@ExtensionPoint(
    id = "MarketplaceLifecycleEnvironmentDialogTabs",
    description = "Add marketplace plugin policy tab to the lifecycle environment dialog",
    extensionPointId = "HopGuiLifecycleEnvironmentDialogTabs")
public class LifecycleEnvironmentDialogTabsExtensionPoint
    implements IExtensionPoint<AttributesDialogExtension> {

  private static final Class<?> PKG = MarketplaceGuiPlugin.class;

  private Text wEnvFile;
  private Combo wOnEnable;
  private Button wStrict;
  private Button wAutoApply;

  @Override
  public void callExtensionPoint(
      ILogChannel log, IVariables variables, AttributesDialogExtension extension)
      throws HopException {
    if (extension == null || extension.getTabFolder() == null) {
      return;
    }

    PropsUi props = PropsUi.getInstance();
    CTabFolder folder = extension.getTabFolder();
    int margin = PropsUi.getMargin();
    int middle = props.getMiddlePct();

    CTabItem tab = new CTabItem(folder, SWT.NONE);
    tab.setText(BaseMessages.getString(PKG, "MarketplaceLifecycleEnv.Tab.Plugins"));
    tab.setImage(GuiResource.getInstance().getImagePlugin());
    Composite comp = new Composite(folder, SWT.NONE);
    PropsUi.setLook(comp);
    FormLayout layout = new FormLayout();
    layout.marginWidth = PropsUi.getFormMargin();
    layout.marginHeight = PropsUi.getFormMargin();
    comp.setLayout(layout);
    tab.setControl(comp);

    Label wlHelp = new Label(comp, SWT.LEFT | SWT.WRAP);
    PropsUi.setLook(wlHelp);
    wlHelp.setText(BaseMessages.getString(PKG, "MarketplaceLifecycleEnv.Help"));
    FormData fdlHelp = new FormData();
    fdlHelp.left = new FormAttachment(0, 0);
    fdlHelp.top = new FormAttachment(0, 0);
    fdlHelp.right = new FormAttachment(100, 0);
    wlHelp.setLayoutData(fdlHelp);

    Label wlEnvFile = new Label(comp, SWT.RIGHT);
    PropsUi.setLook(wlEnvFile);
    wlEnvFile.setText(BaseMessages.getString(PKG, "MarketplaceLifecycleEnv.EnvFile.Label"));
    FormData fdlEnv = new FormData();
    fdlEnv.left = new FormAttachment(0, 0);
    fdlEnv.top = new FormAttachment(wlHelp, margin * 2);
    fdlEnv.right = new FormAttachment(middle, -margin);
    wlEnvFile.setLayoutData(fdlEnv);

    Button wEditEnv = new Button(comp, SWT.PUSH);
    wEditEnv.setText(BaseMessages.getString(PKG, "MarketplaceLifecycleEnv.Button.EditEnv"));
    FormData fdEdit = new FormData();
    fdEdit.right = new FormAttachment(100, 0);
    fdEdit.top = new FormAttachment(wlEnvFile, 0, SWT.CENTER);
    wEditEnv.setLayoutData(fdEdit);
    wEditEnv.addListener(
        SWT.Selection,
        e -> {
          Path initial = null;
          String text = wEnvFile.getText();
          if (StringUtils.isNotBlank(text)) {
            String resolved = variables != null ? variables.resolve(text.trim()) : text.trim();
            Path candidate = Path.of(resolved).toAbsolutePath().normalize();
            if (Files.isRegularFile(candidate)) {
              initial = candidate;
            }
          }
          Path saved = new HopEnvironmentDialog(extension.getShell(), initial).open();
          if (saved != null) {
            wEnvFile.setText(saved.toString());
          }
        });

    Button wBrowse = new Button(comp, SWT.PUSH);
    wBrowse.setText(BaseMessages.getString(PKG, "MarketplaceLifecycleEnv.Button.Browse"));
    FormData fdBrowse = new FormData();
    fdBrowse.right = new FormAttachment(wEditEnv, -margin);
    fdBrowse.top = new FormAttachment(wlEnvFile, 0, SWT.CENTER);
    wBrowse.setLayoutData(fdBrowse);
    wBrowse.addListener(
        SWT.Selection,
        e -> {
          String path =
              BaseDialog.presentFileDialog(
                  false,
                  extension.getShell(),
                  new String[] {"*.yaml;*.yml;*.json", "*.*"},
                  new String[] {
                    BaseMessages.getString(PKG, "MarketplaceDialog.EnvFile.Filter.Env"),
                    BaseMessages.getString(PKG, "MarketplaceDialog.EnvFile.Filter.All")
                  },
                  false);
          if (StringUtils.isNotBlank(path)) {
            wEnvFile.setText(path);
          }
        });

    wEnvFile = new Text(comp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wEnvFile);
    FormData fdEnv = new FormData();
    fdEnv.left = new FormAttachment(middle, 0);
    fdEnv.top = new FormAttachment(wlEnvFile, 0, SWT.CENTER);
    fdEnv.right = new FormAttachment(wBrowse, -margin);
    wEnvFile.setLayoutData(fdEnv);

    Label wlOnEnable = new Label(comp, SWT.RIGHT);
    PropsUi.setLook(wlOnEnable);
    wlOnEnable.setText(BaseMessages.getString(PKG, "MarketplaceLifecycleEnv.OnEnable.Label"));
    FormData fdlOn = new FormData();
    fdlOn.left = new FormAttachment(0, 0);
    fdlOn.top = new FormAttachment(wEnvFile, margin * 2);
    fdlOn.right = new FormAttachment(middle, -margin);
    wlOnEnable.setLayoutData(fdlOn);

    wOnEnable = new Combo(comp, SWT.READ_ONLY | SWT.BORDER);
    PropsUi.setLook(wOnEnable);
    wOnEnable.setItems(
        MarketplaceAttributes.ON_ENABLE_OFF,
        MarketplaceAttributes.ON_ENABLE_WARN,
        MarketplaceAttributes.ON_ENABLE_ENFORCE);
    FormData fdOn = new FormData();
    fdOn.left = new FormAttachment(middle, 0);
    fdOn.top = new FormAttachment(wlOnEnable, 0, SWT.CENTER);
    fdOn.right = new FormAttachment(100, 0);
    wOnEnable.setLayoutData(fdOn);
    wOnEnable.setToolTipText(
        BaseMessages.getString(PKG, "MarketplaceLifecycleEnv.OnEnable.Tooltip"));

    wStrict = new Button(comp, SWT.CHECK);
    PropsUi.setLook(wStrict);
    wStrict.setText(BaseMessages.getString(PKG, "MarketplaceLifecycleEnv.Strict.Label"));
    wStrict.setToolTipText(BaseMessages.getString(PKG, "MarketplaceLifecycleEnv.Strict.Tooltip"));
    FormData fdStrict = new FormData();
    fdStrict.left = new FormAttachment(middle, 0);
    fdStrict.top = new FormAttachment(wOnEnable, margin * 2);
    wStrict.setLayoutData(fdStrict);

    wAutoApply = new Button(comp, SWT.CHECK);
    PropsUi.setLook(wAutoApply);
    wAutoApply.setText(BaseMessages.getString(PKG, "MarketplaceLifecycleEnv.AutoApply.Label"));
    wAutoApply.setToolTipText(
        BaseMessages.getString(PKG, "MarketplaceLifecycleEnv.AutoApply.Tooltip"));
    FormData fdAuto = new FormData();
    fdAuto.left = new FormAttachment(middle, 0);
    fdAuto.top = new FormAttachment(wStrict, margin);
    wAutoApply.setLayoutData(fdAuto);

    extension.addLoadCallback(this::loadFromContext);
    extension.addSaveCallback(this::saveToContext);
  }

  private void loadFromContext(AttributesContext context) {
    if (wEnvFile == null || wEnvFile.isDisposed()) {
      return;
    }
    wEnvFile.setText(Const.NVL(MarketplaceAttributes.envFile(context), ""));
    // Explicit attribute, or purpose-based default when unset (not written until save).
    wOnEnable.setText(MarketplaceAttributes.resolveOnEnable(context, context.getPurpose()));
    wStrict.setSelection(MarketplaceAttributes.isStrict(context));
    wAutoApply.setSelection(MarketplaceAttributes.isAutoApply(context));
  }

  private void saveToContext(AttributesContext context) {
    if (wEnvFile == null || wEnvFile.isDisposed()) {
      return;
    }
    String envFile = wEnvFile.getText();
    if (StringUtils.isBlank(envFile)) {
      // clear key if empty
      if (context.getAttributes(MarketplaceAttributes.GROUP) != null) {
        context
            .getAttributes(MarketplaceAttributes.GROUP)
            .remove(MarketplaceAttributes.KEY_ENV_FILE);
      }
    } else {
      context.setAttribute(
          MarketplaceAttributes.GROUP, MarketplaceAttributes.KEY_ENV_FILE, envFile.trim());
    }
    context.setAttribute(
        MarketplaceAttributes.GROUP,
        MarketplaceAttributes.KEY_ON_ENABLE,
        Const.NVL(wOnEnable.getText(), MarketplaceAttributes.ON_ENABLE_OFF));
    context.setAttribute(
        MarketplaceAttributes.GROUP,
        MarketplaceAttributes.KEY_STRICT,
        Boolean.toString(wStrict.getSelection()));
    context.setAttribute(
        MarketplaceAttributes.GROUP,
        MarketplaceAttributes.KEY_AUTO_APPLY,
        Boolean.toString(wAutoApply.getSelection()));
  }
}
