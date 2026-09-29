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
package org.apache.hop.vfs.git.metadata;

import java.util.HashSet;
import java.util.Set;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.GuiCompositeWidgetsAdapter;
import org.apache.hop.ui.core.gui.IGuiPluginCompositeButtonsListener;
import org.apache.hop.ui.core.metadata.MetadataEditor;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.core.widget.NamingSchemeTypes;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.vfs.explorer.VfsFileExplorerViews;
import org.apache.hop.vfs.git.GitAuthType;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;

@GuiPlugin(
    description = "This is the editor for git VFS connection metadata",
    classLoaderGroup = "vfs-git")
public class GitConnectionEditor extends MetadataEditor<GitConnection> {

  private static final Class<?> PKG = GitConnectionEditor.class;

  public static final String GUI_WIDGETS_PARENT_ID = "GitConnectionEditor-GuiWidgetsParent";

  private TextVar wName;
  private Composite wWidgetsComposite;
  private GuiCompositeWidgets guiCompositeWidgets;

  public GitConnectionEditor(
      HopGui hopGui, MetadataManager<GitConnection> manager, GitConnection metadata) {
    super(hopGui, manager, metadata);
  }

  @Override
  public void createControl(Composite parent) {
    PropsUi props = PropsUi.getInstance();
    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin() + 2;

    Label wIcon = new Label(parent, SWT.RIGHT);
    wIcon.setImage(getImage());
    FormData fdlIcon = new FormData();
    fdlIcon.top = new FormAttachment(0, 0);
    fdlIcon.right = new FormAttachment(100, 0);
    wIcon.setLayoutData(fdlIcon);
    PropsUi.setLook(wIcon);

    Label wlName = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlName);
    wlName.setText(BaseMessages.getString(PKG, "GitConnectionEditor.Name.Label"));
    FormData fdlName = new FormData();
    fdlName.top = new FormAttachment(0, margin);
    fdlName.left = new FormAttachment(0, 0);
    fdlName.right = new FormAttachment(middle, -margin);
    wlName.setLayoutData(fdlName);
    wName =
        new TextVar(hopGui.getVariables(), parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER)
            .asNameField(NamingSchemeTypes.HOP_METADATA);
    PropsUi.setLook(wName);
    FormData fdName = new FormData();
    fdName.top = new FormAttachment(wlName, 0, SWT.CENTER);
    fdName.left = new FormAttachment(middle, 0);
    fdName.right = new FormAttachment(wIcon, -margin);
    wName.setLayoutData(fdName);
    Control lastControl = wName;

    wWidgetsComposite = new Composite(parent, SWT.NONE);
    PropsUi.setLook(wWidgetsComposite);
    wWidgetsComposite.setLayout(new FormLayout());
    FormData fdWidgetsComposite = new FormData();
    fdWidgetsComposite.top = new FormAttachment(lastControl, margin);
    fdWidgetsComposite.left = new FormAttachment(0, 0);
    fdWidgetsComposite.right = new FormAttachment(100, 0);
    fdWidgetsComposite.bottom = new FormAttachment(100, 0);
    wWidgetsComposite.setLayoutData(fdWidgetsComposite);

    guiCompositeWidgets = new GuiCompositeWidgets(manager.getVariables());
    guiCompositeWidgets.createCompositeWidgets(
        metadata, null, wWidgetsComposite, GUI_WIDGETS_PARENT_ID, lastControl);
    guiCompositeWidgets.setWidgetsListener(
        new GuiCompositeWidgetsAdapter() {
          @Override
          public void widgetsCreated(GuiCompositeWidgets compositeWidgets) {
            showFieldsForAuthType(compositeWidgets);
          }

          @Override
          public void widgetModified(
              GuiCompositeWidgets compositeWidgets, Control changedWidget, String widgetId) {
            setChanged();
            if (GitConnection.WIDGET_AUTH_TYPE.equals(widgetId)) {
              showFieldsForAuthType(compositeWidgets);
            }
          }
        });
    guiCompositeWidgets.setCompositeButtonsListener(
        new IGuiPluginCompositeButtonsListener() {
          @Override
          public void buttonPressed(Object sourceObject) {
            Object model = sourceObject != null ? sourceObject : metadata;
            getWidgetsContent((GitConnection) model);
          }
        });

    setWidgetsContent();
    resetChanged();
    wName.addModifyListener(e -> setChanged());
  }

  @Override
  public void setWidgetsContent() {
    GitConnection meta = this.getMetadata();
    wName.setText(Const.NVL(meta.getName(), ""));
    guiCompositeWidgets.setWidgetsContents(metadata, wWidgetsComposite, GUI_WIDGETS_PARENT_ID);
  }

  @Override
  public void getWidgetsContent(GitConnection meta) {
    meta.setName(wName.getText());
    guiCompositeWidgets.getWidgetsContents(meta, GUI_WIDGETS_PARENT_ID);
  }

  @Override
  public boolean setFocus() {
    if (wName == null || wName.isDisposed()) {
      return false;
    }
    return wName.setFocus();
  }

  @Override
  public void save() throws HopException {
    super.save();
    // The name of a connection is a VFS scheme: re-register the providers so the new or changed
    // connection is picked up right away.
    //
    HopVfs.refresh(hopGui.getVariables());
  }

  @Override
  public Button[] createButtonsForButtonBar(Composite parent) {
    return VfsFileExplorerViews.exploreButton(parent, this);
  }

  /**
   * A user name is only used with a password, and a deploy key is only used over SSH. The fields
   * which do not apply to the selected authentication are hidden so the tab shows the ones that do.
   */
  private void showFieldsForAuthType(GuiCompositeWidgets widgets) {
    GitAuthType type =
        GitAuthType.lookupDescription(comboText(widgets, GitConnection.WIDGET_AUTH_TYPE));
    Set<String> hidden = new HashSet<>();
    if (type != GitAuthType.USERNAME_PASSWORD) {
      hidden.add(GitConnection.WIDGET_USER_NAME);
      hidden.add(GitConnection.WIDGET_PASSWORD);
    }
    if (type != GitAuthType.DEPLOY_KEY) {
      hidden.add(GitConnection.WIDGET_PRIVATE_KEY);
      hidden.add(GitConnection.WIDGET_PASSPHRASE);
      hidden.add(GitConnection.WIDGET_SSH_USER);
      hidden.add(GitConnection.WIDGET_KNOWN_HOSTS);
      hidden.add(GitConnection.WIDGET_ACCEPT_UNKNOWN_HOSTS);
    }
    widgets.setWidgetsHidden(metadata, hidden);
  }

  private String comboText(GuiCompositeWidgets widgets, String widgetId) {
    Control control = widgets.getWidgetsMap().get(widgetId);
    if (control instanceof Combo combo) {
      return combo.getText();
    }
    if (control instanceof CCombo combo) {
      return combo.getText();
    }
    return "";
  }
}
