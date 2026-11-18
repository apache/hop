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
package org.apache.hop.vfs.sharepoint.metadatatype;

import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import java.util.HashSet;
import java.util.Set;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.GuiCompositeWidgetsAdapter;
import org.apache.hop.ui.core.metadata.MetadataEditor;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.NamingSchemeTypes;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.vfs.explorer.VfsFileExplorerViews;
import org.apache.hop.vfs.sharepoint.metadatatype.SharePointConnection.AuthenticationType;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.jspecify.annotations.NullMarked;

/**
 * The name is the only widget created here, the other options are the fields of {@link
 * SharePointConnection} annotated with {@code @GuiWidgetElement}.
 */
@NullMarked
@GuiPlugin(
    description = "Editor for SharePoint connection metadata",
    classLoaderGroup = "vfs-graph")
public class SharePointConnectionEditor extends MetadataEditor<SharePointConnection> {

  private static final Class<?> PKG = SharePointConnectionEditor.class;

  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID =
      "SharePointConnectionEditor-GuiWidgetsParent";

  private TextVar wName;
  private GuiCompositeWidgets guiCompositeWidgets;

  public SharePointConnectionEditor(
      HopGui hopGui, MetadataManager<SharePointConnection> manager, SharePointConnection metadata) {
    super(hopGui, manager, metadata);
  }

  @Override
  public void createControl(Composite parent) {
    int middle = PropsUi.getInstance().getMiddlePct();
    int margin = PropsUi.getMargin() + 2;

    Label wIcon = new Label(parent, SWT.RIGHT);
    wIcon.setImage(getImage());
    FormData fdIcon = new FormData();
    fdIcon.top = new FormAttachment(0, 0);
    fdIcon.right = new FormAttachment(100, 0);
    wIcon.setLayoutData(fdIcon);
    PropsUi.setLook(wIcon);

    Label wlName = new Label(parent, SWT.RIGHT);
    PropsUi.setLook(wlName);
    wlName.setText(BaseMessages.getString(PKG, "SharePointConnectionEditor.Name.Label"));
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

    // The options are grouped in tabs, in a container that scrolls when the editor is too small
    guiCompositeWidgets =
        GuiCompositeWidgets.addScrolledComposite(
            parent, manager.getVariables(), wName, null, GUI_PLUGIN_ELEMENT_PARENT_ID, metadata);
    guiCompositeWidgets.setWidgetsListener(
        new GuiCompositeWidgetsAdapter() {
          @Override
          public void widgetModified(
              GuiCompositeWidgets compositeWidgets, Control changedWidget, String widgetId) {
            setChanged();
            if (SharePointConnection.WIDGET_AUTHENTICATION_TYPE.equals(widgetId)) {
              hideUnusedCredentials();
            }
          }
        });

    setWidgetsContent();
    resetChanged();
    wName.addModifyListener(e -> setChanged());
  }

  /** The connection is a VFS scheme, the file systems are registered again to pick up a change. */
  @Override
  public void save() throws HopException {
    super.save();
    HopVfs.refresh(hopGui.getVariables());
  }

  @Override
  public Button[] createButtonsForButtonBar(final Composite parent) {
    Button wTest = new Button(parent, SWT.PUSH);
    wTest.setText(BaseMessages.getString(PKG, "System.Button.Test"));
    wTest.addListener(SWT.Selection, e -> testConnection());

    Button wExplore = VfsFileExplorerViews.exploreButton(parent, this)[0];

    return new Button[] {wTest, wExplore};
  }

  @Override
  public void setWidgetsContent() {
    wName.setText(Const.NVL(getMetadata().getName(), ""));
    guiCompositeWidgets.setWidgetsContents(metadata, null, GUI_PLUGIN_ELEMENT_PARENT_ID);
    hideUnusedCredentials();
  }

  @Override
  public void getWidgetsContent(SharePointConnection meta) {
    meta.setName(wName.getText());
    guiCompositeWidgets.getWidgetsContents(meta, GUI_PLUGIN_ELEMENT_PARENT_ID);
  }

  @Override
  public boolean setFocus() {
    if (wName == null || wName.isDisposed()) {
      return false;
    }
    return wName.setFocus();
  }

  /** Only shows the credentials the selected authentication type uses. */
  private void hideUnusedCredentials() {
    Set<String> hidden = new HashSet<>();
    switch (selectedAuthenticationType()) {
      case SECRET -> hidden.add(SharePointConnection.WIDGET_CERTIFICATE);
      case CERTIFICATE -> hidden.add(SharePointConnection.WIDGET_CLIENT_SECRET);
      case MANAGED_IDENTITY -> {
        hidden.add(SharePointConnection.WIDGET_CLIENT_ID);
        hidden.add(SharePointConnection.WIDGET_CLIENT_SECRET);
        hidden.add(SharePointConnection.WIDGET_CERTIFICATE);
      }
    }
    guiCompositeWidgets.setWidgetsHidden(metadata, hidden);
  }

  private AuthenticationType selectedAuthenticationType() {
    Control control =
        guiCompositeWidgets.getWidgetsMap().get(SharePointConnection.WIDGET_AUTHENTICATION_TYPE);
    String description = null;
    if (control instanceof Combo combo) {
      description = combo.getText();
    } else if (control instanceof ComboVar comboVar) {
      description = comboVar.getText();
    }
    // Nothing selected yet, the connection is the better answer
    return description == null || description.isBlank()
        ? getMetadata().getAuthenticationType()
        : AuthenticationType.lookupDescription(description);
  }

  private void testConnection() {
    try {
      SharePointConnection meta = new SharePointConnection();
      this.getWidgetsContent(meta);

      TokenCredential credential = meta.credentials(LogChannel.UI, hopGui.getVariables());

      // Test credential
      credential
          .getToken(new TokenRequestContext().addScopes("https://graph.microsoft.com/.default"))
          .block();

      MessageBox dialog = new MessageBox(getShell(), SWT.ICON_INFORMATION | SWT.OK);
      dialog.setText(
          BaseMessages.getString(PKG, "SharePointConnectionEditor.TestConnection.Title"));
      dialog.setMessage(
          BaseMessages.getString(PKG, "SharePointConnectionEditor.TestConnection.Success"));
      dialog.open();
    } catch (Exception error) {
      new ErrorDialog(
          getShell(),
          BaseMessages.getString(PKG, "SharePointConnectionEditor.TestConnection.Title"),
          BaseMessages.getString(PKG, "SharePointConnectionEditor.TestConnection.Error"),
          error);
    }
  }
}
