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

package org.apache.hop.git;

import java.net.URISyntaxException;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.git.provider.GitConnection;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.jgit.transport.URIish;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

/**
 * Defines the remote of a repository, origin: its URL and, optionally, the Git connection whose
 * credentials are used to reach it.
 */
public class RemoteDialog extends Dialog {
  private static final Class<?> PKG = GitGuiPlugin.class;

  private final IVariables variables;
  private final IHopMetadataProvider metadataProvider;

  private Shell shell;
  private Text wUrl;
  private MetaSelectionLine<GitConnection> wConnection;

  @Getter private String url;

  /**
   * @return the name of the Git connection, null if none was chosen
   */
  @Getter private String gitConnection;

  private boolean ok;

  /**
   * @param url the current URL of the remote, null if there is none yet: one is then added
   * @param gitConnection the name of the Git connection associated with the remote, null if none
   */
  public RemoteDialog(
      Shell parent,
      IVariables variables,
      IHopMetadataProvider metadataProvider,
      String url,
      String gitConnection) {
    super(parent, SWT.NONE);
    this.variables = variables;
    this.metadataProvider = metadataProvider;
    this.url = url;
    this.gitConnection = gitConnection;
  }

  /**
   * @return true if the user confirmed, the URL and the Git connection are then available
   */
  public boolean open() {
    Shell parent = getParent();
    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.SHEET);
    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();
    shell.setLayout(formLayout);
    shell.setImage(GuiResource.getInstance().getImageHopUi());
    shell.setText(
        BaseMessages.getString(
            PKG,
            StringUtils.isEmpty(url)
                ? "GitPerspective.Dialog.Remote.Add.Header"
                : "GitPerspective.Dialog.Remote.Edit.Header"));
    PropsUi.setLook(shell);

    int margin = PropsUi.getMargin();
    int middle = PropsUi.getInstance().getMiddlePct();

    // The URL of the remote
    Label wlUrl = new Label(shell, SWT.RIGHT);
    wlUrl.setText(BaseMessages.getString(PKG, "GitPerspective.Dialog.Remote.Url.Label"));
    wlUrl.setToolTipText(BaseMessages.getString(PKG, "GitPerspective.Dialog.Remote.Url.Tooltip"));
    PropsUi.setLook(wlUrl);
    wlUrl.setLayoutData(FormDataBuilder.builder().left().top().right(middle, -margin).result());
    wUrl = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wUrl.setToolTipText(BaseMessages.getString(PKG, "GitPerspective.Dialog.Remote.Url.Tooltip"));
    PropsUi.setLook(wUrl);
    wUrl.setLayoutData(FormDataBuilder.builder().left(middle, 0).top().right(100, 0).result());

    // The Git connection holding the credentials, optional
    wConnection =
        new MetaSelectionLine<>(
            variables,
            metadataProvider,
            GitConnection.class,
            shell,
            SWT.SINGLE | SWT.LEFT | SWT.BORDER,
            BaseMessages.getString(PKG, "GitPerspective.Dialog.Remote.GitConnection.Label"),
            BaseMessages.getString(PKG, "GitPerspective.Dialog.Remote.GitConnection.Tooltip"));
    PropsUi.setLook(wConnection);
    wConnection.setLayoutData(
        FormDataBuilder.builder().left().top(wUrl, margin).right(100, 0).result());
    try {
      wConnection.fillItems();
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "GitPerspective.Dialog.Remote.Error.Header"),
          BaseMessages.getString(PKG, "GitPerspective.Dialog.Remote.Error.Connections"),
          e);
    }

    // Some buttons
    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString("System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString("System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    BaseTransformDialog.positionBottomButtons(
        shell, new Button[] {wOk, wCancel}, margin, wConnection);

    wUrl.setText(StringUtils.defaultString(url));
    wConnection.setText(StringUtils.defaultString(gitConnection));
    wUrl.setFocus();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return ok;
  }

  private void ok() {
    String enteredUrl = wUrl.getText().trim();
    if (!isValidUrl(enteredUrl)) {
      MessageBox box = new MessageBox(shell, SWT.ICON_ERROR | SWT.OK);
      box.setText(BaseMessages.getString(PKG, "GitPerspective.Dialog.Remote.Error.Header"));
      box.setMessage(
          BaseMessages.getString(PKG, "GitPerspective.Dialog.Remote.Error.Url", enteredUrl));
      box.open();
      wUrl.setFocus();
      return;
    }
    url = enteredUrl;
    gitConnection = StringUtils.trimToNull(wConnection.getText());
    ok = true;
    dispose();
  }

  private static boolean isValidUrl(String url) {
    if (url.isEmpty()) {
      return false;
    }
    try {
      new URIish(url);
      return true;
    } catch (URISyntaxException e) {
      return false;
    }
  }

  private void cancel() {
    ok = false;
    dispose();
  }

  private void dispose() {
    PropsUi.getInstance().setScreen(new WindowProperty(shell));
    shell.dispose();
  }
}
