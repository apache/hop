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
package org.apache.hop.git.model.repository;

import org.apache.hop.core.Const;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.metadata.MetadataEditor;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Text;

public class GitRepositoryEditor extends MetadataEditor<GitRepository> {

  private static final Class<?> PKG = GitRepositoryEditor.class; // For Translator

  private Text nameText;
  private Text descText;
  private TextVar directoryText;

  protected PropsUi props;

  protected static String APPLICATION_NAME;

  /**
   * A dialog to edit repository settings.
   *
   * @param hopGui
   * @param manager the metadata manager
   * @param repo a git repository to edit. Can be null to create a new one.
   */
  public GitRepositoryEditor(
      HopGui hopGui, MetadataManager<GitRepository> manager, GitRepository metadata) {
    super(hopGui, manager, metadata);
    APPLICATION_NAME = "Edit Repository";
  }

  @Override
  public void createControl(Composite parent) {

    PropsUi props = PropsUi.getInstance();

    GitRepository repository = this.getMetadata();

    // The name line...
    //
    Label wIcon = new Label(parent, SWT.RIGHT);
    wIcon.setImage(getImage());
    FormData fdlicon = new FormData();
    fdlicon.top = new FormAttachment(0, 0);
    fdlicon.right = new FormAttachment(100, 0);
    wIcon.setLayoutData(fdlicon);
    props.setLook(wIcon);

    Label nameLabel = new Label(parent, SWT.RIGHT);
    nameLabel.setText("Name");
    FormData fdNameLabel = new FormData();
    fdNameLabel.left = new FormAttachment(0, 0);
    fdNameLabel.top = new FormAttachment(0, 0);
    nameLabel.setLayoutData(fdNameLabel);
    props.setLook(nameLabel);

    nameText = new Text(parent, SWT.SINGLE | SWT.BORDER);
    nameText.addModifyListener(e -> setChanged());
    FormData fdNameText = new FormData();
    fdNameText.left = new FormAttachment(0, 0);
    fdNameText.right = new FormAttachment(wIcon, -5);
    fdNameText.top = new FormAttachment(nameLabel, 5);
    nameText.setLayoutData(fdNameText);
    props.setLook(nameText);

    // Spacer
    Label spacer = new Label(parent, SWT.HORIZONTAL | SWT.SEPARATOR);
    FormData fdSpacer = new FormData();
    fdSpacer.left = new FormAttachment(0, 0);
    fdSpacer.top = new FormAttachment(nameText, 15);
    fdSpacer.right = new FormAttachment(100, 0);
    spacer.setLayoutData(fdSpacer);

    // The description line
    //
    Label descLabel = new Label(parent, SWT.RIGHT);
    descLabel.setText("Description");
    FormData fdDescLabel = new FormData();
    fdDescLabel.left = new FormAttachment(0, 0);
    fdDescLabel.top = new FormAttachment(spacer, 15);
    descLabel.setLayoutData(fdDescLabel);
    props.setLook(descLabel);

    descText = new Text(parent, SWT.MULTI | SWT.V_SCROLL | SWT.BORDER);
    descText.addModifyListener(e -> setChanged());
    FormData fdDescText = new FormData();
    fdDescText.height = 50;
    fdDescText.left = new FormAttachment(0, 0);
    fdDescText.right = new FormAttachment(100, 0);
    fdDescText.top = new FormAttachment(descLabel, props.getMargin());
    descText.setLayoutData(fdDescText);
    props.setLook(descText);

    // The directory
    //
    Label directoryLabel = new Label(parent, SWT.RIGHT);
    directoryLabel.setText("Directory");
    FormData fdDirectoryLabel = new FormData();
    fdDirectoryLabel.left = new FormAttachment(0, 0);
    fdDirectoryLabel.top = new FormAttachment(descText, props.getMargin());
    directoryLabel.setLayoutData(fdDirectoryLabel);
    props.setLook(directoryLabel);

    Button directoryButton = new Button(parent, SWT.PUSH);
    directoryButton.setText("Browse");
    directoryButton.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            String filterPath =
                BaseDialog.presentDirectoryDialog(getShell(), getMetadataManager().getVariables());
            if (filterPath != null) {
              directoryText.setText(filterPath);
            }
          }
        });
    FormData fdDirectoryButton = new FormData();
    fdDirectoryButton.right = new FormAttachment(100, 0);
    fdDirectoryButton.top = new FormAttachment(directoryLabel, props.getMargin());
    directoryButton.setLayoutData(fdDirectoryButton);

    directoryText =
        new TextVar(getMetadataManager().getVariables(), parent, SWT.SINGLE | SWT.BORDER);
    directoryText.addModifyListener(e -> setChanged());
    FormData fdDirectoryText = new FormData();
    fdDirectoryText.left = new FormAttachment(0, 0);
    fdDirectoryText.right = new FormAttachment(directoryButton, -props.getMargin());
    fdDirectoryText.top = new FormAttachment(directoryLabel, props.getMargin());
    directoryText.setLayoutData(fdDirectoryText);
    props.setLook(directoryText);
  }

  @Override
  public boolean setFocus() {
    if (nameText == null || nameText.isDisposed()) {
      return false;
    }
    return nameText.setFocus();
  }

  @Override
  public void setWidgetsContent() {
    GitRepository repository = this.getMetadata();
    nameText.setText(Const.NVL(repository.getName(), ""));
    descText.setText(Const.NVL(repository.getDescription(), ""));
    directoryText.setText(Const.NVL(repository.getDirectory(), ""));
  }

  @Override
  public void getWidgetsContent(GitRepository repository) {
    repository.setName(nameText.getText());
    repository.setDescription(descText.getText());
    repository.setDirectory(directoryText.getText());
  }
}
